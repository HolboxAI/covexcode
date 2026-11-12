/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! Read-only subagents: child agent loops whose "user" is the parent model.
//!
//! `explore` is built in. Users can add named read-only agents as TOML files
//! under `<project>/.covexcode/agents/` or `~/.covexcode/agents/`; the project
//! wins on a name collision, and `explore` itself cannot be redefined. The
//! child always gets the same read-only tool slice, so a definition changes
//! what the child is asked to do, never what it is able to do.
//!
//! This lives outside `agent.rs` because `agent.rs` also owns the TUI loop and
//! imports `App`; the ACP-only runtime deliberately omits the TUI and still
//! needs subagents. Nothing here touches `App`.

use crate::config::Config;
use crate::llm::{self, ChatMessage, StreamEvent, ToolCall};
use crate::tools::{self, ToolOutcome};
use crate::workspace::Workspace;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use tokio::sync::mpsc;

/// One user-defined agent. Read-only unless `writes = true`, which puts the
/// child in an isolated git worktree and returns its changes as a patch.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentDefinition {
    pub name: String,
    pub description: String,
    /// Appended to the built-in child system prompt, so the read-only or
    /// worktree rules stay in force even for a hand-written definition.
    pub prompt: String,
    pub writes: bool,
}

#[derive(Deserialize)]
struct AgentFile {
    name: String,
    #[serde(default)]
    description: String,
    prompt: String,
    #[serde(default)]
    writes: bool,
}

/// Every usable definition, project over user, sorted by name. Invalid or
/// unreadable files are skipped: a bad agent file is a convenience lost, not
/// a session that should fail.
pub fn definitions(workspace_root: &Path) -> Vec<AgentDefinition> {
    let mut by_name: BTreeMap<String, AgentDefinition> = BTreeMap::new();
    let user = crate::config::Config::config_dir().join("agents");
    let project = workspace_root.join(".covexcode").join("agents");
    // User first, project second: a project can override a personal agent of
    // the same name.
    for dir in [user, project] {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut paths: Vec<_> = entries.flatten().map(|entry| entry.path()).collect();
        paths.sort();
        for path in paths {
            if path.extension().and_then(|extension| extension.to_str()) != Some("toml") {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(file) = toml::from_str::<AgentFile>(&content) else {
                continue;
            };
            let name = file.name.trim();
            if !valid_agent_name(name) || name == "explore" || file.prompt.trim().is_empty() {
                continue;
            }
            by_name.insert(
                name.to_string(),
                AgentDefinition {
                    name: name.to_string(),
                    description: file.description.trim().to_string(),
                    prompt: file.prompt.trim().to_string(),
                    writes: file.writes,
                },
            );
        }
    }
    by_name.into_values().collect()
}

fn valid_agent_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 40
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Run one read-only subagent to completion and return its report as the
/// tool outcome. Phase 4 of `upgrade-plan.md`: a subagent is an agent loop
/// whose "user" is the parent model.
///
/// This is fire-ingest-execute again, but self-contained: the child has no
/// UI, no approval popup, and no need for one -- it is offered nothing but
/// `tools::subagent_schemas()` and every call is re-checked against
/// `tools::subagent_call_allowed` before it runs, so the read-only guarantee
/// is enforced twice and the user is interrupted zero times. The parent's
/// conversation receives only what this returns; the child's transcript
/// lives and dies inside this function, which is the entire point -- research
/// at the cost of one tool result.
///
/// The stream is driven with `select!` in this task rather than spawned, so
/// when the user cancels the turn (aborting the runner task in
/// `execute_approved`), the child's in-flight request is dropped with it --
/// and the same abort kills any command the child had running, via the
/// `kill_on_drop` the command runner already sets. No orphaned stream, no
/// orphaned process, no tokens spent after Esc.
///
/// `progress` is how the child stays visible while it works: one
/// [`StreamEvent::AgentActivity`] per tool call it makes, sent on the same
/// channel as everything else so the stale-id guard covers it too. `None`
/// runs silently -- the loop is not entitled to a UI.
pub async fn run_subagent(
    call: &ToolCall,
    workspace: &Workspace,
    config: &Config,
    progress: Option<(u64, &mpsc::Sender<(u64, StreamEvent)>)>,
) -> ToolOutcome {
    let Some(tools::Action::Agent { task, agent_type }) = tools::describe_action(call) else {
        return ToolOutcome {
            call_id: call.id.clone(),
            display: "agent — bad arguments".to_string(),
            content: "Error: could not parse the arguments. Pass {\"task\": \"...\"} with a \
                      non-empty task describing what to research."
                .to_string(),
            diff: None,
            rollback: None,
        };
    };
    let definition = if agent_type == "explore" {
        None
    } else {
        let mut available = definitions(workspace.root());
        match available
            .iter()
            .position(|definition| definition.name == agent_type)
        {
            Some(index) => Some(available.swap_remove(index)),
            None => {
                let mut names = vec!["explore".to_string()];
                names.extend(available.into_iter().map(|definition| definition.name));
                return ToolOutcome {
                    call_id: call.id.clone(),
                    display: format!("agent — no such type '{agent_type}'"),
                    content: format!(
                        "Error: there is no '{agent_type}' subagent. Available types: {}. \
                         Omit agent_type for 'explore', and do any writing yourself.",
                        names.join(", ")
                    ),
                    diff: None,
                    rollback: None,
                };
            }
        }
    };

    // A writing agent runs in its own checkout. Read-only child behavior is
    // unchanged: same workspace, same tool list, same prompt.
    let write_mode = definition
        .as_ref()
        .is_some_and(|definition| definition.writes);
    if write_mode && config.tools.sandbox == crate::config::SandboxPolicy::ReadOnly {
        return ToolOutcome {
            call_id: call.id.clone(),
            display: format!("agent \"{}\" — disabled by sandbox", brief(&task, 40)),
            content: "Error: writing subagents cannot run while `[tools] sandbox = \
                      \"read-only\"`. Change the sandbox policy or use a read-only agent."
                .to_string(),
            diff: None,
            rollback: None,
        };
    }
    let worktree = if write_mode {
        let label = definition
            .as_ref()
            .map(|definition| definition.name.as_str())
            .unwrap_or("worker");
        match crate::worktree::Worktree::create(workspace.root(), label) {
            Ok(worktree) => Some(worktree),
            Err(error) => {
                return ToolOutcome {
                    call_id: call.id.clone(),
                    display: format!("agent \"{}\" — needs git", brief(&task, 40)),
                    content: format!(
                        "Error: a writing subagent needs a git repository with at least one \
                         commit so it can work in an isolated worktree. {error}"
                    ),
                    diff: None,
                    rollback: None,
                }
            }
        }
    } else {
        None
    };
    let child_workspace = match &worktree {
        Some(worktree) => match Workspace::new(worktree.path()) {
            Ok(workspace) => workspace,
            Err(error) => {
                worktree.remove();
                return ToolOutcome {
                    call_id: call.id.clone(),
                    display: format!("agent \"{}\" — worktree unusable", brief(&task, 40)),
                    content: format!("Error: the isolated worktree could not be used: {error}"),
                    diff: None,
                    rollback: None,
                };
            }
        },
        None => workspace.clone(),
    };

    let max_steps = config.tools.subagent_max_steps;
    let token_budget = config.tools.subagent_token_budget;
    // The parent's model unless the user pointed research at a faster one.
    let model = config
        .tools
        .subagent_model
        .as_deref()
        .unwrap_or(&config.llm.model);
    let mut history = vec![
        ChatMessage::text("system", String::new()), // rewritten every round
        ChatMessage::text("user", task.clone()),
    ];
    let mut steps = 0usize;
    let mut spent = 0usize;
    // How many rounds the child gets *after* its budget ran out. One is owed
    // -- the forced "answer now" round -- and a model that answers even that
    // with tool calls gets those refused once, then cut off. Without the hard
    // floor this loop's exit would depend on the model's cooperation.
    let mut overtime = 0usize;

    loop {
        // `0` means "no limit" for both budgets, matching `max_steps`'s own
        // convention -- an unlimited child keeps its schemas until it answers.
        let budget_left =
            (max_steps == 0 || steps < max_steps) && (token_budget == 0 || spent < token_budget);
        // Telling the prompt the budget is gone (whichever budget it was) is
        // what makes the last round an answer instead of another search.
        let mut system = if write_mode {
            tools::subagent_write_system_prompt(
                &child_workspace,
                if budget_left { steps } else { max_steps },
                max_steps,
            )
        } else {
            tools::subagent_system_prompt(
                &child_workspace,
                if budget_left { steps } else { max_steps },
                max_steps,
            )
        };
        // A definition extends the built-in prompt rather than replacing it:
        // the read-only rules and the budget warning stay in force.
        if let Some(definition) = &definition {
            system.push_str("\n\n");
            system.push_str(&definition.prompt);
        }
        history[0] = ChatMessage::text("system", system);
        let schemas = if budget_left {
            if write_mode {
                tools::subagent_write_schemas()
            } else {
                tools::subagent_schemas()
            }
        } else {
            Vec::new()
        };
        // Subagents never get a message with a non-empty `images` today --
        // `subagent_schemas()` never offers `check_in_browser`, the only
        // thing that ever populates it -- but this sums `images` anyway so
        // this budget estimate stays correct by construction rather than
        // by the coincidence of what subagents currently can't do.
        let request_chars: usize = history
            .iter()
            .map(|m| {
                m.content.as_deref().map_or(0, str::len)
                    + m.images.iter().map(|i| i.data_base64.len()).sum::<usize>()
            })
            .sum();

        let (tx, mut rx) = mpsc::channel(64);
        let stream = llm::stream_chat(
            llm::Target {
                endpoint: &config.llm.endpoint,
                model,
                api_key: &config.llm.api_key,
                max_tokens: config.llm.max_tokens,
                context_window: config.llm.effective_context_window(),
                include_usage: config.quota.include_usage,
                temperature: config.llm.effective_temperature(),
            },
            history.clone(),
            schemas,
            steps as u64,
            tx,
        );
        tokio::pin!(stream);
        let mut stream_finished = false;
        let mut text = String::new();
        let mut calls: Vec<ToolCall> = Vec::new();
        let mut usage: Option<llm::ApiUsage> = None;
        let error = loop {
            tokio::select! {
                _ = &mut stream, if !stream_finished => stream_finished = true,
                received = rx.recv() => match received {
                    Some((_, StreamEvent::Token(t))) => text.push_str(&t),
                    // The truncation flag is dropped here on purpose: a
                    // subagent's schemas never include write_file/edit_file
                    // (see `SUBAGENT_TOOLS`), so nothing it calls can act on
                    // it.
                    Some((_, StreamEvent::ToolCalls(c, _))) => calls = c,
                    Some((_, StreamEvent::Usage(u))) => usage = Some(u),
                    // Notices ("answer was truncated") are addressed to a
                    // user, and this loop has none; ToolsFinished and
                    // AgentActivity never arrive here -- the child runs its
                    // tools inline below, and only *sends* activity, upward.
                    // A child's reasoning is dropped along with the rest:
                    // the parent sees only the child's final report, so
                    // surfacing its thinking would put a second, competing
                    // narration on a screen that already has one.
                    Some((
                        _,
                        StreamEvent::Notice(_)
                        | StreamEvent::Reasoning(_)
                        | StreamEvent::ToolsFinished(_)
                        | StreamEvent::AgentActivity { .. },
                    )) => {}
                    Some((_, StreamEvent::Done)) | None => break None,
                    Some((_, StreamEvent::Error(e))) => break Some(e),
                },
            }
        };
        if let Some(e) = error {
            record_subagent_usage(spent, model);
            return finish_subagent(
                call,
                format!("agent \"{}\" — failed", brief(&task, 40)),
                format!(
                    "The subagent failed before finishing: {e}\n{}",
                    if write_mode {
                        "Do the task yourself, or try the agent again."
                    } else {
                        "Do the research yourself with read_file/grep_search/glob, or try the \
                         agent again."
                    }
                ),
                worktree.as_ref(),
                config.tools.max_output_bytes,
            );
        }

        // Exact counts when the endpoint reports them, the same
        // character-count-over-four estimate `usage.rs` describes when it
        // does not. Counted against the ceiling either way, so "no usage
        // reporting" never means "no budget".
        spent += usage
            .map(|u| u.total())
            .unwrap_or_else(|| (request_chars + text.len()) / 4);

        if calls.is_empty() {
            record_subagent_usage(spent, model);
            let report = text.trim().to_string();
            return finish_subagent(
                call,
                format!(
                    "agent \"{}\" — done ({} tool round{}, ~{}k tokens)",
                    brief(&task, 40),
                    steps,
                    if steps == 1 { "" } else { "s" },
                    (spent + 500) / 1000,
                ),
                if report.is_empty() {
                    "The subagent finished without producing a report.".to_string()
                } else {
                    report
                },
                worktree.as_ref(),
                config.tools.max_output_bytes,
            );
        }

        history.push(ChatMessage {
            role: "assistant".to_string(),
            content: (!text.trim().is_empty()).then_some(text),
            tool_calls: calls.clone(),
            tool_call_id: None,
            images: Vec::new(),
        });
        if !budget_left {
            overtime += 1;
            if overtime > 1 {
                // Refused once already and it asked again: stop paying.
                record_subagent_usage(spent, model);
                return finish_subagent(
                    call,
                    format!("agent \"{}\" — budget spent", brief(&task, 40)),
                    "The subagent spent its whole budget without writing a report.".to_string(),
                    worktree.as_ref(),
                    config.tools.max_output_bytes,
                );
            }
        }
        steps += 1;
        for c in &calls {
            // Judged before the activity event is sent, so a refused call is
            // *reported* as refused -- a trail that said "write notes.md"
            // about a write that never happened would be a lie in the UI.
            let gate = if !budget_left {
                Err(
                    "Your budget is spent. Write your report now, from what you have seen."
                        .to_string(),
                )
            } else if write_mode {
                tools::subagent_write_call_allowed(c)
            } else {
                tools::subagent_call_allowed(c)
            };
            if let Some((id, tx)) = progress {
                let mut label = tools::describe_action(c)
                    .map(|a| a.label())
                    .unwrap_or_else(|| c.function.name.clone());
                if gate.is_err() {
                    label.push_str(" — refused");
                }
                // Best-effort: a full channel or a gone receiver must never
                // stall or fail the child, so the error is dropped.
                let _ = tx
                    .send((
                        id,
                        StreamEvent::AgentActivity {
                            call_id: call.id.clone(),
                            label,
                            rounds: steps,
                        },
                    ))
                    .await;
            }
            // Answered in order, one message per call, even the refused ones
            // -- an unanswered tool call leaves the transcript invalid and
            // fails the *next* request instead of this one.
            let content = match gate {
                Ok(()) => tools::execute(c, &child_workspace, &config.tools).await.content,
                Err(reason) => reason,
            };
            history.push(ChatMessage {
                role: "tool".to_string(),
                content: Some(content),
                tool_calls: Vec::new(),
                tool_call_id: Some(c.id.clone()),
                images: Vec::new(),
            });
        }
    }
}

/// Turn a child's final state into the parent-visible outcome. A writing
/// child's worktree is captured as a patch here; the worktree is removed only
/// once the patch is safely in the text. If the patch is too large to include,
/// or cannot be produced, the worktree is left in place and its path is named
/// -- losing uncommitted work to cleanup would be worse than a stale dir.
fn finish_subagent(
    call: &ToolCall,
    display: String,
    mut content: String,
    worktree: Option<&crate::worktree::Worktree>,
    max_patch_bytes: usize,
) -> ToolOutcome {
    if let Some(worktree) = worktree {
        match worktree.patch() {
            Ok(patch) if patch.files.is_empty() => {
                content.push_str("\n\n(no files were changed)");
                worktree.remove();
            }
            Ok(patch) => {
                let plural = if patch.files.len() == 1 { "" } else { "s" };
                if patch.diff.chars().count() > max_patch_bytes {
                    content.push_str(&format!(
                        "\n\n--- patch ({} file{plural}) — too large to include ---\n\
                         The full worktree is at {} and was left in place.",
                        patch.files.len(),
                        worktree.path().display()
                    ));
                } else {
                    content.push_str(&format!(
                        "\n\n--- patch ({} file{plural}) ---\n{}",
                        patch.files.len(),
                        patch.diff
                    ));
                    worktree.remove();
                }
            }
            Err(error) => content.push_str(&format!(
                "\n\nThe worktree diff could not be captured ({error}); it was left at {}.",
                worktree.path().display()
            )),
        }
    }
    ToolOutcome {
        call_id: call.id.clone(),
        display,
        content,
        diff: None,
        rollback: None,
    }
}

/// A child's spend goes in the same ledger as everyone else's, attributed to
/// the model that actually spent it, so `/usage` stays honest when research
/// is routed to a different model. Per-agent attribution is Phase 5.4; this
/// is the part that cannot wait.
fn record_subagent_usage(spent: usize, model: &str) {
    // Tests run in one process with a process-global `$HOME`. The app's own
    // usage tests isolate it with a lock, but the agent tests do not, so a
    // child's ledger write can land inside another test's temporary home and
    // change its totals. `usage::record_turn` already has its own round-trip
    // test; skipping the write here keeps parallel tests deterministic
    // without touching the production path.
    #[cfg(test)]
    {
        let _ = (spent, model);
    }
    #[cfg(not(test))]
    crate::usage::record_turn(spent, model);
}

/// First `max` characters with an ellipsis -- for one-line transcript labels,
/// where `tools`' own `clip` would append a multi-line truncation notice.
fn brief(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let kept: String = s.chars().take(max).collect();
        format!("{kept}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitions_merge_user_and_project_and_skip_bad_files() {
        crate::config::test_support::with_isolated_home(|| {
            let workspace = tempfile::tempdir().expect("workspace");
            let user = crate::config::Config::config_dir().join("agents");
            let project = workspace.path().join(".covexcode").join("agents");
            std::fs::create_dir_all(&user).unwrap();
            std::fs::create_dir_all(&project).unwrap();
            let write = |dir: &Path, file: &str, name: &str, prompt: &str| {
                std::fs::write(
                    dir.join(file),
                    format!("name = \"{name}\"\ndescription = \"desc\"\nprompt = \"{prompt}\"\n"),
                )
                .unwrap();
            };
            write(&user, "reviewer.toml", "reviewer", "user prompt");
            // Project wins on a name collision.
            write(&project, "reviewer.toml", "reviewer", "project prompt");
            write(&project, "tests.toml", "tests", "test prompt");
            // Built-in names cannot be redefined, and bad files are skipped.
            write(&project, "explore.toml", "explore", "hijack");
            write(&project, "bad-name.toml", "bad name", "nope");
            std::fs::write(project.join("broken.toml"), "not toml").unwrap();

            let definitions = definitions(workspace.path());
            let names: Vec<&str> = definitions
                .iter()
                .map(|definition| definition.name.as_str())
                .collect();
            assert_eq!(names, vec!["reviewer", "tests"]);
            assert_eq!(definitions[0].prompt, "project prompt");
            assert_eq!(definitions[1].prompt, "test prompt");
        });
    }

    #[test]
    fn a_definition_can_declare_itself_a_writer() {
        crate::config::test_support::with_isolated_home(|| {
            let workspace = tempfile::tempdir().expect("workspace");
            let project = workspace.path().join(".covexcode").join("agents");
            std::fs::create_dir_all(&project).unwrap();
            std::fs::write(
                project.join("worker.toml"),
                "name = \"worker\"\ndescription = \"edit files\"\nprompt = \"do it\"\n\
                 writes = true\n",
            )
            .unwrap();

            let definitions = definitions(workspace.path());
            assert_eq!(definitions.len(), 1);
            assert_eq!(definitions[0].name, "worker");
            assert!(definitions[0].writes);
        });
    }
}
