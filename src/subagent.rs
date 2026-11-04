/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! The read-only `explore` subagent: a child agent loop whose "user" is the
//! parent model.
//!
//! It lives in its own module rather than in `agent.rs` because `agent.rs`
//! also owns the TUI loop and imports `App`; the ACP-only runtime deliberately
//! omits the TUI, and it still needs subagents. Nothing here touches `App`.

use crate::config::Config;
use crate::llm::{self, ChatMessage, StreamEvent, ToolCall};
use crate::tools::{self, ToolOutcome};
use crate::workspace::Workspace;
use tokio::sync::mpsc;

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
    if agent_type != "explore" {
        return ToolOutcome {
            call_id: call.id.clone(),
            display: format!("agent — no such type '{agent_type}'"),
            content: format!(
                "Error: there is no '{agent_type}' subagent. Only 'explore' (read-only \
                 research) exists; omit agent_type to get it, and do any writing yourself."
            ),
            diff: None,
            rollback: None,
        };
    }

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
        history[0] = ChatMessage::text(
            "system",
            tools::subagent_system_prompt(
                workspace,
                if budget_left { steps } else { max_steps },
                max_steps,
            ),
        );
        let schemas = if budget_left {
            tools::subagent_schemas()
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
            return ToolOutcome {
                call_id: call.id.clone(),
                display: format!("agent \"{}\" — failed", brief(&task, 40)),
                content: format!(
                    "The subagent failed before finishing: {e}\nDo the research yourself \
                     with read_file/grep_search/glob, or try the agent again."
                ),
                diff: None,
                rollback: None,
            };
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
            return ToolOutcome {
                call_id: call.id.clone(),
                display: format!(
                    "agent \"{}\" — done ({} tool round{}, ~{}k tokens)",
                    brief(&task, 40),
                    steps,
                    if steps == 1 { "" } else { "s" },
                    (spent + 500) / 1000,
                ),
                content: if report.is_empty() {
                    "The subagent finished without producing a report.".to_string()
                } else {
                    report
                },
                diff: None,
                rollback: None,
            };
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
                return ToolOutcome {
                    call_id: call.id.clone(),
                    display: format!("agent \"{}\" — budget spent", brief(&task, 40)),
                    content: "The subagent spent its whole budget without writing a report."
                        .to_string(),
                    diff: None,
                    rollback: None,
                };
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
                Ok(()) => tools::execute(c, workspace, &config.tools).await.content,
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
