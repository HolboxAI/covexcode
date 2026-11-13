/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! Background teammates: subagents that outlive the tool call that started
//! them.
//!
//! A foreground `agent` call blocks the parent's turn until the child reports.
//! A call with `background: true` instead starts the child in its own task and
//! returns immediately, so the parent can keep working, start more teammates,
//! or answer the user. The child's report is kept here until the parent reads
//! it with the `teammates` tool, which can also list running jobs and abort
//! one.
//!
//! Scope of this first version, deliberately narrow:
//!
//! - **Read-only types only.** A writing child would apply its patch from a
//!   detached task at a moment the requester is not watching; that is a
//!   decision the foreground path already asks about, so it is refused here
//!   rather than made implicit.
//! - **Process-local.** Jobs are tasks in this covexcode process, with no
//!   supervision after a daemon or CLI exits. The mailbox remains the
//!   cross-process channel; a teammate's report can also be mailed.
//! - **No live event stream.** Progress is not forwarded to `/agents` yet;
//!   the parent polls `teammates list`/`report`. Forwarding needs the engine's
//!   event sink to survive the call that created it, which is follow-up work.
//!
//! The cap on running jobs reuses `tools.subagent_concurrency` so background
//! work cannot quietly multiply the token bill past the same budget the
//! foreground fan-out already respects.

use crate::config::Config;
use crate::llm::ToolCall;
use crate::tools::{self, Action, ToolOutcome};
use crate::workspace::Workspace;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

/// Finished reports nobody collected still hold child output in memory; past
/// this many jobs, completed ones are dropped to make room.
const MAX_JOBS: usize = 24;
/// A run with no configured concurrency limit still needs a ceiling: every
/// child is a model loop spending tokens.
const MAX_UNCONFIGURED_RUNNING: usize = 8;

static NEXT: AtomicU64 = AtomicU64::new(1);

struct Job {
    id: String,
    name: String,
    task: String,
    /// The workspace that started it. A daemon can host sessions in several
    /// projects; one project's jobs must not be visible in another.
    workspace: PathBuf,
    started: Instant,
    /// Filled by the spawned task when it returns. An `Arc` so the task can
    /// write it without touching the registry, which may be locked while a
    /// report is being collected.
    outcome: Arc<Mutex<Option<ToolOutcome>>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Job {
    fn report_ready(&self) -> bool {
        self.outcome
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    fn finished_without_report(&self) -> bool {
        !self.report_ready()
            && self
                .handle
                .as_ref()
                .is_some_and(|handle| handle.is_finished())
    }
}

static JOBS: OnceLock<Mutex<BTreeMap<String, Job>>> = OnceLock::new();

fn jobs() -> &'static Mutex<BTreeMap<String, Job>> {
    JOBS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Started {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub id: String,
    pub name: String,
    pub task: String,
    pub running: bool,
    pub has_report: bool,
    pub elapsed_secs: u64,
}

/// Start the background teammate a call describes. The call must be an
/// `agent` call with `background: true`; the engine only routes those here.
pub fn spawn(call: ToolCall, workspace: Workspace, config: Config) -> Result<Started, String> {
    let Some(Action::Agent {
        task,
        agent_type,
        background: true,
    }) = tools::describe_action(&call)
    else {
        return Err("only an agent call marked `background: true` starts a teammate".to_string());
    };
    if definition_writes(&workspace, &agent_type) {
        return Err(format!(
            "'{agent_type}' is a writing agent. Background teammates are read-only for now, \
             because a writing teammate's patch would be applied while the requester is not \
             watching. Run it in the foreground, or use a read-only type such as explore."
        ));
    }

    // Belt and braces: even if an agent definition changes between this
    // check and the child's own resolution, the teammate runs under the
    // read-only sandbox. `run_subagent` itself refuses a writing agent under
    // that policy, so the detached task cannot land a patch by a race.
    let mut config = config;
    config.tools.sandbox = crate::config::SandboxPolicy::ReadOnly;

    let limit = if config.tools.subagent_concurrency == 0 {
        MAX_UNCONFIGURED_RUNNING
    } else {
        config.tools.subagent_concurrency.max(1)
    };
    let root = workspace.root().to_path_buf();
    let mut map = jobs().lock().unwrap_or_else(|e| e.into_inner());
    let running = map
        .values()
        .filter(|job| job.workspace == root)
        .filter(|job| !job.report_ready() && !job.finished_without_report())
        .count();
    if running >= limit {
        return Err(format!(
            "{running} teammates are already running (the limit is {limit}); collect a report \
             or stop one before starting another"
        ));
    }
    if map.len() >= MAX_JOBS {
        // Drop the oldest completed job. A finished job with an uncollected
        // report is only discarded when there is no alternative -- the error
        // below says so rather than silently losing a report.
        let mut removable: Vec<(Instant, String)> = map
            .iter()
            .filter(|(_, job)| job.finished_without_report())
            .map(|(id, job)| (job.started, id.clone()))
            .collect();
        removable.sort_by_key(|(started, _)| *started);
        for (_, id) in removable {
            map.remove(&id);
            if map.len() < MAX_JOBS {
                break;
            }
        }
        if map.len() >= MAX_JOBS {
            return Err(format!(
                "too many teammates ({MAX_JOBS}); collect finished reports with \
                 teammates {{ action: \"report\" }} or stop one first"
            ));
        }
    }

    let number = NEXT.fetch_add(1, Ordering::Relaxed);
    let id = format!("teammate-{number}");
    let name = format!("{agent_type}-{number}");
    let outcome = Arc::new(Mutex::new(None));
    map.insert(
        id.clone(),
        Job {
            id: id.clone(),
            name: name.clone(),
            task,
            workspace: root,
            started: Instant::now(),
            outcome: outcome.clone(),
            handle: None,
        },
    );
    // Keep the task short: it only runs the child and stores the outcome.
    let slot = outcome.clone();
    let handle = tokio::spawn(async move {
        let outcome = crate::subagent::run_subagent(&call, &workspace, &config, None).await;
        *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome);
    });
    if let Some(job) = map.get_mut(&id) {
        job.handle = Some(handle);
    }
    Ok(Started { id, name })
}

/// Jobs for `workspace`, in a stable order, newest id last.
pub fn list(workspace: &Path) -> Vec<View> {
    let map = jobs().lock().unwrap_or_else(|e| e.into_inner());
    map.values()
        .filter(|job| job.workspace == workspace)
        .map(|job| View {
            id: job.id.clone(),
            name: job.name.clone(),
            task: job.task.clone(),
            running: !job.report_ready() && !job.finished_without_report(),
            has_report: job.report_ready(),
            elapsed_secs: job.started.elapsed().as_secs(),
        })
        .collect()
}

/// Take a finished teammate's report and remove the job. A still-running job
/// is left in place; a job that ended without a report (aborted or panicked)
/// is removed and reported as such.
pub fn report(workspace: &Path, id: &str) -> Result<String, String> {
    let mut map = jobs().lock().unwrap_or_else(|e| e.into_inner());
    let Some(job) = map.get(id).filter(|job| job.workspace == workspace) else {
        return Err(format!("there is no teammate '{id}'"));
    };
    let outcome = job
        .outcome
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let ended_without_report = job.finished_without_report();
    let name = job.name.clone();
    if let Some(outcome) = outcome {
        let content = outcome.content;
        map.remove(id);
        return Ok(content);
    }
    if ended_without_report {
        map.remove(id);
        return Err(format!(
            "teammate '{id}' ({name}) ended without a report -- it was stopped, or its task \
             panicked"
        ));
    }
    Err(format!(
        "teammate '{id}' is still running; check teammates {{ action: \"list\" }}"
    ))
}

/// Abort a teammate and remove it. Its report, if it had already finished, is
/// discarded.
pub fn stop(workspace: &Path, id: &str) -> Result<String, String> {
    let mut map = jobs().lock().unwrap_or_else(|e| e.into_inner());
    if !map.get(id).is_some_and(|job| job.workspace == workspace) {
        return Err(format!("there is no teammate '{id}'"));
    }
    let Some(job) = map.remove(id) else {
        return Err(format!("there is no teammate '{id}'"));
    };
    if let Some(handle) = job.handle {
        handle.abort();
    }
    Ok(job.name)
}

fn definition_writes(workspace: &Workspace, agent_type: &str) -> bool {
    if agent_type == "explore" {
        return false;
    }
    if let Some(definition) = crate::subagent::definitions(workspace.root())
        .into_iter()
        .find(|definition| definition.name == agent_type)
    {
        return definition.writes;
    }
    crate::subagent::builtin(agent_type)
        .map(|definition| definition.writes)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::FunctionCall;
    use serde_json::json;

    fn background_call(agent_type: &str) -> ToolCall {
        ToolCall {
            id: "call_bg".to_string(),
            kind: "function".to_string(),
            function: FunctionCall {
                name: tools::AGENT.to_string(),
                arguments: json!({
                    "task": "look around",
                    "agent_type": agent_type,
                    "background": true
                })
                .to_string(),
            },
        }
    }

    fn workspace() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("temp dir");
        let ws = Workspace::new(dir.path()).expect("workspace");
        (dir, ws)
    }

    fn config(endpoint: &str) -> Config {
        let mut config = Config::default();
        config.llm.endpoint = endpoint.to_string();
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();
        config
    }

    #[test]
    fn a_writing_agent_cannot_start_in_the_background() {
        crate::config::test_support::with_isolated_home(|| {
            let (dir, ws) = workspace();
            let agents = dir.path().join(".covexcode").join("agents");
            std::fs::create_dir_all(&agents).unwrap();
            std::fs::write(
                agents.join("worker.toml"),
                "name = \"worker\"\nwrites = true\nprompt = \"edit\"\n",
            )
            .unwrap();
            let error = spawn(background_call("worker"), ws, config("http://127.0.0.1:9"))
                .expect_err("writing is refused");
            assert!(error.contains("read-only"), "{error}");
        });
    }

    #[tokio::test]
    async fn a_report_can_be_collected_and_the_job_removed() {
        // An unknown type returns its refusal without touching the network,
        // which makes the job finish deterministically.
        let (_dir, ws) = workspace();
        let root = ws.root().to_path_buf();
        let started = spawn(
            background_call("nonsense"),
            ws,
            config("http://127.0.0.1:9"),
        )
        .expect("spawn");
        assert!(list(&root).iter().any(|job| job.id == started.id));

        let mut collected = None;
        for _ in 0..100 {
            match report(&root, &started.id) {
                Ok(text) => {
                    collected = Some(text);
                    break;
                }
                Err(e) if e.contains("still running") => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(e) => panic!("unexpected report error: {e}"),
            }
        }
        let text = collected.expect("the job finishes");
        assert!(text.contains("Error"), "{text}");
        assert!(
            report(&root, &started.id).is_err(),
            "the job is removed once collected"
        );
    }

    #[tokio::test]
    async fn another_workspace_cannot_see_or_collect_a_job() {
        let (_dir_a, ws_a) = workspace();
        let (_dir_b, ws_b) = workspace();
        let root_a = ws_a.root().to_path_buf();
        let root_b = ws_b.root().to_path_buf();
        let started = spawn(
            background_call("nonsense"),
            ws_a,
            config("http://127.0.0.1:9"),
        )
        .expect("spawn");

        assert!(list(&root_a).iter().any(|job| job.id == started.id));
        assert!(list(&root_b).is_empty(), "jobs are per workspace");
        assert!(
            report(&root_b, &started.id).is_err(),
            "a report cannot be collected from another project"
        );

        // Collect it from its own workspace so the global registry does not
        // carry it into other tests.
        let mut collected = None;
        for _ in 0..100 {
            match report(&root_a, &started.id) {
                Ok(text) => {
                    collected = Some(text);
                    break;
                }
                Err(e) if e.contains("still running") => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(e) => panic!("unexpected report error: {e}"),
            }
        }
        assert!(collected.is_some());
    }

    #[tokio::test]
    async fn stop_removes_a_running_teammate() {
        // Accept connections but never answer, so the child stays in its
        // request until it is aborted.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    held.push(socket);
                }
            }
        });

        let (_dir, ws) = workspace();
        let root = ws.root().to_path_buf();
        let started = spawn(
            background_call("explore"),
            ws,
            config(&format!("http://{addr}")),
        )
        .expect("spawn");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            list(&root)
                .iter()
                .any(|job| job.id == started.id && job.running),
            "the child is in flight"
        );

        stop(&root, &started.id).expect("stop");
        assert!(!list(&root).iter().any(|job| job.id == started.id));
        server.abort();
    }
}
