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
use crate::llm::{FunctionCall, StreamEvent, ToolCall};
use crate::locked_store::{self, Document};
use crate::tools::{self, Action, ToolOutcome};
use crate::workspace::Workspace;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime};

/// Finished reports nobody collected still hold child output in memory; past
/// this many jobs, completed ones are dropped to make room.
const MAX_JOBS: usize = 24;
/// A run with no configured concurrency limit still needs a ceiling: every
/// child is a model loop spending tokens.
const MAX_UNCONFIGURED_RUNNING: usize = 8;
/// Reports kept on disk for a later process to collect. Reaching this many
/// refuses a new job rather than silently discarding an unread report.
const MAX_PERSISTED: usize = 50;
/// A persisted "running" record older than this is assumed lost: a teammate
/// that outlived a day belongs to a process that is gone.
const RUNNING_TTL_SECS: u64 = 24 * 60 * 60;

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

/// Progress a teammate reports while it runs. `id` is the teammate id; a
/// frontend that shows trails keys on the parent `agent` call instead, so the
/// caller's sink decides how to translate it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Activity {
    Step { label: String, rounds: usize },
    Finished { summary: String },
}

/// Called from the teammate's task for each activity; must be cheap and must
/// not block on the teammate itself.
pub type ActivitySink = Arc<dyn Fn(&str, Activity) + Send + Sync>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub id: String,
    pub name: String,
    pub task: String,
    pub running: bool,
    pub has_report: bool,
    pub elapsed_secs: u64,
    /// The process that started it. A job can be visible here after the
    /// process that ran it is gone; `remote` says it belongs to a different
    /// process than the one listing.
    pub pid: u32,
    pub remote: bool,
    /// The owning process is gone (or the record is stale): the job cannot
    /// finish unless it is retried.
    pub interrupted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordStatus {
    Running,
    Finished,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Record {
    id: String,
    name: String,
    task: String,
    pid: u32,
    started_at: u64,
    status: RecordStatus,
    report: Option<String>,
    /// The original `agent` call's arguments JSON, enough to retry the job
    /// after the process that started it has exited.
    #[serde(default)]
    arguments: String,
}

#[derive(Serialize, Deserialize)]
struct Persisted {
    workspace: String,
    jobs: BTreeMap<String, Record>,
}

impl Document for Persisted {
    fn workspace(&self) -> &str {
        &self.workspace
    }

    fn set_workspace(&mut self, workspace: String) {
        self.workspace = workspace;
    }
}

fn empty_persisted(workspace: String) -> Persisted {
    Persisted {
        workspace,
        jobs: BTreeMap::new(),
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn persist_spawn(workspace: &Path, record: Record) -> Result<(), String> {
    locked_store::update("teammates", workspace, true, empty_persisted, |store| {
        store.jobs.retain(|_, job| {
            job.status == RecordStatus::Finished || now_secs() < job.started_at + RUNNING_TTL_SECS
        });
        if store.jobs.len() >= MAX_PERSISTED {
            return Err(format!(
                "too many persisted teammate reports ({MAX_PERSISTED}); collect them with \
                 teammates {{ action: \"report\" }} first"
            ));
        }
        store.jobs.insert(record.id.clone(), record);
        Ok(())
    })
}

fn persist_finish(workspace: &Path, id: &str, report: &str) {
    let _ = locked_store::update("teammates", workspace, true, empty_persisted, |store| {
        if let Some(record) = store.jobs.get_mut(id) {
            record.status = RecordStatus::Finished;
            record.report = Some(report.to_string());
        }
        Ok(())
    });
}

fn persist_remove(workspace: &Path, id: &str) {
    let _ = locked_store::update("teammates", workspace, true, empty_persisted, |store| {
        store.jobs.remove(id);
        Ok(())
    });
}

fn persisted(workspace: &Path) -> Vec<Record> {
    locked_store::update("teammates", workspace, false, empty_persisted, |store| {
        Ok(store.jobs.values().cloned().collect())
    })
    .unwrap_or_default()
}

/// Remove and return a finished persisted report. A record that is still
/// running is left in place and named in the error.
fn take_persisted(workspace: &Path, id: &str) -> Result<Option<String>, String> {
    locked_store::update("teammates", workspace, true, empty_persisted, |store| {
        let Some(record) = store.jobs.get(id) else {
            return Ok(None);
        };
        if record.status != RecordStatus::Finished {
            let interrupted = record.pid == std::process::id()
                || process_alive(record.pid) == Some(false)
                || now_secs() >= record.started_at + RUNNING_TTL_SECS;
            if interrupted {
                // Keep the record: it holds the stored call that `retry`
                // needs. Removing it here would lose the only way to resume.
                return Err(format!(
                    "teammate '{id}' was interrupted before it reported; retry it with \
                     teammates {{ action: \"retry\", id: \"{id}\" }}"
                ));
            }
            return Err(format!(
                "teammate '{id}' is still running in process {}; it was started by another \
                 covexcode process and cannot be collected until it finishes",
                record.pid
            ));
        }
        let report = record.report.clone().unwrap_or_default();
        store.jobs.remove(id);
        Ok(Some(report))
    })
}

/// Start the background teammate a call describes. The call must be an
/// `agent` call with `background: true`; the engine only routes those here.
pub fn spawn(
    call: ToolCall,
    workspace: Workspace,
    config: Config,
    activity: Option<ActivitySink>,
) -> Result<Started, String> {
    let Some(Action::Agent {
        task,
        agent_type,
        background: true,
    }) = tools::describe_action(&call)
    else {
        return Err("only an agent call marked `background: true` starts a teammate".to_string());
    };
    let writes = definition_writes(&workspace, &agent_type);
    if writes && !config.tools.background_writes {
        return Err(format!(
            "'{agent_type}' is a writing agent. A background teammate applies its patch from a \
             detached task, after the turn that approved it has moved on, so writing types \
             stay foreground-only unless `[tools] background_writes = true` is set. Run it in \
             the foreground, or opt in explicitly."
        ));
    }

    // Belt and braces for the read-only posture: even if an agent definition
    // changes between this check and the child's own resolution, a teammate
    // that was allowed as read-only runs under the read-only sandbox.
    // `run_subagent` itself refuses a writing agent under that policy, so such
    // a change cannot land a patch. An opted-in writing teammate keeps the
    // configured sandbox (and its worktree).
    let mut config = config;
    if !writes {
        config.tools.sandbox = crate::config::SandboxPolicy::ReadOnly;
    }

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
    // Persist before starting: a job record on disk is what lets a later
    // process collect the report, and failing here must not leave a running
    // child nobody can find.
    persist_spawn(
        &root,
        Record {
            id: id.clone(),
            name: name.clone(),
            task: task.clone(),
            pid: std::process::id(),
            started_at: now_secs(),
            status: RecordStatus::Running,
            report: None,
            arguments: call.function.arguments.clone(),
        },
    )?;
    let outcome = Arc::new(Mutex::new(None));
    map.insert(
        id.clone(),
        Job {
            id: id.clone(),
            name: name.clone(),
            task,
            workspace: root.clone(),
            started: Instant::now(),
            outcome: outcome.clone(),
            handle: None,
        },
    );
    // Keep the task short: it only runs the child, stores the outcome, and
    // writes the report to the persisted store for a later process.
    let slot = outcome.clone();
    let job_id = id.clone();
    let event_id = id.clone();
    let handle = tokio::spawn(async move {
        // Drive the child and its progress channel in this task, the same
        // shape the foreground fan-out uses. The sink is the only way a
        // background child reaches the parent's live view.
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(64);
        let runner =
            crate::subagent::run_subagent(&call, &workspace, &config, Some((0, &progress_tx)));
        tokio::pin!(runner);
        let outcome = loop {
            tokio::select! {
                outcome = &mut runner => break outcome,
                Some((_, event)) = progress_rx.recv() => {
                    if let Some(activity) = &activity {
                        if let StreamEvent::AgentActivity { label, rounds, .. } = event {
                            activity(&event_id, Activity::Step { label, rounds });
                        }
                    }
                }
            }
        };
        if let Some(activity) = &activity {
            let summary = if outcome.content.starts_with("Error:") {
                "failed"
            } else {
                "done"
            };
            activity(
                &event_id,
                Activity::Finished {
                    summary: summary.to_string(),
                },
            );
        }
        let report = outcome.content.clone();
        // Persist before publishing the in-memory outcome: anyone who sees
        // "report ready" can then trust that a later process could collect it
        // too.
        persist_finish(&root, &job_id, &report);
        *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome);
    });
    if let Some(job) = map.get_mut(&id) {
        job.handle = Some(handle);
    }
    Ok(Started { id, name })
}

/// Whether `pid` is alive. `None` when the platform's probe is unavailable,
/// in which case a persisted running record is treated as alive rather than
/// guessed dead.
fn process_alive(pid: u32) -> Option<bool> {
    #[cfg(unix)]
    {
        // `kill -0` sends no signal; success means the process exists (or is
        // owned by someone else and merely un-signalable, which still means
        // it exists).
        let status = std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;
        Some(status.success())
    }
    #[cfg(windows)]
    {
        let filter = format!("PID eq {pid}");
        let output = std::process::Command::new("tasklist")
            .args(["/FI", &filter, "/NH"])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout);
        Some(text.contains(&pid.to_string()))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        None
    }
}

/// Jobs for `workspace`, in a stable order, newest id last. Includes records
/// persisted by a previous process: a finished report stays collectable after
/// the process that produced it exits, and a running record whose process is
/// gone is reported as interrupted (and can be retried).
pub fn list(workspace: &Path) -> Vec<View> {
    let mut views: Vec<View> = {
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
                pid: std::process::id(),
                remote: false,
                interrupted: false,
            })
            .collect()
    };
    let memory: std::collections::BTreeSet<String> =
        views.iter().map(|view| view.id.clone()).collect();
    let now = now_secs();
    for record in persisted(workspace) {
        if memory.contains(&record.id) {
            continue;
        }
        let interrupted = record.status == RecordStatus::Running
            && (record.pid == std::process::id()
                || process_alive(record.pid) == Some(false)
                || now >= record.started_at + RUNNING_TTL_SECS);
        views.push(View {
            id: record.id.clone(),
            name: record.name.clone(),
            task: record.task.clone(),
            running: record.status == RecordStatus::Running && !interrupted,
            has_report: record.status == RecordStatus::Finished && record.report.is_some(),
            elapsed_secs: now.saturating_sub(record.started_at),
            pid: record.pid,
            remote: record.pid != std::process::id(),
            interrupted,
        });
    }
    views.sort_by(|a, b| a.id.cmp(&b.id));
    views
}

/// Take a finished teammate's report and remove the job. A still-running job
/// is left in place; a job that ended without a report (aborted or panicked)
/// is removed and reported as such. A report persisted by a previous process
/// is collected from disk.
pub fn report(workspace: &Path, id: &str) -> Result<String, String> {
    let collected = {
        let mut map = jobs().lock().unwrap_or_else(|e| e.into_inner());
        match map.get(id).filter(|job| job.workspace == workspace) {
            Some(job) => {
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
                    Some(Ok(content))
                } else if ended_without_report {
                    map.remove(id);
                    Some(Err(format!(
                        "teammate '{id}' ({name}) ended without a report -- it was stopped, or \
                         its task panicked"
                    )))
                } else {
                    Some(Err(format!(
                        "teammate '{id}' is still running; check teammates \
                         {{ action: \"list\" }}"
                    )))
                }
            }
            None => None,
        }
    };
    if let Some(result) = collected {
        persist_remove(workspace, id);
        return result;
    }
    match take_persisted(workspace, id)? {
        Some(report) => Ok(report),
        None => Err(format!("there is no teammate '{id}'")),
    }
}

/// Abort a teammate and remove it. Its report, if it had already finished, is
/// discarded. A job owned by another live process cannot be aborted from
/// here; a finished or stale persisted record is simply removed.
pub fn stop(workspace: &Path, id: &str) -> Result<String, String> {
    let name = {
        let mut map = jobs().lock().unwrap_or_else(|e| e.into_inner());
        if map.get(id).is_some_and(|job| job.workspace == workspace) {
            let job = map.remove(id).expect("checked above");
            if let Some(handle) = job.handle {
                handle.abort();
            }
            Some(job.name)
        } else {
            None
        }
    };
    if let Some(name) = name {
        persist_remove(workspace, id);
        return Ok(name);
    }
    locked_store::update("teammates", workspace, true, empty_persisted, |store| {
        let Some(record) = store.jobs.get(id) else {
            return Err(format!("there is no teammate '{id}'"));
        };
        if record.status == RecordStatus::Running {
            let interrupted = record.pid == std::process::id()
                || process_alive(record.pid) == Some(false)
                || now_secs() >= record.started_at + RUNNING_TTL_SECS;
            if !interrupted {
                return Err(format!(
                    "teammate '{id}' is running in process {}; it cannot be stopped from another \
                     covexcode process",
                    record.pid
                ));
            }
        }
        let name = record.name.clone();
        store.jobs.remove(id);
        Ok(name)
    })
}

/// Resume an interrupted teammate from the call stored in its persisted
/// record. Only a running record whose process is gone qualifies; a finished
/// job has a report to collect, and a live job must be stopped first.
///
/// The replacement is a new job with a new id, because the old id's record is
/// removed in the same locked step that hands back its call.
pub fn retry(workspace: &Path, id: &str, config: &Config) -> Result<Started, String> {
    // The in-memory registry only holds jobs owned by this process, and one
    // of them is alive by definition.
    if jobs()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(id)
        .is_some_and(|job| job.workspace == workspace)
    {
        return Err(format!(
            "teammate '{id}' is still running in this process; stop it before retrying"
        ));
    }

    let arguments = locked_store::update("teammates", workspace, true, empty_persisted, |store| {
        let Some(record) = store.jobs.get(id) else {
            return Err(format!("there is no teammate '{id}'"));
        };
        if record.status == RecordStatus::Finished {
            return Err(format!(
                "teammate '{id}' already finished; collect its report instead of retrying"
            ));
        }
        let interrupted = record.pid == std::process::id()
            || process_alive(record.pid) == Some(false)
            || now_secs() >= record.started_at + RUNNING_TTL_SECS;
        if !interrupted {
            return Err(format!(
                "teammate '{id}' is still running in process {}; it cannot be retried while it \
                 is alive",
                record.pid
            ));
        }
        let arguments = record.arguments.clone();
        if arguments.trim().is_empty() {
            return Err(format!(
                "teammate '{id}' has no stored call to retry (it was started by an older \
                 version)"
            ));
        }
        store.jobs.remove(id);
        Ok(arguments)
    })?;

    let call = ToolCall {
        id: format!("call_retry_{id}"),
        kind: "function".to_string(),
        function: FunctionCall {
            name: tools::AGENT.to_string(),
            arguments,
        },
    };
    let workspace = Workspace::new(workspace)
        .map_err(|e| format!("the teammate's workspace could not be reopened: {e}"))?;
    spawn(call, workspace, config.clone(), None)
}

/// Forget a job in memory without touching its persisted record, simulating
/// the process that ran it going away.
#[cfg(test)]
fn forget_memory(workspace: &Path, id: &str) {
    let mut map = jobs().lock().unwrap_or_else(|e| e.into_inner());
    if map.get(id).is_some_and(|job| job.workspace == workspace) {
        let job = map.remove(id).expect("checked above");
        if let Some(handle) = job.handle {
            handle.abort();
        }
    }
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

    /// A pid that is certainly gone: spawn a process that exits immediately
    /// and wait for it.
    fn dead_pid() -> u32 {
        let mut child = if cfg!(windows) {
            std::process::Command::new("cmd")
                .args(["/C", "exit"])
                .spawn()
                .expect("spawn a short-lived process")
        } else {
            std::process::Command::new("true")
                .spawn()
                .expect("spawn a short-lived process")
        };
        let pid = child.id();
        child.wait().expect("the process exits");
        pid
    }

    #[test]
    fn process_liveness_is_probed() {
        assert_eq!(process_alive(std::process::id()), Some(true));
        assert_eq!(process_alive(dead_pid()), Some(false));
    }

    fn seed_interrupted(root: &Path, id: &str, task: &str) {
        persist_spawn(
            root,
            Record {
                id: id.to_string(),
                name: "nonsense-900".to_string(),
                task: task.to_string(),
                pid: dead_pid(),
                started_at: now_secs(),
                status: RecordStatus::Running,
                report: None,
                arguments: json!({
                    "task": task,
                    "agent_type": "nonsense",
                    "background": true
                })
                .to_string(),
            },
        )
        .expect("seed the interrupted record");
    }

    #[tokio::test]
    async fn an_interrupted_teammate_can_be_retried() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace();
        let root = ws.root().to_path_buf();
        seed_interrupted(&root, "teammate-900", "resume me");

        let view = list(&root)
            .into_iter()
            .find(|job| job.id == "teammate-900")
            .expect("the record is visible");
        assert!(view.interrupted, "{view:?}");
        assert!(!view.running && !view.has_report, "{view:?}");

        let error = report(&root, "teammate-900").expect_err("interrupted has no report");
        assert!(error.contains("retry"), "{error}");

        let started = retry(&root, "teammate-900", &config("http://127.0.0.1:9"))
            .expect("retry starts a new job");
        assert_ne!(started.id, "teammate-900");
        assert!(
            !list(&root).iter().any(|job| job.id == "teammate-900"),
            "the old record is consumed by the retry"
        );

        // The replacement runs the stored call (an unknown type, so it ends
        // without a network request) and its report is collectable.
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
        assert!(collected.expect("a report").contains("Error"));
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
            let error = spawn(
                background_call("worker"),
                ws,
                config("http://127.0.0.1:9"),
                None,
            )
            .expect_err("writing is refused");
            assert!(error.contains("background_writes"), "{error}");
        });
    }

    #[tokio::test]
    async fn a_report_can_be_collected_and_the_job_removed() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        // An unknown type returns its refusal without touching the network,
        // which makes the job finish deterministically.
        let (_dir, ws) = workspace();
        let root = ws.root().to_path_buf();
        let started = spawn(
            background_call("nonsense"),
            ws,
            config("http://127.0.0.1:9"),
            None,
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
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir_a, ws_a) = workspace();
        let (_dir_b, ws_b) = workspace();
        let root_a = ws_a.root().to_path_buf();
        let root_b = ws_b.root().to_path_buf();
        let started = spawn(
            background_call("nonsense"),
            ws_a,
            config("http://127.0.0.1:9"),
            None,
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
    async fn a_finished_report_survives_the_process_that_produced_it() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace();
        let root = ws.root().to_path_buf();
        let started = spawn(
            background_call("nonsense"),
            ws,
            config("http://127.0.0.1:9"),
            None,
        )
        .expect("spawn");

        let mut ready = false;
        for _ in 0..100 {
            if list(&root)
                .iter()
                .any(|job| job.id == started.id && job.has_report)
            {
                ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(ready, "the job finishes");

        // The record on disk is the only copy now, as if this process had
        // exited after the child finished.
        forget_memory(&root, &started.id);
        let text = report(&root, &started.id).expect("report from disk");
        assert!(text.contains("Error"), "{text}");
        assert!(
            report(&root, &started.id).is_err(),
            "collecting removes the persisted record"
        );
    }

    #[tokio::test]
    async fn stop_removes_a_running_teammate() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
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
            None,
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
