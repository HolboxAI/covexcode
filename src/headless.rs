//! A headless, RPC-driven counterpart to `App`'s agent loop -- what
//! `protocol.rs`'s ACP types actually get driven by. `App` itself is
//! untouched by this module; see `approval.rs`'s `Verdict`/`verdict_for`
//! docs for why the two don't share state, only the one piece of logic
//! (`verdict_for`) where drift would be dangerous.
//!
//! Deliberately narrower than `App` for a first working version, matching
//! an independent review's explicit recommendation (see `approval.rs`):
//! - Plan mode is supported. A `session/prompt` may carry `mode: "plan"`,
//!   which runs the turn read-only (`verdict_for` refuses writes with
//!   "describe it in your plan"); the model proposes via `exit_plan_mode`,
//!   and the usual `session/request_permission` flow either approves it
//!   (→ `plan.md` written, `mode` flips back to `Normal`, implementation
//!   proceeds) or declines it (→ stay in plan mode, revise).
//! - No deploy tool. `Action::Deploy` needs `deploy_takes_over`'s real
//!   terminal-based OAuth flow (`app.rs`'s own doc comment: deployment "may
//!   need... the terminal itself for a browser login"), which a headless
//!   session has no answer for yet. The deploy schema is never offered
//!   (`deploy: false` in `tools::schemas_for`), so the model never sees the
//!   tool at all -- refusing a call that was never offered, rather than
//!   offering one and always refusing it.
//! - Subagents are supported through the shared read-only `explore` child in
//!   `subagent.rs`. Its per-call activity is not streamed yet; the client
//!   sees the `agent` call start and the child's final report, which is the
//!   same report the parent model receives.
//! - No compaction. `App::finish_compaction`'s logic isn't replicated here;
//!   a long headless session just keeps growing its own history for now.
//!
//! One round of `stream_chat` is consumed the same way `subagent::run_subagent`
//! already does (`tokio::select!` over the pinned stream future and its
//! event channel) -- a proven pattern in this codebase, not a new one
//! invented for this module.

use crate::approval::{verdict_for, Decision, Verdict};
use crate::config::Config;
use crate::domain::{
    ApprovalDetail, ApprovalOption, ApprovalOptionKind, ApprovalResolution, SessionCommand,
    SessionEvent, SessionStatus, ToolStatus, TurnId, TurnStopReason,
};
use crate::llm::{self, ApiUsage, ChatMessage, StreamEvent, Target, ToolCall};
use crate::protocol::{
    AcpToolCall, ChangeEntry, ContentBlock, DiffHunk, DiffHunkLine, DiffLineChange,
    ListChangesResponse, PermissionOption, PermissionOptionId, PermissionOptionKind,
    RequestPermissionOutcome, RequestPermissionRequest, RollbackRequest, SessionId, SessionUpdate,
    StopReason, ToolCallContent, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolKind,
};
use crate::tools::{self, Action, BrowserInteraction, Mode};
use crate::workspace::Workspace;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// Drop image attachments for providers that cannot read them.
///
/// A text-only provider must never receive an image: some endpoints reject the
/// request outright, and DeepSeek answers as though the image were text it
/// cannot read. The caller still decides whether to tell the user; this is the
/// single place that decides what actually goes on the wire.
fn images_for_model(
    provider: &str,
    model: &str,
    images: Vec<llm::ImageAttachment>,
) -> Vec<llm::ImageAttachment> {
    if crate::providers::supports_images(provider, model) {
        images
    } else {
        Vec::new()
    }
}

/// What a caller (the real stdio transport, or a test) gets sent for each
/// `RequestPermissionRequest` a turn needs answered. The answer comes back
/// through the same `ApprovalHandle` a non-ACP client uses, keyed by the
/// engine-local `request_id` -- so there is one answer path, not two.
#[derive(Clone)]
pub struct PermissionAsk {
    pub request: RequestPermissionRequest,
    pub request_id: u64,
    pub approvals: ApprovalHandle,
}

/// What a caller gets sent for each `check_in_browser` call. The answer comes
/// back through `AskHandle`, keyed by `request_id`, so every attached
/// connection can receive the ask and the first answer wins.
#[derive(Clone)]
pub struct BrowserCheckAsk {
    pub session_id: SessionId,
    pub request_id: u64,
    pub url: String,
    pub answers: AskHandle<BrowserCheckResult>,
}

/// What the client reports back for one `BrowserCheckAsk`. `Failed` covers
/// everything that can go wrong on the client's side (no browser tab, CDP
/// failed, timed out, `url` never loaded) -- covexcode has no way to tell
/// those apart from here, only to say the check didn't happen and hand the
/// model text it can react to, the same posture `ask_permission` already
/// takes toward "the client disconnected."
pub enum BrowserCheckResult {
    Screenshot {
        mime_type: String,
        data: String,
        ax_tree: Option<String>,
    },
    Failed(String),
}

/// One `interact_in_browser` call awaiting a client answer -- same shape and
/// reasoning as `BrowserCheckAsk`, plus the interaction to perform before the
/// screenshot.
#[derive(Clone)]
pub struct BrowserInteractAsk {
    pub session_id: SessionId,
    pub request_id: u64,
    pub url: String,
    pub interaction: BrowserInteraction,
    pub answers: AskHandle<BrowserInteractResult>,
}

/// What the client reports back for one `BrowserInteractAsk`. Same
/// `Screenshot`/`Failed` shape as `BrowserCheckResult` -- a successful
/// interaction still ends in a screenshot, the same proof-of-result
/// `check_in_browser` already gives the model.
pub enum BrowserInteractResult {
    Screenshot {
        mime_type: String,
        data: String,
        ax_tree: Option<String>,
    },
    Failed(String),
}

/// The per-hunk split of a pending write/edit, ready for the ACP wire -- or
/// `None` when whole-file review is the right call instead.
///
/// Returns `None` for three distinct reasons, deliberately not conflated:
/// - **CRLF line endings**: covexcode's diff runs over `str::lines()`, which
///   drops `\r`, so a hunk list cannot be spliced back onto `\r\n` text
///   faithfully. The client's whole-file `oldText`/`newText` path handles
///   those.
/// - **An empty diff**: there are no hunks to review.
/// - **A diff larger than `APPROVAL_DIFF_LINES`**: offering per-hunk review
///   on a *clipped* hunk list would silently hide hunks, so it is withheld
///   and the client falls back to the whole-file path rather than being
///   shown a partial diff as if it were the whole one.
fn diff_hunks_for_review(before: &str, after: &str) -> Option<Vec<DiffHunk>> {
    if before.contains("\r\n") || after.contains("\r\n") {
        return None;
    }
    let diff = crate::diff::diff(before, after);
    if diff.is_empty() || diff.line_count() > crate::tools::APPROVAL_DIFF_LINES {
        return None;
    }
    Some(
        diff.hunks
            .iter()
            .map(|hunk| DiffHunk {
                lines: hunk
                    .lines
                    .iter()
                    .map(|line| DiffHunkLine {
                        change: match line.change {
                            crate::diff::Change::Context => DiffLineChange::Context,
                            crate::diff::Change::Added => DiffLineChange::Added,
                            crate::diff::Change::Removed => DiffLineChange::Removed,
                        },
                        old_no: line.old_no,
                        new_no: line.new_no,
                        text: line.text.clone(),
                    })
                    .collect(),
            })
            .collect(),
    )
}

/// A per-session, per-turn cancellation signal. `watch` stores the value, so a
/// cancel that arrives before the turn's `select!` is polled is not lost (a
/// bare `Notify` would drop that wakeup).
#[derive(Clone)]
pub struct CancelHandle {
    tx: watch::Sender<bool>,
}

impl Default for CancelHandle {
    fn default() -> Self {
        let (tx, _) = watch::channel(false);
        Self { tx }
    }
}

impl CancelHandle {
    pub fn cancel(&self) {
        self.tx.send_replace(true);
    }

    pub fn reset(&self) {
        self.tx.send_replace(false);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }

    pub async fn cancelled(&self) {
        let mut rx = self.tx.subscribe();
        if *rx.borrow() {
            return;
        }
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

/// Answers one engine-to-client ask, keyed by an engine-local request id.
/// The first answer consumes the entry; a later one returns false.
///
/// The map holds only oneshot senders and is never held across an await, so a
/// plain `std::sync::Mutex` is enough.
pub struct AskHandle<T> {
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<T>>>>,
}

// Manual, not `derive(Clone)`: cloning the handle only clones the Arc and must
// not require the answer type itself to be `Clone`.
impl<T> Clone for AskHandle<T> {
    fn clone(&self) -> Self {
        Self {
            pending: self.pending.clone(),
        }
    }
}

impl<T> Default for AskHandle<T> {
    fn default() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T> AskHandle<T> {
    /// Answer one request. Returns false when the id is unknown (already
    /// answered, wrong session, or never issued).
    pub fn respond(&self, request_id: u64, result: T) -> bool {
        let pending = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.remove(&request_id)
        };
        match pending {
            Some(sender) => sender.send(result).is_ok(),
            None => false,
        }
    }

    fn register(&self, request_id: u64) -> oneshot::Receiver<T> {
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.insert(request_id, sender);
        receiver
    }

    fn remove(&self, request_id: u64) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.remove(&request_id);
    }
}

/// Approval answers, as seen by a client. The id comes from
/// `SessionEvent::ApprovalRequested`.
pub type ApprovalHandle = AskHandle<ApprovalResolution>;

/// A serializable control handle for one session. Mirrors what a daemon
/// receives off the wire; applying a command never needs the session actor,
/// which is usually awaiting the very turn the command targets.
#[derive(Clone)]
pub struct SessionCommandHandle {
    cancel: CancelHandle,
    approvals: ApprovalHandle,
}

impl SessionCommandHandle {
    /// Apply one command. Returns false when it targeted an unknown approval
    /// (already answered or never issued); cancel always succeeds.
    pub fn apply(&self, command: SessionCommand) -> bool {
        match command {
            SessionCommand::Cancel => {
                self.cancel.cancel();
                true
            }
            SessionCommand::RespondApproval {
                request_id,
                resolution,
            } => self.approvals.respond(request_id, resolution),
        }
    }
}

/// Removes a pending ask entry when the waiting code leaves its scope,
/// including when the whole turn is cancelled and this future is dropped.
struct PendingAsk<'a, T> {
    answers: &'a AskHandle<T>,
    request_id: u64,
}

impl<T> Drop for PendingAsk<'_, T> {
    fn drop(&mut self) {
        self.answers.remove(self.request_id);
    }
}

/// Fan-out for one session's transport-agnostic events. Subscribers start at
/// the next event; there is no replay, because a manager attaching to a live
/// session needs the current state (snapshotted separately), not a re-run of
/// its history.
#[derive(Clone, Default)]
pub struct EventHub {
    subscribers: Arc<Mutex<Vec<mpsc::UnboundedSender<SessionEvent>>>>,
}

impl EventHub {
    /// Subscribe for events emitted from now on.
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<SessionEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.add(tx);
        rx
    }

    fn add(&self, tx: mpsc::UnboundedSender<SessionEvent>) {
        self.subscribers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(tx);
    }

    fn send(&self, event: SessionEvent) {
        let mut subscribers = self.subscribers.lock().unwrap_or_else(|e| e.into_inner());
        // A closed receiver is a detached client; drop it rather than letting
        // closed subscribers accumulate for the life of the session.
        subscribers.retain(|tx| tx.send(event.clone()).is_ok());
    }
}

/// Adapts the engine's ACP-shaped progress updates to the
/// transport-agnostic event stream in one place.
///
/// Every `SessionUpdate` still goes to the ACP channel unchanged; the same
/// update is also mapped to a `SessionEvent` for every subscriber, so the
/// two views cannot drift and adding an event never means editing the
/// forty-odd tool sites. Holds clones, not a borrow of the session: the
/// engine mutates itself while a sink is alive.
struct UpdateSink<'a> {
    updates: &'a mpsc::Sender<SessionUpdate>,
    events: EventHub,
    session_id: SessionId,
    turn: TurnId,
}

impl<'a> UpdateSink<'a> {
    async fn send(&self, update: SessionUpdate) {
        if let Some(event) = self.event_for(&update) {
            self.events.send(event);
        }
        let _ = self.updates.send(update).await;
    }

    fn event_for(&self, update: &SessionUpdate) -> Option<SessionEvent> {
        let session_id = self.session_id.clone();
        match update {
            SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } => Some(SessionEvent::TextDelta {
                session_id,
                turn: self.turn,
                text: text.clone(),
            }),
            SessionUpdate::ToolCall(call) => Some(SessionEvent::ToolCallStarted {
                session_id,
                turn: self.turn,
                tool_call_id: call.tool_call_id.0.clone(),
                title: call.title.clone(),
            }),
            SessionUpdate::ToolCallUpdate(update) => Some(SessionEvent::ToolCallUpdated {
                session_id,
                turn: self.turn,
                tool_call_id: update.tool_call_id.0.clone(),
                status: update.status.map(ToolStatus::from),
                text: match &update.content {
                    Some(ToolCallContent::Text { text }) => Some(text.clone()),
                    _ => None,
                },
            }),
            SessionUpdate::UsageUpdate {
                used,
                size,
                prompt_tokens,
            } => Some(SessionEvent::UsageUpdated {
                session_id,
                turn: self.turn,
                used: *used,
                size: *size,
                prompt_tokens: *prompt_tokens,
            }),
            _ => None,
        }
    }
}

impl From<ToolCallStatus> for ToolStatus {
    fn from(status: ToolCallStatus) -> Self {
        match status {
            ToolCallStatus::Pending => Self::Pending,
            ToolCallStatus::InProgress => Self::InProgress,
            ToolCallStatus::Completed => Self::Completed,
            ToolCallStatus::Failed => Self::Failed,
        }
    }
}

impl From<RequestPermissionOutcome> for ApprovalResolution {
    fn from(outcome: RequestPermissionOutcome) -> Self {
        match outcome {
            RequestPermissionOutcome::Selected { option_id } => Self::Selected {
                option_id: option_id.0,
            },
            RequestPermissionOutcome::Partial { new_text } => Self::Partial { new_text },
            RequestPermissionOutcome::Cancelled => Self::Cancelled,
        }
    }
}

/// Turn a direct command answer back into the engine's internal ACP-shaped
/// outcome, so both answer paths feed the same downstream logic. A client
/// that sends `Unavailable` is treated as having cancelled: there is nothing
/// to execute.
fn resolution_outcome(resolution: ApprovalResolution) -> RequestPermissionOutcome {
    match resolution {
        ApprovalResolution::Selected { option_id } => RequestPermissionOutcome::Selected {
            option_id: PermissionOptionId(option_id),
        },
        ApprovalResolution::Partial { new_text } => RequestPermissionOutcome::Partial { new_text },
        ApprovalResolution::Cancelled | ApprovalResolution::Unavailable => {
            RequestPermissionOutcome::Cancelled
        }
    }
}

/// The transport-neutral review payload for one approval request.
fn approval_detail(update: &ToolCallUpdate) -> Option<ApprovalDetail> {
    match update.content.as_ref()? {
        ToolCallContent::Text { text } => Some(ApprovalDetail::Text { text: text.clone() }),
        ToolCallContent::Diff {
            path,
            old_text,
            new_text,
            ..
        } => Some(ApprovalDetail::Diff {
            path: path.clone(),
            old_text: old_text.clone(),
            new_text: new_text.clone(),
        }),
        ToolCallContent::Plan {
            title,
            summary,
            steps,
            not_doing,
        } => Some(ApprovalDetail::Plan {
            title: title.clone(),
            summary: summary.clone(),
            steps: steps.clone(),
            not_doing: not_doing.clone(),
        }),
        ToolCallContent::Image { mime_type, data } => Some(ApprovalDetail::Image {
            mime_type: mime_type.clone(),
            data_base64: data.clone(),
        }),
    }
}

/// The ACP checklist for an approved plan: completed steps ticked, the next
/// pending one in progress, the rest pending. ACP has no blocked state, so a
/// step recorded as blocked stays pending -- its note is in the tool result.
fn plan_update(plan: &crate::plan::Plan) -> crate::protocol::Plan {
    let next = plan.next_step().map(|(index, _)| index);
    crate::protocol::Plan {
        entries: plan
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| crate::protocol::PlanEntry {
                content: step.description.clone(),
                priority: crate::protocol::PlanEntryPriority::Medium,
                status: if step.done {
                    crate::protocol::PlanEntryStatus::Completed
                } else if Some(index + 1) == next {
                    crate::protocol::PlanEntryStatus::InProgress
                } else {
                    crate::protocol::PlanEntryStatus::Pending
                },
            })
            .collect(),
    }
}

/// Announce a background teammate to an ACP client. `id` is the teammate id,
/// not the parent call: the parent call completed when the child started, so
/// a client keyed on it would close the row immediately.
fn teammate_tool_call(id: &str, title: &str) -> SessionUpdate {
    SessionUpdate::ToolCall(AcpToolCall {
        tool_call_id: ToolCallId(id.to_string()),
        title: title.to_string(),
        kind: ToolKind::Other,
        status: ToolCallStatus::InProgress,
    })
}

fn teammate_progress(id: &str, label: &str, rounds: usize) -> SessionUpdate {
    SessionUpdate::ToolCallUpdate(ToolCallUpdate {
        tool_call_id: ToolCallId(id.to_string()),
        title: None,
        kind: None,
        status: Some(ToolCallStatus::InProgress),
        content: Some(ToolCallContent::Text {
            text: format!("{label} (round {rounds})"),
        }),
    })
}

fn teammate_outcome(id: &str, summary: &str) -> SessionUpdate {
    SessionUpdate::ToolCallUpdate(ToolCallUpdate {
        tool_call_id: ToolCallId(id.to_string()),
        title: None,
        kind: None,
        status: Some(if summary == "done" {
            ToolCallStatus::Completed
        } else {
            // "failed" and "stopped" both end the row as a failure; only a
            // report is a success.
            ToolCallStatus::Failed
        }),
        content: Some(ToolCallContent::Text {
            text: summary.to_string(),
        }),
    })
}

/// Best effort: the session-lifetime channel exists only under the ACP
/// transport, and a full or closed channel must never stall the child.
fn send_teammate_update(
    sender: &Option<mpsc::UnboundedSender<SessionUpdate>>,
    update: SessionUpdate,
) {
    if let Some(sender) = sender {
        let _ = sender.send(update);
    }
}

/// One ACP session's worth of state -- deliberately independent of `App`,
/// not extracted from it. See this module's own docs for the remaining
/// narrower scope (no deploy) that keeps this from becoming a shadow `App`.
pub struct HeadlessSession {
    session_id: SessionId,
    workspace: Workspace,
    config: Config,
    /// MCP servers connected for this session, with the tools each
    /// advertises. Listed once at session start and cached: the model-facing
    /// schema list must stay byte-identical across a turn's rounds or the
    /// prefix cache is busted.
    mcp: crate::mcp::McpRegistry,
    messages: Vec<ChatMessage>,
    request_id: u64,
    /// Monotonic id for approval requests sent to the client. Distinct from
    /// `request_id` (the model round) so a client can correlate an answer
    /// without knowing anything about ACP's JSON-RPC ids.
    approval_seq: u64,
    tool_steps: usize,
    /// The 1-based turn counter, incremented once per `session/prompt`. It
    /// is the "turn" the rollback journal attributes each file to, so a
    /// client can ask to undo a specific turn's changes.
    turn: u64,
    /// `Normal` by default, set from `PromptRequest.mode` at the top of each
    /// `prompt`. `Plan` keeps the turn read-only until a proposed plan is
    /// approved (see `ask_plan_approval`).
    mode: Mode,
    /// The approved plan, while one is being implemented. Set on approval,
    /// so `plan_progress` calls land against the file the user agreed to.
    active_plan: Option<crate::plan::Plan>,
    /// The model's short-term checklist (`update_todos`), replaced whole on
    /// every call. Session-scoped and never persisted; sent to the client as
    /// an ACP `plan` update so the editor can render it.
    todos: Vec<tools::TodoItem>,
    /// The `messages` index each turn's user message starts at, parallel to
    /// `turn` (entry 0 is turn 1's message, and so on). Lets a
    /// `restoreBeforeTurn` rollback also cut the conversation back to that
    /// turn -- the chat window drops the later turns, so the model's history
    /// must too, or the next prompt would still "remember" work the user just
    /// undid. Recorded before the user message is pushed, so a turn boundary
    /// is always `messages.len()` at that moment.
    turn_starts: Vec<usize>,
    /// Prompt tokens the endpoint reported for the most recent response.
    /// Preferred over the character estimate when deciding whether the
    /// context has grown past `[compact] auto_at_tokens`, because the
    /// endpoint's own count is what the context actually cost.
    last_prompt_tokens: Option<usize>,
    /// The same journal `App::rollback`/`/rollback` reads and writes,
    /// populated the identical way `App::push_tool_outcome` does (see
    /// `decide_and_run`/`ask_permission`'s own calls to `.record(...)`) --
    /// not a parallel reimplementation, the same mechanism reused.
    rollback: crate::rollback::Journal,
    /// The same per-workspace session record the TUI keeps. Written at round
    /// boundaries so a completed turn survives a restart even though ACP
    /// clients have no other persistence of their own.
    log: crate::session::ChatLog,
    /// Tests share one process-global `$HOME`, and most engine tests do not
    /// isolate it; writing every turn into the developer's real session
    /// directory would pollute it (or another test's temporary home). Off by
    /// default under `cfg(test)`; the one test that verifies the file opts in
    /// with `allow_persistence_for_test`. Production always writes.
    #[cfg(test)]
    test_persist: bool,
    /// Transport-agnostic event fan-out. The ACP transport keeps using
    /// `SessionUpdate`; any number of TUI/daemon subscribers can observe the
    /// same session without changing the ACP path.
    events: EventHub,
    /// Updates that belong to the session, not one prompt turn: background
    /// teammate progress. The transport wires this to the session's update
    /// hub, which stays subscribed for the connection's lifetime, so a
    /// teammate can reach the client after the prompt that started it has
    /// returned. The TUI engine leaves it `None` and uses `events` instead.
    background_updates: Option<mpsc::UnboundedSender<SessionUpdate>>,
    /// Cooperative cancellation for the currently running turn. Reset at the
    /// start of every prompt, so a stale cancel cannot kill the next turn.
    cancel: CancelHandle,
    /// Direct answers to outstanding approval requests, from a non-ACP client.
    approvals: ApprovalHandle,
    /// Monotonic id for browser asks. Separate from approval ids because the
    /// transport correlates each ask kind independently. Atomic because
    /// concurrent browser calls may register asks from `&self`.
    browser_seq: AtomicU64,
    browser_checks: AskHandle<BrowserCheckResult>,
    browser_interactions: AskHandle<BrowserInteractResult>,
}

impl HeadlessSession {
    pub fn new(session_id: SessionId, workspace: Workspace, config: Config) -> Self {
        let log = crate::session::ChatLog::new(&workspace.root().to_string_lossy());
        Self {
            session_id,
            workspace,
            config,
            messages: Vec::new(),
            request_id: 0,
            approval_seq: 0,
            tool_steps: 0,
            turn: 0,
            mode: Mode::Normal,
            active_plan: None,
            todos: Vec::new(),
            turn_starts: Vec::new(),
            last_prompt_tokens: None,
            rollback: crate::rollback::Journal::default(),
            log,
            #[cfg(test)]
            test_persist: false,
            mcp: crate::mcp::McpRegistry::default(),
            events: EventHub::default(),
            background_updates: None,
            cancel: CancelHandle::default(),
            approvals: ApprovalHandle::default(),
            browser_seq: AtomicU64::new(0),
            browser_checks: AskHandle::default(),
            browser_interactions: AskHandle::default(),
        }
    }

    /// A cloneable handle a client can use to answer approval requests by the
    /// engine-local id from `SessionEvent::ApprovalRequested`.
    pub fn approval_handle(&self) -> ApprovalHandle {
        self.approvals.clone()
    }

    /// A cloneable handle that applies serializable [`SessionCommand`]s --
    /// cancel and approval answers today, the surface a daemon exposes.
    pub fn command_handle(&self) -> SessionCommandHandle {
        SessionCommandHandle {
            cancel: self.cancel.clone(),
            approvals: self.approvals.clone(),
        }
    }

    /// A cloneable handle a transport can use to cancel the current turn
    /// without waiting for the session actor to drain its prompt queue.
    pub fn cancel_handle(&self) -> CancelHandle {
        self.cancel.clone()
    }

    /// Attach the transport-agnostic event sink. ACP does not use it; this is
    /// the seam the TUI/daemon client will consume instead of ACP types.
    pub fn set_event_sink(&mut self, tx: mpsc::UnboundedSender<SessionEvent>) {
        self.events.add(tx);
    }

    /// Subscribe to the transport-agnostic event stream from now on. This is
    /// the seam a TUI or a session manager uses; the ACP path is unchanged.
    pub fn subscribe_events(&self) -> mpsc::UnboundedReceiver<SessionEvent> {
        self.events.subscribe()
    }

    /// Give background teammates a session-lifetime update channel. The ACP
    /// transport wires it to its update hub; callers that already consume
    /// `SessionEvent` (the TUI engine) leave it unset.
    pub fn set_background_updates(&mut self, updates: mpsc::UnboundedSender<SessionUpdate>) {
        self.background_updates = Some(updates);
    }

    /// Replace this session's conversation with a recorded one (see
    /// `session::load_chat`). Called once, before the session is served.
    ///
    /// Turn boundaries are rebuilt the same way `App::resume_path` rebuilds
    /// them: every `user` message starts a turn. A browser screenshot note is
    /// also a `user` message, so a resumed session may count one extra turn
    /// per screenshot -- a rollback still lands on a user-message boundary,
    /// just possibly a synthetic one.
    ///
    /// The log rotates: the recording being read is never appended to, so
    /// resuming a session and then changing your mind still leaves the
    /// original intact.
    pub fn restore(&mut self, messages: Vec<ChatMessage>) {
        self.turn_starts = messages
            .iter()
            .enumerate()
            .filter(|(_, message)| message.role == "user")
            .map(|(index, _)| index)
            .collect();
        self.turn = self.turn_starts.len() as u64;
        self.messages = messages;
        self.log.reset();
    }

    /// Forget the conversation, matching the TUI's `/new`: transcript, turn
    /// boundaries, todo list, usage snapshot and rollback journal all reset,
    /// and the next turn starts a fresh session file. The active plan is
    /// deliberately kept -- it lives on disk and `/new` in the TUI leaves it
    /// alone too.
    pub fn reset(&mut self) {
        self.messages.clear();
        self.turn_starts.clear();
        self.turn = 0;
        self.tool_steps = 0;
        self.last_prompt_tokens = None;
        self.todos.clear();
        self.rollback = crate::rollback::Journal::default();
        self.log.reset();
    }

    /// Replace the config future turns use. A frontend that lets the user
    /// switch provider or model mid-conversation calls this so the engine
    /// stops sending to the endpoint it was started with. Workspace, session
    /// id, history and the session log are untouched.
    pub fn update_config(&mut self, config: Config) {
        self.config = config;
    }

    fn emit(&self, event: SessionEvent) {
        self.events.send(event);
    }

    fn emit_status(&self, status: SessionStatus) {
        self.emit(SessionEvent::StatusChanged {
            session_id: self.session_id.clone(),
            status,
        });
    }

    fn next_approval_id(&mut self) -> u64 {
        self.approval_seq += 1;
        self.approval_seq
    }

    /// Announce an approval request on the transport-agnostic stream. The ACP
    /// request still carries its own JSON-RPC id; `request_id` is the
    /// engine-local id a non-ACP client answers with.
    fn emit_approval_requested(&self, request_id: u64, request: &RequestPermissionRequest) {
        self.emit(SessionEvent::ApprovalRequested {
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
            request_id,
            tool_call_id: request.tool_call.tool_call_id.0.clone(),
            title: request
                .tool_call
                .title
                .clone()
                .unwrap_or_else(|| "approval".to_string()),
            detail: approval_detail(&request.tool_call),
            options: request
                .options
                .iter()
                .map(|option| ApprovalOption {
                    id: option.option_id.0.clone(),
                    label: option.name.clone(),
                    kind: match option.kind {
                        PermissionOptionKind::AllowOnce => ApprovalOptionKind::AllowOnce,
                        PermissionOptionKind::AllowAlways => ApprovalOptionKind::AllowAlways,
                        PermissionOptionKind::RejectOnce => ApprovalOptionKind::RejectOnce,
                        PermissionOptionKind::RejectAlways => ApprovalOptionKind::RejectAlways,
                    },
                })
                .collect(),
        });
    }

    fn emit_approval_resolved(
        &self,
        request_id: u64,
        outcome: Option<&RequestPermissionOutcome>,
    ) {
        let resolution = match outcome {
            Some(RequestPermissionOutcome::Selected { option_id }) => {
                ApprovalResolution::Selected {
                    option_id: option_id.0.clone(),
                }
            }
            Some(RequestPermissionOutcome::Partial { new_text }) => ApprovalResolution::Partial {
                new_text: new_text.clone(),
            },
            Some(RequestPermissionOutcome::Cancelled) => ApprovalResolution::Cancelled,
            None => ApprovalResolution::Unavailable,
        };
        self.emit_approval_resolution(request_id, resolution);
    }

    fn emit_approval_resolution(&self, request_id: u64, resolution: ApprovalResolution) {
        self.emit(SessionEvent::ApprovalResolved {
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
            request_id,
            resolution,
        });
    }

    /// Bring the session file up to date with the engine's history. Called at
    /// round boundaries, so a crash leaves at most the in-flight round
    /// unwritten, and a completed turn is always on disk.
    fn persist(&mut self) {
        #[cfg(test)]
        if !self.test_persist {
            return;
        }
        self.log.append(&self.messages);
    }

    #[cfg(test)]
    fn allow_persistence_for_test(&mut self) {
        self.test_persist = true;
    }

    fn finish_turn(&self, reason: StopReason) -> StopReason {
        self.emit(SessionEvent::TurnFinished {
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
            reason: match &reason {
                StopReason::EndTurn => TurnStopReason::EndTurn,
                StopReason::MaxTokens => TurnStopReason::MaxTokens,
                StopReason::MaxTurnRequests => TurnStopReason::MaxTurnRequests,
                StopReason::Refusal => TurnStopReason::Refusal,
                StopReason::Cancelled => TurnStopReason::Cancelled,
            },
        });
        // A cancelled turn is not idle in the same sense as a finished one:
        // the client may want to show the stop state rather than a normal
        // prompt. The next prompt emits `Running` again either way.
        self.emit_status(match reason {
            StopReason::Cancelled => SessionStatus::Cancelled,
            _ => SessionStatus::Idle,
        });
        reason
    }

    /// Connects this session's MCP servers and caches what they advertise.
    ///
    /// Called once, when the session is created -- never on the request path,
    /// because the handshake spawns a process and can take seconds. Returns the
    /// problems so the caller can surface them; a server that fails to start must
    /// not cost you the ones that work.
    pub async fn connect_mcp(&mut self, configs: &[crate::mcp::McpServerConfig]) -> Vec<String> {
        let (registry, problems) = crate::mcp::McpRegistry::connect_all(configs).await;
        self.mcp = registry;
        problems
    }

    /// The MCP servers connected for this session.
    pub fn mcp(&self) -> &crate::mcp::McpRegistry {
        &self.mcp
    }

    /// The turn this session is currently on (1-based, incremented at the
    /// top of each `prompt`). Read after a `prompt` returns to learn which
    /// turn that response belongs to -- `transport.rs` folds it into the
    /// `PromptResponse` so the client can scope rollbacks by turn.
    pub fn current_turn(&self) -> u64 {
        self.turn
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Handles one `session/prompt`: appends the user's message, then runs
    /// rounds of fire-stream-decide-execute until the model answers with no
    /// further tool calls or a hard stop condition is hit. Blocks until
    /// done, per ACP v1's `PromptResponse` -- progress streams out via
    /// `updates` while this runs.
    ///
    /// `images` carries whatever `ContentBlock::Image` blocks the client
    /// attached to this prompt (e.g. boxcode-ide's element-picker
    /// screenshot) -- unlike `check_in_browser`'s own screenshot, these are
    /// part of the user's own message and kept in history for the life of
    /// the session, not evicted after one round.
    pub async fn prompt(
        &mut self,
        text: String,
        images: Vec<crate::llm::ImageAttachment>,
        mode: Option<Mode>,
        updates: &mpsc::Sender<SessionUpdate>,
        permissions: &mpsc::Sender<PermissionAsk>,
        browser: &mpsc::Sender<BrowserCheckAsk>,
        browser_interact: &mpsc::Sender<BrowserInteractAsk>,
    ) -> StopReason {
        // `mode` is per-turn: a client can start a `plan` turn, then after
        // approval (which flips `self.mode` back to `Normal` itself) send the
        // next prompt as `normal` again, or leave it out for the default.
        if let Some(mode) = mode {
            self.mode = mode;
        }
        self.cancel.reset();
        // One `session/prompt` = one turn, numbered from 1 so a client's
        // first prompt maps to "turn 1". The rollback journal records files
        // against this value (see `record_turn` below).
        self.turn += 1;
        self.emit(SessionEvent::TurnStarted {
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
        });
        self.emit_status(SessionStatus::Running);
        // Everything below reports progress through the sink, which keeps the
        // ACP update stream and the transport-agnostic event stream in sync.
        let updates = UpdateSink {
            updates,
            events: self.events.clone(),
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
        };
        // The step budget is *per turn*, not per session: without this reset a
        // long-lived ACP session would accumulate tool rounds across turns and
        // eventually withhold the schemas on every later turn, permanently
        // stranding the chat with no way to call a tool. `App` resets its own
        // counter the same way at the start of each TUI turn.
        self.tool_steps = 0;
        self.turn_starts.push(self.messages.len());

        let had_images = !images.is_empty();
        let images_supported = crate::providers::supports_images(
            &self.config.llm.provider,
            &self.config.llm.model,
        );
        let mut user_message = ChatMessage::text("user", text);
        user_message.images = images_for_model(
            &self.config.llm.provider,
            &self.config.llm.model,
            images,
        );
        if had_images && !images_supported {
            // Say it once, on the turn that carried the image. The image is
            // not sent to a model that would reject or garble it; the turn
            // still proceeds with the text so the user is not stranded.
            let _ = updates
                .send(SessionUpdate::AgentMessageChunk {
                    content: ContentBlock::Text {
                        text: "The current model can't read images. The image was omitted; \
                               switch to a vision model with /model and try again."
                            .to_string(),
                    },
                    message_id: None,
                })
                .await;
        }
        // Kept so a mid-turn rescue can put the prompt back after compaction:
        // the summary replaces the history, including this turn's user
        // message, and a conversation with only system messages is refused.
        let retry_message = user_message.clone();
        self.messages.push(user_message);

        // `App`'s own welcome screen already surfaces a missing key via
        // `Config::warnings()` before the user ever types a prompt -- an ACP
        // client has no equivalent surface, so without this check the exact
        // same misconfiguration would only show up as a raw connection/HTTP
        // failure after a real round trip to `self.config.llm.endpoint`,
        // which is a strictly worse version of the same answer arriving
        // slower and less clearly.
        if self.config.llm.api_key.is_empty() {
            let _ = updates
                .send(SessionUpdate::AgentMessageChunk {
                    content: ContentBlock::Text {
                        text: "No API key set. Export COVEXCODE_API_KEY or add api_key to \
                               ~/.covexcode/config.toml."
                            .to_string(),
                    },
                    message_id: None,
                })
                .await;
            return self.finish_turn(StopReason::Refusal);
        }

        // One rescue per turn. If the summary itself cannot fit, or the retry
        // still hits the ceiling, the error is real and should surface.
        let mut compacted_this_turn = false;
        loop {
            // Everything pushed since the previous round (the user message,
            // tool results, a retry after compaction) is on disk before the
            // next model request goes out.
            self.persist();
            if self.cancel.is_cancelled() {
                return self.finish_turn(StopReason::Cancelled);
            }
            let budget_left =
                self.config.tools.max_steps == 0 || self.tool_steps < self.config.tools.max_steps;
            if !budget_left {
                // Same reasoning as agent::fire_request: withholding the
                // schemas is what actually stops a runaway loop, not asking
                // nicely in the prompt.
            }

            let schemas = if budget_left {
                tools::schemas_for(
                    self.mode,
                    self.active_plan.is_some(),
                    false, // no deploy tool in v1 -- see module docs
                    false, // published-artifact detection not wired yet
                    true,  // check_in_browser -- the one action only an ACP client can fulfill
                    tools::SchemaDiet::for_workspace(self.workspace.root()),
                )
            } else {
                Vec::new()
            };
            let system = tools::system_prompt(
                &self.workspace,
                &self.config.tools,
                self.tool_steps,
                self.mode,
            );
            let mut history = self.messages.clone();
            history.insert(0, ChatMessage::text("system", system));
            if let Some(status) = tools::turn_status(
                &self.config.tools,
                self.tool_steps,
                self.active_plan.as_ref(),
            ) {
                history.push(ChatMessage::text("user", status));
            }

            self.request_id += 1;
            let round = self.run_one_round(history, schemas, &updates).await;

            match round {
                RoundOutcome::Answered => {
                    // Same moment as the TUI: only after an ordinary completed
                    // turn, never after a failure or a compaction itself.
                    self.maybe_auto_compact(&updates).await;
                    // The final assistant message was pushed inside the round;
                    // the next loop iteration it would have been saved at
                    // never comes.
                    self.persist();
                    return self.finish_turn(StopReason::EndTurn);
                }
                RoundOutcome::Error(message) => {
                    // A request refused by the context guard is recoverable
                    // while compaction is on: summarize, then retry the same
                    // round once. Without this, the error ends the turn and
                    // every later message fails the same preflight -- and
                    // auto-compaction, which only runs after a *successful*
                    // turn, can never run. That was the dead end where the
                    // only escape the message offered was a new chat.
                    if !compacted_this_turn
                        && crate::notice::classify(&message) == crate::notice::Kind::ContextFull
                        && self.maybe_auto_compact(&updates).await
                    {
                        compacted_this_turn = true;
                        self.messages.push(retry_message.clone());
                        continue;
                    }
                    let _ = updates
                        .send(SessionUpdate::AgentMessageChunk {
                            content: ContentBlock::Text {
                                text: format!("Error: {message}"),
                            },
                            message_id: None,
                        })
                        .await;
                    return self.finish_turn(StopReason::Refusal);
                }
                RoundOutcome::Cancelled => {
                    return self.finish_turn(StopReason::Cancelled);
                }
                RoundOutcome::ToolCalls(calls) => {
                    self.tool_steps += 1;
                    let cancel = self.cancel.clone();
                    let outcomes = tokio::select! {
                        outcomes = self.decide_and_run(
                            calls,
                            &updates,
                            permissions,
                            browser,
                            browser_interact,
                        ) => Some(outcomes),
                        _ = cancel.cancelled() => None,
                    };
                    let Some(outcomes) = outcomes else {
                        return self.finish_turn(StopReason::Cancelled);
                    };
                    // An image attached below rides in `self.messages` for
                    // exactly one round: it needs to be there for the very
                    // next `run_one_round` call so the model can react to
                    // it, but `self.messages` has no other trimming or
                    // compaction at all (this module's own doc comment),
                    // so without this it would get resent -- and rebilled
                    // as real image tokens -- on every request for the rest
                    // of the session. Evicting anything left over from an
                    // earlier round, right before this round's own results
                    // land, is what bounds that to one round.
                    for message in &mut self.messages {
                        message.images.clear();
                    }
                    for (call_id, mut content, images) in outcomes {
                        let images_supported = crate::providers::supports_images(
                            &self.config.llm.provider,
                            &self.config.llm.model,
                        );
                        if !images.is_empty() && !images_supported {
                            // The screenshot is still shown to the human by
                            // the client, but a text-only model must not get
                            // it. The accessibility tree above is the useful
                            // part for this model; name the fix rather than
                            // silently dropping the image.
                            content.push_str(
                                "\n\n(A screenshot was captured but the current model cannot \
                                 read images; use the accessibility tree text above. Switch to \
                                 a vision model with /model to use screenshots.)",
                            );
                        }
                        self.messages.push(ChatMessage {
                            role: "tool".to_string(),
                            content: Some(content),
                            tool_calls: Vec::new(),
                            tool_call_id: Some(call_id),
                            images: Vec::new(),
                        });
                        // A vision content-block array is only valid on a
                        // `user`/`assistant` message for every
                        // OpenAI-compatible endpoint this codebase targets
                        // -- a `role: "tool"` message carrying one is
                        // rejected outright by real endpoints, which is why
                        // this is a second, separate message rather than
                        // attached to the tool result above.
                        if !images.is_empty() && images_supported {
                            self.messages.push(ChatMessage {
                                role: "user".to_string(),
                                content: Some(
                                    "Screenshot from the browser check above:".to_string(),
                                ),
                                tool_calls: Vec::new(),
                                tool_call_id: None,
                                images,
                            });
                        }
                    }
                    if !budget_left {
                        self.persist();
                        return self.finish_turn(StopReason::MaxTurnRequests);
                    }
                    // Loop again: the model gets to react to what just ran.
                }
            }
        }
    }

    /// The context size used to decide whether auto-compaction is due: the
    /// endpoint's exact prompt-token count when it reported one, otherwise the
    /// same character estimate the preflight uses.
    fn context_tokens(&self) -> usize {
        self.last_prompt_tokens.unwrap_or_else(|| {
            crate::llm::estimate_request_tokens(&self.messages, &[]) as usize
        })
    }

    /// Compact automatically after an ordinary completed turn, matching the
    /// TUI's `[compact] auto` behavior. ACP sessions used to grow forever even
    /// though `/compact` existed in the TUI -- this is that feature in the
    /// headless path, not a replacement for it.
    ///
    /// Returns true when the history was actually replaced by a summary, so a
    /// failed round can use it to decide whether a retry is worth attempting.
    async fn maybe_auto_compact(&mut self, updates: &UpdateSink<'_>) -> bool {
        if !self.config.compact.auto || self.messages.len() < 2 {
            return false;
        }
        let context = self.context_tokens();
        let threshold = self.config.compact.effective_auto_at_tokens(
            self.config.llm.effective_context_window(),
            self.config.llm.max_tokens,
        ) as usize;
        if context < threshold {
            return false;
        }

        let _ = updates
            .send(SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text {
                    text: format!(
                        "\n\nContext reached ~{context} tokens, past the auto-compact threshold \
                         ({threshold}); summarizing the conversation to free it up.\n\n"
                    ),
                },
                message_id: None,
            })
            .await;

        match self.compact_history(updates).await {
            Ok(Some(_)) => {
                let _ = updates
                    .send(SessionUpdate::AgentMessageChunk {
                        content: ContentBlock::Text {
                            text: format!(
                                "\n\nConversation compacted: the earlier messages were replaced \
                                 by a summary. Context is now ~{} tokens.\n\n",
                                self.context_tokens()
                            ),
                        },
                        message_id: None,
                    })
                    .await;
                true
            }
            Ok(None) => {
                let _ = updates
                    .send(SessionUpdate::AgentMessageChunk {
                        content: ContentBlock::Text {
                            text: "\n\nAuto-compaction returned an empty summary; the \
                                   conversation is unchanged.\n\n"
                                .to_string(),
                        },
                        message_id: None,
                    })
                    .await;
                false
            }
            Err(error) => {
                let _ = updates
                    .send(SessionUpdate::AgentMessageChunk {
                        content: ContentBlock::Text {
                            text: format!(
                                "\n\nAuto-compaction failed ({error}); the conversation is \
                                 unchanged.\n\n"
                            ),
                        },
                        message_id: None,
                    })
                    .await;
                false
            }
        }
    }

    /// Summarize and replace the conversation. `Ok(Some(summary))` when it
    /// was replaced, `Ok(None)` for an empty summary, `Err` when the request
    /// failed. The success note is the caller's, because auto and manual
    /// compaction word it differently.
    async fn compact_history(
        &mut self,
        updates: &UpdateSink<'_>,
    ) -> Result<Option<String>, String> {
        match self.request_summary(updates).await {
            Ok(summary) if !summary.trim().is_empty() => {
                let summary = summary.trim().to_string();
                self.messages = vec![crate::compact::summary_message(&summary)];
                // The old turn boundaries index a conversation that no longer
                // exists. Keep the rollback journal (its file before-states are
                // still valid), but start the conversation afresh -- including
                // on disk, so the pre-compaction file stays resumable.
                self.turn_starts.clear();
                self.last_prompt_tokens = None;
                self.log.reset();
                self.persist();
                Ok(Some(summary))
            }
            Ok(_) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Manual compaction (`/compact`), for an in-process client that owns
    /// the command surface rather than sending a prompt. Emits
    /// [`SessionEvent::Compacted`] with the outcome and returns whether the
    /// history was actually replaced.
    pub async fn compact_now(&mut self, updates: &mpsc::Sender<SessionUpdate>) -> bool {
        if self.messages.len() < 2 {
            self.emit(SessionEvent::Compacted {
                session_id: self.session_id.clone(),
                turn: TurnId::new(self.turn),
                summary: None,
                error: Some("there is nothing to compact".to_string()),
            });
            return false;
        }
        let sink = UpdateSink {
            updates,
            events: self.events.clone(),
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
        };
        let outcome = self.compact_history(&sink).await;
        let (summary, error) = match outcome {
            Ok(Some(summary)) => (Some(summary), None),
            Ok(None) => (
                None,
                Some("the endpoint returned an empty summary".to_string()),
            ),
            Err(error) => (None, Some(error)),
        };
        let replaced = summary.is_some();
        self.emit(SessionEvent::Compacted {
            session_id: self.session_id.clone(),
            turn: TurnId::new(self.turn),
            summary,
            error,
        });
        replaced
    }

    /// Ask the model to summarize the current conversation. This request has
    /// no tools and no workspace system prompt: its only job is to read what is
    /// already in history and write a replacement for it.
    async fn request_summary(&mut self, updates: &UpdateSink<'_>) -> Result<String, String> {
        let mut history = self.messages.clone();
        history.push(ChatMessage::text(
            "user",
            crate::compact::COMPACT_INSTRUCTION,
        ));

        self.request_id += 1;
        let (tx, mut rx) = mpsc::channel(64);
        let target = Target {
            endpoint: &self.config.llm.endpoint,
            model: &self.config.llm.model,
            api_key: &self.config.llm.api_key,
            // The summary, not a chat reply: reserving the full reply budget
            // here would make compaction fail at the same context ceiling it
            // is meant to rescue.
            max_tokens: self
                .config
                .llm
                .max_tokens
                .min(crate::compact::SUMMARY_MAX_TOKENS),
            context_window: self.config.llm.effective_context_window(),
            include_usage: self.config.quota.include_usage,
            temperature: self.config.llm.effective_temperature(),
        };
        let stream = llm::stream_chat(target, history, Vec::new(), self.request_id, tx);
        tokio::pin!(stream);

        let mut stream_finished = false;
        let mut text = String::new();
        let mut usage: Option<ApiUsage> = None;
        let mut error = None;
        loop {
            tokio::select! {
                _ = &mut stream, if !stream_finished => stream_finished = true,
                received = rx.recv() => match received {
                    Some((_, StreamEvent::Token(token))) => text.push_str(&token),
                    Some((_, StreamEvent::Usage(u))) => usage = Some(u),
                    Some((_, StreamEvent::Error(message))) => {
                        error = Some(message);
                        break;
                    }
                    Some((_, StreamEvent::Done)) | None => break,
                    Some(_) => {}
                },
            }
        }

        if let Some(u) = usage {
            let _ = updates
                .send(u.to_usage_update(self.config.llm.effective_context_window()))
                .await;
        }

        match error {
            Some(error) => Err(error),
            None => Ok(text),
        }
    }

    /// One `stream_chat` round, consumed the same way `subagent::run_subagent`
    /// already does -- see that function's own comments for why
    /// `tokio::select!` over the pinned future is the right shape here
    /// (this was not reinvented for this module).
    async fn run_one_round(
        &mut self,
        history: Vec<ChatMessage>,
        schemas: Vec<serde_json::Value>,
        updates: &UpdateSink<'_>,
    ) -> RoundOutcome {
        let (tx, mut rx) = mpsc::channel(64);
        let target = Target {
            endpoint: &self.config.llm.endpoint,
            model: &self.config.llm.model,
            api_key: &self.config.llm.api_key,
            max_tokens: self.config.llm.max_tokens,
            context_window: self.config.llm.effective_context_window(),
            include_usage: self.config.quota.include_usage,
            temperature: self.config.llm.effective_temperature(),
        };
        let stream = llm::stream_chat(target, history.clone(), schemas, self.request_id, tx);
        tokio::pin!(stream);

        let mut stream_finished = false;
        let mut text = String::new();
        let mut calls: Vec<ToolCall> = Vec::new();
        let mut usage: Option<ApiUsage> = None;
        let cancel = self.cancel.clone();
        let mut cancelled = false;
        let outcome = loop {
            tokio::select! {
                _ = &mut stream, if !stream_finished => stream_finished = true,
                _ = cancel.cancelled() => {
                    cancelled = true;
                    break None;
                }
                received = rx.recv() => match received {
                    Some((_, StreamEvent::Token(t))) => {
                        text.push_str(&t);
                        let _ = updates
                            .send(SessionUpdate::AgentMessageChunk {
                                content: ContentBlock::Text { text: t },
                                message_id: None,
                            })
                            .await;
                    }
                    // Reasoning is deliberately not forwarded -- see
                    // protocol.rs's module docs for why: no ACP-compatible
                    // way to signal "thinking" exists yet that doesn't
                    // either require content (chunks can't be content-free)
                    // or misuse a mode-change notification.
                    Some((_, StreamEvent::Reasoning(_))) => {}
                    Some((_, StreamEvent::ToolCalls(c, _))) => calls = c,
                    Some((_, StreamEvent::Usage(u))) => usage = Some(u),
                    // A truncation notice used to be TUI-only. Without it an
                    // ACP client cannot tell a deliberately short answer from
                    // one the output cap cut off mid-sentence, so show it in
                    // the conversation alongside the partial text.
                    Some((_, StreamEvent::Notice(message))) => {
                        let _ = updates
                            .send(SessionUpdate::AgentMessageChunk {
                                content: ContentBlock::Text {
                                    text: format!("\n\n{message}\n\n"),
                                },
                                message_id: None,
                            })
                            .await;
                    }
                    Some((_, StreamEvent::AgentActivity { .. })) => {}
                    Some((_, StreamEvent::ToolsFinished(_))) => {}
                    Some((_, StreamEvent::Done)) | None => break None,
                    Some((_, StreamEvent::Error(e))) => break Some(e),
                },
            }
        };

        if cancelled {
            return RoundOutcome::Cancelled;
        }
        if let Some(error) = outcome {
            return RoundOutcome::Error(error);
        }

        if let Some(u) = usage {
            if u.prompt_tokens > 0 {
                self.last_prompt_tokens = Some(u.prompt_tokens);
            }
            let _ = updates
                .send(u.to_usage_update(self.config.llm.effective_context_window()))
                .await;
        }

        if !calls.is_empty() {
            // Mirrors App::request_tools: whatever prose streamed alongside
            // the tool calls still belongs in history, even though it's
            // not the final answer.
            self.messages.push(ChatMessage {
                role: "assistant".to_string(),
                content: (!text.trim().is_empty()).then_some(text),
                tool_calls: calls.clone(),
                tool_call_id: None,
                images: Vec::new(),
            });
            return RoundOutcome::ToolCalls(calls);
        }

        if !text.trim().is_empty() {
            self.messages.push(ChatMessage::text("assistant", text));
        } else {
            // A blank completion used to end the turn with no explanation, so
            // the chat looked like it had silently stopped after its last tool
            // step. Say what happened; the user can then retry deliberately.
            let _ = updates
                .send(SessionUpdate::AgentMessageChunk {
                    content: ContentBlock::Text {
                        text: "\n\nThe model returned an empty response. \
                               Send another message to retry.\n\n"
                            .to_string(),
                    },
                    message_id: None,
                })
                .await;
        }
        RoundOutcome::Answered
    }

    /// Applies `verdict_for` to each queued call -- the one piece of logic
    /// this module shares with `App` rather than reimplementing, per
    /// `approval::Verdict`'s own docs. Returns `(call_id, content, images)`
    /// triples ready to become `tool` (and, when `images` is non-empty, a
    /// following `user`) messages -- only `check_in_browser` ever produces
    /// a non-empty `images`, every other verdict returns an empty one.
    async fn decide_and_run(
        &mut self,
        calls: Vec<ToolCall>,
        updates: &UpdateSink<'_>,
        permissions: &mpsc::Sender<PermissionAsk>,
        browser: &mpsc::Sender<BrowserCheckAsk>,
        browser_interact: &mpsc::Sender<BrowserInteractAsk>,
    ) -> Vec<(String, String, Vec<llm::ImageAttachment>)> {
        let mut results = Vec::with_capacity(calls.len());
        let mut i = 0;
        while i < calls.len() {
            // Inline edit is file-scoped and approval-free. write_file/edit_file
            // propose a reviewable diff without touching disk (the editor buffer
            // is the source of truth), pure reads fall through to the ordinary
            // auto-approve path below, and everything else -- shell commands,
            // deploy/publish, browser, web search, subagents -- is refused
            // outright, because none of it can be rendered as an accept/reject
            // diff and the inline surface has no room for an approval prompt.
            if self.mode.is_edit() {
                let action = tools::describe_action(&calls[i]);
                let is_write_or_edit = matches!(
                    &action,
                    Some(crate::tools::Action::Write { .. })
                        | Some(crate::tools::Action::Edit { .. })
                );
                let is_read = matches!(
                    &action,
                    Some(crate::tools::Action::Read { .. })
                        | Some(crate::tools::Action::List { .. })
                        | Some(crate::tools::Action::Glob { .. })
                        | Some(crate::tools::Action::Grep { .. })
                        | Some(crate::tools::Action::Progress { .. })
                        | Some(crate::tools::Action::Todos(_))
                );
                if is_write_or_edit {
                    if let Some(a) = action {
                        results.push(self.propose_inline_edit(&calls[i], &a, updates).await);
                        i += 1;
                        continue;
                    }
                } else if !is_read {
                    let reason = "Inline edit is file-scoped: only reading files and proposing \
                                  write_file/edit_file changes are allowed -- not shell commands, \
                                  deploy, publish, web search, or browser actions."
                        .to_string();
                    let _ = updates
                        .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                            tool_call_id: ToolCallId(calls[i].id.clone()),
                            title: None,
                            kind: None,
                            status: Some(ToolCallStatus::Failed),
                            content: Some(ToolCallContent::Text {
                                text: reason.clone(),
                            }),
                        }))
                        .await;
                    results.push((calls[i].id.clone(), reason, Vec::new()));
                    i += 1;
                    continue;
                }
            }

            let verdict = verdict_for(
                &calls[i],
                Path::new(self.workspace.root()),
                self.mode,
                self.config.tools.approval,
            );

            // A contiguous run of auto-approved pure reads is fanned out
            // concurrently -- the same fence and split `agent::run_calls`
            // uses for `App`, so the two paths agree on what is safe to
            // overlap. Everything else stays one-at-a-time below.
            if matches!(&verdict, Verdict::AutoApprove)
                && crate::read_policy::is_pure_read_call(&calls[i])
            {
                let start = i;
                i += 1;
                while i < calls.len()
                    && matches!(
                        verdict_for(
                            &calls[i],
                            Path::new(self.workspace.root()),
                            self.mode,
                            self.config.tools.approval,
                        ),
                        Verdict::AutoApprove
                    )
                    && crate::read_policy::is_pure_read_call(&calls[i])
                {
                    i += 1;
                }
                let run = &calls[start..i];

                // Light the reads up in order before any of them finishes,
                // so the client sees the batch begin as a batch.
                for call in run {
                    let action = tools::describe_action(call);
                    let _ = updates
                        .send(SessionUpdate::ToolCall(AcpToolCall {
                            tool_call_id: ToolCallId(call.id.clone()),
                            title: action
                                .map(|a| a.label())
                                .unwrap_or_else(|| call.function.name.clone()),
                            kind: ToolKind::Other,
                            status: ToolCallStatus::InProgress,
                        }))
                        .await;
                }

                // `join_all` (not `buffer_unordered`): each future owns its
                // cloned args, and results come back in submission order, so
                // the transcript order below needs no re-sorting.
                let futures = run.iter().map(|call| {
                    let call = call.clone();
                    let workspace = self.workspace.clone();
                    let tools_config = self.config.tools.clone();
                    async move { tools::execute(&call, &workspace, &tools_config).await }
                });
                let outcomes = futures::future::join_all(futures).await;

                for (call, mut outcome) in run.iter().zip(outcomes) {
                    if let Some(record) = outcome.rollback.take() {
                        self.rollback.record_turn(record, self.turn);
                    }
                    let _ = updates
                        .send(SessionUpdate::ToolCallUpdate((&outcome).into()))
                        .await;
                    results.push((call.id.clone(), outcome.content, Vec::new()));
                }
                continue;
            }

            // Independent file writes: same fan-out as reads, except a file
            // may appear at most once in the run. A repeat ends the run so
            // the second write is ordered after the first rather than racing
            // its read-modify-write.
            if matches!(&verdict, Verdict::AutoApprove)
                && tools::write_target(&self.workspace, &calls[i]).is_some()
            {
                let start = i;
                let mut seen = std::collections::HashSet::new();
                while i < calls.len() {
                    if !matches!(
                        verdict_for(
                            &calls[i],
                            Path::new(self.workspace.root()),
                            self.mode,
                            self.config.tools.approval,
                        ),
                        Verdict::AutoApprove
                    ) {
                        break;
                    }
                    let Some(target) = tools::write_target(&self.workspace, &calls[i]) else {
                        break;
                    };
                    if !seen.insert(target) {
                        break;
                    }
                    i += 1;
                }
                let run = &calls[start..i];

                for call in run {
                    let action = tools::describe_action(call);
                    let _ = updates
                        .send(SessionUpdate::ToolCall(AcpToolCall {
                            tool_call_id: ToolCallId(call.id.clone()),
                            title: action
                                .map(|a| a.label())
                                .unwrap_or_else(|| call.function.name.clone()),
                            kind: ToolKind::Edit,
                            status: ToolCallStatus::InProgress,
                        }))
                        .await;
                }

                let mut outcomes = Vec::with_capacity(run.len());
                for chunk in run.chunks(tools::MAX_CONCURRENT_WRITES) {
                    let futures = chunk.iter().map(|call| {
                        let call = call.clone();
                        let workspace = self.workspace.clone();
                        let tools_config = self.config.tools.clone();
                        async move { tools::execute(&call, &workspace, &tools_config).await }
                    });
                    outcomes.extend(futures::future::join_all(futures).await);
                }

                for (call, mut outcome) in run.iter().zip(outcomes) {
                    if let Some(record) = outcome.rollback.take() {
                        self.rollback.record_turn(record, self.turn);
                    }
                    let _ = updates
                        .send(SessionUpdate::ToolCallUpdate((&outcome).into()))
                        .await;
                    results.push((call.id.clone(), outcome.content, Vec::new()));
                }
                continue;
            }

            // Independent read-only research tasks: run a contiguous run of
            // auto-approved `agent` calls concurrently. Inline-edit mode
            // already refused them above, and plan mode allows them. Judging
            // each call with the same `verdict_for` as the one-at-a-time path
            // means a future policy that asks for an agent call still gets
            // its prompt instead of being silently batched past.
            if matches!(&verdict, Verdict::AutoApprove) && calls[i].function.name == tools::AGENT {
                let start = i;
                i += 1;
                while i < calls.len()
                    && calls[i].function.name == tools::AGENT
                    && matches!(
                        verdict_for(
                            &calls[i],
                            Path::new(self.workspace.root()),
                            self.mode,
                            self.config.tools.approval,
                        ),
                        Verdict::AutoApprove
                    )
                {
                    i += 1;
                }
                let contents = self.run_agent_batch(&calls[start..i], updates).await;
                for (call, content) in calls[start..i].iter().zip(contents) {
                    results.push((call.id.clone(), content, Vec::new()));
                }
                continue;
            }

            let call = &calls[i];
            i += 1;

            let mut images = Vec::new();
            let content = match verdict {
                Verdict::Blocked(reason) => {
                    let _ = updates
                        .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                            tool_call_id: ToolCallId(call.id.clone()),
                            title: None,
                            kind: None,
                            status: Some(ToolCallStatus::Failed),
                            content: Some(ToolCallContent::Text {
                                text: reason.clone(),
                            }),
                        }))
                        .await;
                    reason
                }
                Verdict::PlanRefused(reason) => reason,
                Verdict::Progress { step, done, note } => {
                    self.record_plan_progress(call, step, done, note, updates)
                        .await
                }
                Verdict::Todos(items) => {
                    self.todos = items;
                    // ACP has one checklist slot; todos are the engine's
                    // short-term version of it. The client replaces the whole
                    // list, matching `update_todos`' replace-whole semantics.
                    let _ = updates
                        .send(SessionUpdate::Plan(crate::protocol::plan_for_todos(
                            &self.todos,
                        )))
                        .await;
                    tools::todos_recorded(call, &self.todos).content
                }
                Verdict::AutoApprove => {
                    let action = tools::describe_action(call);
                    // Auto-approved actions that cannot go through
                    // `tools::execute`: `check_in_browser` needs an ACP
                    // client's browser tab, while `agent` and `teammates
                    // retry` need the LLM config the tool runner deliberately
                    // never sees. All are intercepted here, before dispatch.
                    if let Some(Action::CheckInBrowser { url }) = &action {
                        let (text, screenshot_images) =
                            self.check_browser(call, url, updates, browser).await;
                        images = screenshot_images;
                        text
                    } else if matches!(&action, Some(Action::Agent { .. })) {
                        self.run_agent_call(call, updates).await
                    } else if tools::teammates_retry_id(call).is_some() {
                        let _ = updates
                            .send(SessionUpdate::ToolCall(AcpToolCall {
                                tool_call_id: ToolCallId(call.id.clone()),
                                title: action
                                    .map(|a| a.label())
                                    .unwrap_or_else(|| call.function.name.clone()),
                                kind: ToolKind::Other,
                                status: ToolCallStatus::InProgress,
                            }))
                            .await;
                        let outcome =
                            tools::execute_teammates_retry(call, &self.workspace, &self.config);
                        let _ = updates
                            .send(SessionUpdate::ToolCallUpdate((&outcome).into()))
                            .await;
                        outcome.content
                    } else {
                        let _ = updates
                            .send(SessionUpdate::ToolCall(AcpToolCall {
                                tool_call_id: ToolCallId(call.id.clone()),
                                title: action
                                    .map(|a| a.label())
                                    .unwrap_or_else(|| call.function.name.clone()),
                                kind: ToolKind::Other,
                                status: ToolCallStatus::InProgress,
                            }))
                            .await;
                        let mut outcome =
                            tools::execute(call, &self.workspace, &self.config.tools).await;
                        // Same call, same place in the flow as
                        // `App::push_tool_outcome`'s own -- before the
                        // outcome is taken apart, since `.content` moves out
                        // of it just below.
                        if let Some(record) = outcome.rollback.take() {
                            self.rollback.record_turn(record, self.turn);
                        }
                        let _ = updates
                            .send(SessionUpdate::ToolCallUpdate((&outcome).into()))
                            .await;
                        outcome.content
                    }
                }
                Verdict::Ask(action) => {
                    // Defensive only: `agent` is read-only, so the shipped
                    // approval modes never ask for it. Run it rather than
                    // refusing if a future policy does.
                    if matches!(&action, crate::tools::Action::Agent { .. }) {
                        self.run_agent_call(call, updates).await
                    } else {
                        let (text, interact_images) = self
                            .ask_permission(call, &action, updates, permissions, browser_interact)
                            .await;
                        images = interact_images;
                        text
                    }
                }
            };

            results.push((call.id.clone(), content, images));
        }
        results
    }

    /// Run one read-only `agent` call. `tools::execute` cannot: the child loop
    /// takes the LLM config, which the tool runner deliberately never sees.
    /// The engine therefore intercepts the call exactly as `App` does, and
    /// gets the same `explore` guarantees: the child is offered only
    /// `subagent_schemas()`, every call is re-checked against
    /// `subagent_call_allowed`, and only its final report comes back.
    async fn run_agent_call(&self, call: &ToolCall, updates: &UpdateSink<'_>) -> String {
        self.run_agent_batch(std::slice::from_ref(call), updates)
            .await
            .pop()
            .unwrap_or_default()
    }

    /// Run read-only `agent` calls concurrently, up to the configured
    /// `subagent_concurrency`, and report every call before and after it runs.
    /// Results come back in call order, so the transcript does not change.
    async fn run_agent_batch(&self, calls: &[ToolCall], updates: &UpdateSink<'_>) -> Vec<String> {
        for call in calls {
            let title = tools::describe_action(call)
                .map(|a| a.label())
                .unwrap_or_else(|| call.function.name.clone());
            let _ = updates
                .send(SessionUpdate::ToolCall(AcpToolCall {
                    tool_call_id: ToolCallId(call.id.clone()),
                    title,
                    kind: ToolKind::Other,
                    status: ToolCallStatus::InProgress,
                }))
                .await;
        }
        // Background calls start a teammate and finish immediately; the rest
        // keep the foreground fan-out below. Results are placed back in call
        // order, so a mixed batch does not reorder the transcript.
        let mut contents: Vec<Option<String>> = vec![None; calls.len()];
        let mut foreground: Vec<(usize, ToolCall)> = Vec::new();
        for (index, call) in calls.iter().enumerate() {
            let background = matches!(
                tools::describe_action(call),
                Some(tools::Action::Agent {
                    background: true,
                    ..
                })
            );
            if !background {
                foreground.push((index, call.clone()));
                continue;
            }
            // A background child has no tool result to close its trail with,
            // so forward its progress and completion to the session's event
            // hub. Events are keyed by the teammate id, not the parent call:
            // the parent call completed when the child started, and keying it
            // that way would close the trail immediately.
            let title = tools::describe_action(call)
                .map(|action| action.label())
                .unwrap_or_else(|| call.function.name.clone());
            let events = updates.events.clone();
            let background = self.background_updates.clone();
            let session_id = updates.session_id.clone();
            let turn = updates.turn;
            let announced = std::sync::Once::new();
            let activity: crate::teammates::ActivitySink =
                std::sync::Arc::new(move |teammate_id, event| {
                    announced.call_once(|| {
                        events.send(SessionEvent::ToolCallStarted {
                            session_id: session_id.clone(),
                            turn,
                            tool_call_id: teammate_id.to_string(),
                            title: title.clone(),
                        });
                        // The ACP hub stays subscribed for the connection's
                        // lifetime, so this reaches an IDE even though the
                        // prompt that started the teammate has returned.
                        send_teammate_update(&background, teammate_tool_call(teammate_id, &title));
                    });
                    match event {
                        crate::teammates::Activity::Step { label, rounds } => {
                            send_teammate_update(
                                &background,
                                teammate_progress(teammate_id, &label, rounds),
                            );
                            events.send(SessionEvent::SubagentActivity {
                                session_id: session_id.clone(),
                                turn,
                                call_id: teammate_id.to_string(),
                                label,
                                rounds,
                            });
                        }
                        crate::teammates::Activity::Finished { summary } => {
                            send_teammate_update(
                                &background,
                                teammate_outcome(teammate_id, &summary),
                            );
                            events.send(SessionEvent::SubagentFinished {
                                session_id: session_id.clone(),
                                turn,
                                call_id: teammate_id.to_string(),
                                summary,
                            });
                        }
                    }
                });
            let content = match crate::teammates::spawn(
                call.clone(),
                self.workspace.clone(),
                self.config.clone(),
                Some(activity),
            ) {
                Ok(started) => format!(
                    "Started background teammate '{}' ({}). Continue with other work; collect \
                     its report with teammates {{ \"action\": \"report\", \"id\": \"{}\" }}.",
                    started.name, started.id, started.id
                ),
                Err(e) => format!("Error: {e}"),
            };
            let _ = updates
                .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                    tool_call_id: ToolCallId(call.id.clone()),
                    title: None,
                    kind: None,
                    status: Some(if content.starts_with("Error:") {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Completed
                    }),
                    content: Some(ToolCallContent::Text {
                        text: content.clone(),
                    }),
                }))
                .await;
            contents[index] = Some(content);
        }
        let concurrency = self.config.tools.subagent_concurrency;
        let cap = if concurrency == 0 {
            foreground.len()
        } else {
            concurrency
        };
        for chunk in foreground.chunks(cap.max(1)) {
            // One progress channel per chunk. Children announce each tool
            // call they make; forwarding that as an in-progress update on the
            // parent `agent` row keeps the client from sitting blind through
            // a long research turn.
            let (progress_tx, mut progress_rx) = mpsc::channel(64);
            let progress = &progress_tx;
            let futures = chunk.iter().map(|(_, call)| {
                let call = call.clone();
                let workspace = self.workspace.clone();
                let config = self.config.clone();
                async move {
                    crate::subagent::run_subagent(&call, &workspace, &config, Some((0, progress)))
                        .await
                }
            });
            let mut batch = Box::pin(futures::future::join_all(futures));
            let outcomes = loop {
                tokio::select! {
                    outcomes = &mut batch => break outcomes,
                    Some((_, event)) = progress_rx.recv() => {
                        if let StreamEvent::AgentActivity {
                            call_id,
                            label,
                            rounds,
                        } = event
                        {
                            let _ = updates
                                .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                                    tool_call_id: ToolCallId(call_id.clone()),
                                    title: None,
                                    kind: None,
                                    status: Some(ToolCallStatus::InProgress),
                                    content: Some(ToolCallContent::Text {
                                        text: format!("{label} (round {rounds})"),
                                    }),
                                }))
                                .await;
                            // The same fact on the transport-agnostic stream:
                            // a TUI client shows it in `/subagents`, which
                            // does not parse ACP-shaped updates.
                            self.emit(SessionEvent::SubagentActivity {
                                session_id: self.session_id.clone(),
                                turn: TurnId::new(self.turn),
                                call_id,
                                label,
                                rounds,
                            });
                        }
                    }
                }
            };
            for ((index, _), outcome) in chunk.iter().zip(outcomes) {
                let _ = updates
                    .send(SessionUpdate::ToolCallUpdate((&outcome).into()))
                    .await;
                contents[*index] = Some(outcome.content);
            }
        }
        contents
            .into_iter()
            .map(|content| content.unwrap_or_default())
            .collect()
    }

    /// `plan_progress`: tick a step on the approved plan, persist it, and
    /// send the new checklist. Mirrors `App::record_progress`; the engine has
    /// no separate dirty flag, so the plan file is written immediately.
    async fn record_plan_progress(
        &mut self,
        call: &ToolCall,
        step: usize,
        done: bool,
        note: Option<String>,
        updates: &UpdateSink<'_>,
    ) -> String {
        let root = Path::new(self.workspace.root());
        let Some(plan) = self.active_plan.as_mut() else {
            return tools::progress_failed(
                call,
                "there is no plan being worked on, so there is no step to record. Just do the \
                 work and tell the user what you did.",
            )
            .content;
        };
        match plan.mark(step, done, note) {
            Ok(description) => {
                let (finished, total) = plan.progress();
                let shown = plan.display_path(root);
                let outcome =
                    tools::progress_recorded(call, &description, done, total - finished, &shown);
                let checklist = plan_update(plan);
                let _ = plan.save();
                let _ = updates.send(SessionUpdate::Plan(checklist)).await;
                outcome.content
            }
            Err(reason) => tools::progress_failed(call, &reason).content,
        }
    }

    /// `write_file`/`edit_file` in inline-edit mode: compute the before/after
    /// text with the same `preview_change_text` the approval path uses, send it
    /// as a `ToolCallContent::Diff` for the client to render as an inline
    /// accept/reject diff, and return a "proposed, not applied" result to the
    /// model. Nothing touches disk and nothing is journaled -- the editor
    /// buffer is the source of truth, and the user's accept/save is what
    /// commits.
    async fn propose_inline_edit(
        &mut self,
        call: &ToolCall,
        action: &crate::tools::Action,
        updates: &UpdateSink<'_>,
    ) -> (String, String, Vec<llm::ImageAttachment>) {
        let preview = tools::preview_change_text(action, Path::new(self.workspace.root()));

        // What the model reads. `preview_change_text` is `None` when the path
        // does not resolve or the change has nothing to show -- a "proposed,
        // not applied" receipt is still the honest answer.
        let model_text = match &preview {
            Some((path, _, _)) => format!(
                "Proposed a change to {path} (shown in the editor as a reviewable diff; nothing \
                 was written to disk)."
            ),
            None => "Proposed a change, but its diff could not be previewed.".to_string(),
        };

        let diff_content = preview.map(|(path, before, after)| {
            let hunks = diff_hunks_for_review(&before, &after);
            ToolCallContent::Diff {
                path,
                old_text: (!before.is_empty()).then_some(before),
                new_text: after,
                hunks,
            }
        });

        let _ = updates
            .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                tool_call_id: ToolCallId(call.id.clone()),
                title: Some(action.label()),
                kind: None,
                status: Some(ToolCallStatus::Completed),
                content: diff_content,
            }))
            .await;

        (call.id.clone(), model_text, Vec::new())
    }

    async fn ask_permission(
        &mut self,
        call: &ToolCall,
        action: &crate::tools::Action,
        updates: &UpdateSink<'_>,
        permissions: &mpsc::Sender<PermissionAsk>,
        browser_interact: &mpsc::Sender<BrowserInteractAsk>,
    ) -> (String, Vec<llm::ImageAttachment>) {
        // A plan proposal is not a file change -- it has no before/after diff
        // to preview, so it does not take the ordinary `Diff` path below. It
        // gets its own wire shape (`ToolCallContent::Plan`) and its own
        // approve/decline effect (write `plan.md` and leave plan mode, or stay
        // in plan mode and revise).
        if let Action::Plan(proposal) = action {
            return self
                .ask_plan_approval(call, proposal, updates, permissions)
                .await;
        }
        // Same call, same reasoning as `App::advance_approvals`'s own
        // `tools::preview_change(&action, ...)`: computed once, here, where
        // the question is being asked. The whole-file before/after text is
        // what ACP's `Diff` wire shape wants; `diff_hunks_for_review` adds the
        // pre-computed hunk split so a client can offer per-hunk accept/reject
        // without reimplementing covexcode's diff (see its own doc comment for
        // why the hunks are `None` for CRLF or oversized changes).
        let preview = tools::preview_change_text(action, Path::new(self.workspace.root()));
        let partial_path = preview.as_ref().map(|(path, _, _)| path.clone());
        let diff_content = preview.map(|(path, before, after)| {
            let hunks = diff_hunks_for_review(&before, &after);
            ToolCallContent::Diff {
                path,
                old_text: (!before.is_empty()).then_some(before),
                new_text: after,
                hunks,
            }
        });
        let tool_call_update = ToolCallUpdate {
            tool_call_id: ToolCallId(call.id.clone()),
            title: Some(action.label()),
            kind: None,
            status: Some(ToolCallStatus::Pending),
            content: diff_content,
        };
        let request = RequestPermissionRequest {
            session_id: self.session_id.clone(),
            tool_call: tool_call_update,
            options: vec![
                PermissionOption {
                    option_id: PermissionOptionId("allow".to_string()),
                    name: "Allow".to_string(),
                    kind: PermissionOptionKind::AllowOnce,
                },
                PermissionOption {
                    option_id: PermissionOptionId("reject".to_string()),
                    name: "Reject".to_string(),
                    kind: PermissionOptionKind::RejectOnce,
                },
            ],
        };
        self.emit_status(SessionStatus::WaitingForApproval);
        let approval_request_id = self.next_approval_id();
        // Register before announcing: a client that answers the instant it
        // sees the event must find the slot already there.
        let direct = self.approvals.register(approval_request_id);
        let pending = PendingAsk {
            answers: &self.approvals,
            request_id: approval_request_id,
        };
        self.emit_approval_requested(approval_request_id, &request);
        let ask = PermissionAsk {
            request,
            request_id: approval_request_id,
            approvals: self.approvals.clone(),
        };
        if permissions.send(ask).await.is_err() {
            self.emit_status(SessionStatus::Running);
            self.emit_approval_resolution(approval_request_id, ApprovalResolution::Unavailable);
            return (
                "The client disconnected before answering.".to_string(),
                Vec::new(),
            );
        }
        let outcome = direct.await.ok().map(resolution_outcome);
        self.emit_status(SessionStatus::Running);
        self.emit_approval_resolved(approval_request_id, outcome.as_ref());
        drop(pending);

        // Per-hunk review: the client accepted some hunks and returned the
        // merged file text. `Partial` never reaches the `Decision` mapping
        // below -- it is applied here as an ordinary `write_file`, exactly as
        // if the model had issued it, and the pending `call` is marked
        // completed (not re-executed: its content was the diff the client
        // just edited). Without a `partial_path` (the call previewed no file
        // diff) the client sent a shape it has no business sending, so fall
        // through to the binary decision instead of writing to nowhere.
        if let (Some(RequestPermissionOutcome::Partial { new_text }), Some(path)) =
            (&outcome, partial_path.as_ref())
        {
            let synthetic = ToolCall {
                id: call.id.clone(),
                kind: "function".to_string(),
                function: llm::FunctionCall {
                    name: crate::tools::WRITE_FILE.to_string(),
                    arguments:
                        serde_json::json!({ "path": path.as_str(), "content": new_text.as_str() })
                            .to_string(),
                },
            };
            let mut applied = tools::execute(&synthetic, &self.workspace, &self.config.tools).await;
            if let Some(record) = applied.rollback.take() {
                self.rollback.record_turn(record, self.turn);
            }
            let _ = updates
                .send(SessionUpdate::ToolCallUpdate((&applied).into()))
                .await;
            return (applied.content, Vec::new());
        }

        let decision: Decision = match outcome {
            Some(o) => o.into(),
            None => Decision::Deny,
        };

        if decision.is_allowed() {
            // `interact_in_browser`, like `check_in_browser`, has no local
            // implementation `tools::execute` can run -- covexcode has no
            // browser tab, only the client does. Intercepted here, after
            // approval, rather than reaching `tools::execute` -- the
            // `AutoApprove` branch above intercepts `CheckInBrowser` at the
            // equivalent point in its own flow, before execution, for the
            // same reason.
            if let Action::InteractInBrowser { url, interaction } = action {
                return self
                    .interact_browser(call, url, interaction, updates, browser_interact)
                    .await;
            }
            // An MCP call is executed by the server it names, not by this
            // process, so it is dispatched here rather than falling through
            // to `tools::execute` -- the same reason `InteractInBrowser` is
            // intercepted just above.
            //
            // Reaching this point already means `verdict_for` classified the
            // call as `Dangerous` and the user allowed it. Nothing below
            // re-checks that, and no call runs that was not approved.
            if let Action::Mcp {
                server,
                tool,
                arguments,
            } = action
            {
                let (result, ok) = match self.mcp.client_mut(server) {
                    None => (
                        format!(
                            "No MCP server named `{server}` is connected, so `{tool}` was not run."
                        ),
                        false,
                    ),
                    Some(client) => match client.call_tool(tool, arguments.clone()).await {
                        Ok(text) => (text, true),
                        Err(err) => (format!("{server} could not run `{tool}`: {err}"), false),
                    },
                };
                let _ = updates
                    .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                        tool_call_id: ToolCallId(call.id.clone()),
                        title: Some(format!("mcp: {server} / {tool}")),
                        kind: None,
                        status: Some(if ok {
                            ToolCallStatus::Completed
                        } else {
                            ToolCallStatus::Failed
                        }),
                        content: Some(ToolCallContent::Text {
                            text: result.clone(),
                        }),
                    }))
                    .await;
                return (result, Vec::new());
            }
            let mut outcome = tools::execute(call, &self.workspace, &self.config.tools).await;
            if let Some(record) = outcome.rollback.take() {
                self.rollback.record_turn(record, self.turn);
            }
            let _ = updates
                .send(SessionUpdate::ToolCallUpdate((&outcome).into()))
                .await;
            (outcome.content, Vec::new())
        } else {
            let _ = updates
                .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                    tool_call_id: ToolCallId(call.id.clone()),
                    title: None,
                    kind: None,
                    status: Some(ToolCallStatus::Failed),
                    content: Some(ToolCallContent::Text {
                        text: "Refused by user.".to_string(),
                    }),
                }))
                .await;
            ("The user declined this action.".to_string(), Vec::new())
        }
    }

    /// `exit_plan_mode` in a headless session: sends the proposal to the
    /// client as a `ToolCallContent::Plan` (so it renders a plan card, not a
    /// diff), and maps Approve → adopt the plan (write `plan.md`, leave plan
    /// mode, implement) / Reject → decline (stay in plan mode, revise). The
    /// one place `self.mode` flips from `Plan` back to `Normal`.
    async fn ask_plan_approval(
        &mut self,
        call: &ToolCall,
        proposal: &tools::Proposal,
        updates: &UpdateSink<'_>,
        permissions: &mpsc::Sender<PermissionAsk>,
    ) -> (String, Vec<llm::ImageAttachment>) {
        let tool_call_update = ToolCallUpdate {
            tool_call_id: ToolCallId(call.id.clone()),
            title: Some(format!("plan: {}", proposal.title)),
            kind: None,
            status: Some(ToolCallStatus::Pending),
            content: Some(ToolCallContent::Plan {
                title: proposal.title.clone(),
                summary: proposal.summary.clone(),
                steps: proposal.steps.clone(),
                not_doing: proposal.not_doing.clone(),
            }),
        };
        let request = RequestPermissionRequest {
            session_id: self.session_id.clone(),
            tool_call: tool_call_update,
            options: vec![
                PermissionOption {
                    option_id: PermissionOptionId("allow".to_string()),
                    name: "Approve".to_string(),
                    kind: PermissionOptionKind::AllowOnce,
                },
                PermissionOption {
                    option_id: PermissionOptionId("reject".to_string()),
                    name: "Reject".to_string(),
                    kind: PermissionOptionKind::RejectOnce,
                },
            ],
        };
        self.emit_status(SessionStatus::WaitingForApproval);
        let approval_request_id = self.next_approval_id();
        // Register before announcing: a client that answers the instant it
        // sees the event must find the slot already there.
        let direct = self.approvals.register(approval_request_id);
        let pending = PendingAsk {
            answers: &self.approvals,
            request_id: approval_request_id,
        };
        self.emit_approval_requested(approval_request_id, &request);
        let ask = PermissionAsk {
            request,
            request_id: approval_request_id,
            approvals: self.approvals.clone(),
        };
        if permissions.send(ask).await.is_err() {
            self.emit_status(SessionStatus::Running);
            self.emit_approval_resolution(approval_request_id, ApprovalResolution::Unavailable);
            return (
                "The client disconnected before answering.".to_string(),
                Vec::new(),
            );
        }
        let outcome = direct.await.ok().map(resolution_outcome);
        self.emit_status(SessionStatus::Running);
        self.emit_approval_resolved(approval_request_id, outcome.as_ref());
        drop(pending);
        let decision: Decision = match outcome {
            Some(o) => o.into(),
            None => Decision::Deny,
        };

        if decision.is_allowed() {
            let root = Path::new(self.workspace.root());
            let today = crate::quota::today();
            let plan = crate::plan::Plan {
                title: proposal.title.clone(),
                summary: proposal.summary.clone(),
                steps: proposal.steps.iter().map(crate::plan::Step::new).collect(),
                not_doing: proposal.not_doing.clone(),
                created: today.clone(),
                updated: today,
                base_commit: crate::plan::head_commit(root),
                model: self.config.llm.model.clone(),
                path: crate::plan::path(root),
            };
            let shown = plan.display_path(root);
            let steps = plan.steps.len();

            // Leaving plan mode is unconditional once approved, even if the
            // file write fails -- see `App::note_plan_save_failure`: losing
            // the file is bad, but not a reason to refuse what was agreed.
            self.mode = Mode::Normal;
            match plan.save() {
                Ok(()) => {
                    let checklist = plan_update(&plan);
                    self.active_plan = Some(plan);
                    let approved = tools::plan_approved(call, &shown, steps);
                    let content = approved.content.clone();
                    let _ = updates
                        .send(SessionUpdate::ToolCallUpdate((&approved).into()))
                        .await;
                    // ACP clients render the approved plan from this, not
                    // from the tool-call prose.
                    let _ = updates.send(SessionUpdate::Plan(checklist)).await;
                    (content, Vec::new())
                }
                Err(reason) => {
                    self.active_plan = None;
                    let content = tools::plan_save_failed(&reason);
                    let _ = updates
                        .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                            tool_call_id: ToolCallId(call.id.clone()),
                            title: Some(format!("plan: {}", proposal.title)),
                            kind: None,
                            status: Some(ToolCallStatus::Completed),
                            content: Some(ToolCallContent::Text {
                                text: content.clone(),
                            }),
                        }))
                        .await;
                    (content, Vec::new())
                }
            }
        } else {
            let declined = tools::plan_declined(call);
            let content = declined.content.clone();
            let _ = updates
                .send(SessionUpdate::ToolCallUpdate((&declined).into()))
                .await;
            (content, Vec::new())
        }
    }

    /// Fulfills a `check_in_browser` call by asking the client to take the
    /// screenshot -- covexcode itself has no browser tab, only the client
    /// does. Mirrors `ask_permission`'s own shape (send an ask, await one
    /// reply on a fresh `oneshot`) since a `HeadlessSession` only ever has
    /// one of either outstanding at a time.
    ///
    /// The returned image is only ever non-empty when
    /// `config.tools.attach_browser_screenshots` is on -- off by default,
    /// since this is real image-token cost on every check, not something
    /// to silently start charging for. The tool result *text* always
    /// describes what happened either way; that text alone is what a human
    /// sees rendered (via the `SessionUpdate::ToolCallUpdate` sent below),
    /// same as before this setting existed. See `prompt`'s own doc comment
    /// on where the returned image actually goes and why it's evicted from
    /// history after one round rather than kept forever.
    async fn check_browser(
        &self,
        call: &ToolCall,
        url: &str,
        updates: &UpdateSink<'_>,
        browser: &mpsc::Sender<BrowserCheckAsk>,
    ) -> (String, Vec<llm::ImageAttachment>) {
        let _ = updates
            .send(SessionUpdate::ToolCall(AcpToolCall {
                tool_call_id: ToolCallId(call.id.clone()),
                title: format!("check in browser — {url}"),
                kind: ToolKind::Fetch,
                status: ToolCallStatus::InProgress,
            }))
            .await;

        let request_id = self.browser_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut answers = self.browser_checks.register(request_id);
        let pending = PendingAsk {
            answers: &self.browser_checks,
            request_id,
        };
        let ask = BrowserCheckAsk {
            session_id: self.session_id.clone(),
            request_id,
            url: url.to_string(),
            answers: self.browser_checks.clone(),
        };
        self.emit_status(SessionStatus::WaitingForApproval);
        if browser.send(ask).await.is_err() {
            self.emit_status(SessionStatus::Running);
            return (
                "The client disconnected before taking the screenshot.".to_string(),
                Vec::new(),
            );
        }
        // Defense in depth, not the primary fix: the client's own CDP
        // commands (extension.ts's CdpClient.send) now carry their own
        // timeout, which is what should actually turn a stalled command
        // (a backgrounded tab that never produces a compositor frame for
        // Page.captureScreenshot is the real case that motivated this)
        // into a `Failed` outcome on this same oneshot. This wrapper
        // exists so that no misbehaving or future client implementation
        // can ever hang a whole turn -- with nothing on either side
        // bounding it, `receive.await` alone would wait forever, and
        // `session/prompt` is a blocking ACP call, so that hang reaches
        // all the way out to whatever's waiting on the turn to finish.
        const BROWSER_CHECK_TIMEOUT: Duration = Duration::from_secs(90);
        let result = match tokio::time::timeout(BROWSER_CHECK_TIMEOUT, &mut answers).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                BrowserCheckResult::Failed("The client disconnected before responding.".to_string())
            }
            Err(_) => BrowserCheckResult::Failed(
                "Timed out waiting for the client to respond.".to_string(),
            ),
        };
        self.emit_status(SessionStatus::Running);
        drop(pending);

        match result {
            BrowserCheckResult::Screenshot {
                mime_type,
                data,
                ax_tree,
            } => {
                let _ = updates
                    .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                        tool_call_id: ToolCallId(call.id.clone()),
                        title: None,
                        kind: None,
                        status: Some(ToolCallStatus::Completed),
                        content: Some(ToolCallContent::Image {
                            mime_type: mime_type.clone(),
                            data: data.clone(),
                        }),
                    }))
                    .await;
                let images = if self.config.tools.attach_browser_screenshots {
                    vec![llm::ImageAttachment {
                        mime_type,
                        data_base64: data,
                    }]
                } else {
                    Vec::new()
                };
                let text = match ax_tree {
                    Some(ax_tree) => format!(
                        "Page structure of {url} (accessibility tree):\n\n{ax_tree}\n\n(Screenshot also shown to the user.)"
                    ),
                    None => format!("Screenshot of {url} captured and shown to the user."),
                };
                (text, images)
            }
            BrowserCheckResult::Failed(reason) => {
                let _ = updates
                    .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                        tool_call_id: ToolCallId(call.id.clone()),
                        title: None,
                        kind: None,
                        status: Some(ToolCallStatus::Failed),
                        content: Some(ToolCallContent::Text {
                            text: reason.clone(),
                        }),
                    }))
                    .await;
                (
                    format!("Could not check {url} in the browser: {reason}"),
                    Vec::new(),
                )
            }
        }
    }

    /// Fulfills an `interact_in_browser` call by asking the client to click,
    /// type, press a key, or navigate in the tab, then screenshot the result
    /// -- covexcode itself has no browser tab, only the client does. Mirrors
    /// `check_browser`'s own shape exactly (send an ask, await one reply on a
    /// fresh `oneshot`) rather than duplicating the timeout/relay reasoning
    /// documented on that method -- read its doc comment for why the 90s
    /// bound exists, it applies here unchanged.
    async fn interact_browser(
        &self,
        call: &ToolCall,
        url: &str,
        interaction: &BrowserInteraction,
        updates: &UpdateSink<'_>,
        browser_interact: &mpsc::Sender<BrowserInteractAsk>,
    ) -> (String, Vec<llm::ImageAttachment>) {
        let _ = updates
            .send(SessionUpdate::ToolCall(AcpToolCall {
                tool_call_id: ToolCallId(call.id.clone()),
                title: format!("interact in browser — {url} ({})", interaction.describe()),
                kind: ToolKind::Fetch,
                status: ToolCallStatus::InProgress,
            }))
            .await;

        let request_id = self.browser_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let mut answers = self.browser_interactions.register(request_id);
        let pending = PendingAsk {
            answers: &self.browser_interactions,
            request_id,
        };
        let ask = BrowserInteractAsk {
            session_id: self.session_id.clone(),
            request_id,
            url: url.to_string(),
            interaction: interaction.clone(),
            answers: self.browser_interactions.clone(),
        };
        self.emit_status(SessionStatus::WaitingForApproval);
        if browser_interact.send(ask).await.is_err() {
            self.emit_status(SessionStatus::Running);
            return (
                "The client disconnected before performing the interaction.".to_string(),
                Vec::new(),
            );
        }
        // Same 90s bound as `check_browser`, same reasoning -- see that
        // method's own doc comment.
        const BROWSER_INTERACT_TIMEOUT: Duration = Duration::from_secs(90);
        let result = match tokio::time::timeout(BROWSER_INTERACT_TIMEOUT, &mut answers).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => BrowserInteractResult::Failed(
                "The client disconnected before responding.".to_string(),
            ),
            Err(_) => BrowserInteractResult::Failed(
                "Timed out waiting for the client to respond.".to_string(),
            ),
        };
        self.emit_status(SessionStatus::Running);
        drop(pending);

        match result {
            BrowserInteractResult::Screenshot {
                mime_type,
                data,
                ax_tree,
            } => {
                let _ = updates
                    .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                        tool_call_id: ToolCallId(call.id.clone()),
                        title: None,
                        kind: None,
                        status: Some(ToolCallStatus::Completed),
                        content: Some(ToolCallContent::Image {
                            mime_type: mime_type.clone(),
                            data: data.clone(),
                        }),
                    }))
                    .await;
                let images = if self.config.tools.attach_browser_screenshots {
                    vec![llm::ImageAttachment {
                        mime_type,
                        data_base64: data,
                    }]
                } else {
                    Vec::new()
                };
                let text = match ax_tree {
                    Some(ax_tree) => format!(
                        "Page structure of {url} after {} (accessibility tree):\n\n{ax_tree}\n\n(Screenshot also shown to the user.)",
                        interaction.describe()
                    ),
                    None => format!(
                        "{} in {url}, then screenshot captured and shown to the user.",
                        interaction.describe()
                    ),
                };
                (text, images)
            }
            BrowserInteractResult::Failed(reason) => {
                let _ = updates
                    .send(SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                        tool_call_id: ToolCallId(call.id.clone()),
                        title: None,
                        kind: None,
                        status: Some(ToolCallStatus::Failed),
                        content: Some(ToolCallContent::Text {
                            text: reason.clone(),
                        }),
                    }))
                    .await;
                (
                    format!("Could not {} in {url}: {reason}", interaction.describe()),
                    Vec::new(),
                )
            }
        }
    }

    /// Fulfills `session/rollback` -- a plain client-initiated request, not
    /// deferred like `session/prompt` (this is local disk I/O, not an LLM
    /// round trip, so there's no risk of the deadlock class that made
    /// `session/prompt`'s own response need deferring; see transport.rs's
    /// docs on that).
    ///
    /// Same three steps as `App::finish_rollback`, the same order, for the
    /// same reasons -- run the plan, clear the journal (every entry has now
    /// been acted on), and put `report.notice()` in the model's own history
    /// so its next edit isn't reasoning about a disk that no longer matches
    /// what it was told. The one difference: the plan is narrowed by `req`'s
    /// `files`/`turn` filters when present, and only the *selected* entries
    /// are spent (`Journal::remove`), so a later full `/rollback` still
    /// offers the files this call left alone.
    pub fn rollback(&mut self, req: &RollbackRequest) -> crate::protocol::RollbackResponse {
        let steps = match req.restore_before_turn {
            // "Restore to before this turn" reaches the per-turn before-states
            // (see rollback::plan_before_turn), unlike the plain `turn` filter
            // which only matches files *first* touched in one turn.
            Some(turn) => self.rollback.plan_before_turn(turn),
            None => self.rollback.plan_filtered(req.files.as_deref(), req.turn),
        };
        let report = crate::rollback::apply(&steps);
        self.rollback.remove(&steps);

        // A checkpoint restore ("restore before turn N") also drops the chat's
        // later turns, so keep the model's history and turn counter in step
        // with what the client is about to show: truncate everything from turn
        // N+1 on, and rewind `turn` to N so the next prompt is turn N+1 and
        // stays aligned with the client's request indices. The plain
        // `files`/`turn` filters are selective file undos and leave the
        // conversation alone.
        if let Some(turn) = req.restore_before_turn {
            if turn <= self.turn {
                let keep = self
                    .turn_starts
                    .get(turn as usize)
                    .copied()
                    .unwrap_or(self.messages.len());
                self.messages.truncate(keep);
                self.turn_starts.truncate(turn as usize);
                self.turn = turn;
            }
        }

        self.messages
            .push(ChatMessage::text("user", report.notice()));
        crate::protocol::RollbackResponse {
            summary: report.summary(),
        }
    }

    /// `session/list_changes` -- a read-only peek at what `/rollback` would
    /// do, in the structured shape a client's list UI needs (one entry per
    /// undoable file, plus the shell-command warning). Pure: it reads no
    /// disk and mutates nothing.
    pub fn list_changes(&self) -> ListChangesResponse {
        let changes = self
            .rollback
            .plan()
            .into_iter()
            .map(|step| {
                let (action, reason) = match &step.action {
                    crate::rollback::Action::Restore(_) => ("restore".to_string(), None),
                    crate::rollback::Action::Delete => ("delete".to_string(), None),
                    crate::rollback::Action::Blocked(why) => {
                        ("blocked".to_string(), Some(why.clone()))
                    }
                };
                ChangeEntry {
                    path: step.display,
                    turn: step.turn,
                    touches: step.touches,
                    action,
                    reason,
                }
            })
            .collect();
        ListChangesResponse {
            changes,
            shell_warning: self.rollback.shell_warning(),
        }
    }

    /// The journal's current plan, including each file's before-content --
    /// what an in-process `/diff` needs (the ACP `ChangeEntry` deliberately
    /// omits it). Read-only and clone-cheap; the TUI renders it and the
    /// engine still applies any undo itself.
    pub fn changes(&self) -> Vec<crate::rollback::Step> {
        self.rollback.plan()
    }
}

enum RoundOutcome {
    Answered,
    ToolCalls(Vec<ToolCall>),
    Error(String),
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn sse(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{body}"
        )
    }

    fn text_round(text: &str) -> String {
        sse(&format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\ndata: [DONE]\n\n"
        ))
    }

    fn tool_call_round(name: &str, args: &str) -> String {
        let escaped = args.replace('\\', "\\\\").replace('"', "\\\"");
        sse(&format!(
            "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call_1\",\
             \"type\":\"function\",\"function\":{{\"name\":\"{name}\",\"arguments\":\
             \"{escaped}\"}}}}]}}}}]}}\n\ndata: [DONE]\n\n"
        ))
    }

    /// One response asking for two tool calls at once.
    fn two_tool_call_round(first: (&str, &str), second: (&str, &str)) -> String {
        let escape = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        sse(&format!(
            "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[\
             {{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":\
             {{\"name\":\"{}\",\"arguments\":\"{}\"}}}},\
             {{\"index\":1,\"id\":\"call_2\",\"type\":\"function\",\"function\":\
             {{\"name\":\"{}\",\"arguments\":\"{}\"}}}}]}}}}]}}\n\ndata: [DONE]\n\n",
            first.0,
            escape(first.1),
            second.0,
            escape(second.1)
        ))
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = socket.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                if buf.len() >= header_end + 4 + length {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    async fn serve_rounds(responses: Vec<String>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let _ = read_request(&mut socket).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{addr}")
    }

    /// Accepts one request and deliberately holds the response back, so a
    /// cancellation test has a real in-flight stream to stop.
    async fn serve_delayed(delay: Duration, response: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = read_request(&mut socket).await;
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
        format!("http://{addr}")
    }

    fn drain_events(rx: &mut mpsc::UnboundedReceiver<SessionEvent>) -> Vec<SessionEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    fn config_for(endpoint: &str) -> Config {
        let mut config = Config::default();
        config.llm.endpoint = endpoint.to_string();
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();
        config
    }

    fn workspace() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("hello.txt"), "hi\n").unwrap();
        let ws = Workspace::new(dir.path()).expect("workspace");
        (dir, ws)
    }

    /// A missing API key must fail fast and clearly -- before ever touching
    /// the network -- both because that's a strictly better answer for a
    /// real misconfiguration, and because an ACP client (unlike the TUI,
    /// which shows this on its own welcome screen via `Config::warnings()`)
    /// has no other way to learn about it. `config.llm.endpoint`
    /// deliberately points nowhere real: if this check were ever skipped,
    /// the assertion on the exact message text below would fail against
    /// whatever generic connection error came back instead, rather than
    /// this test simply hanging -- proving the check runs before any
    /// network attempt, not just that *some* error eventually surfaces.
    #[tokio::test]
    async fn a_missing_api_key_fails_before_any_network_call() {
        let (_dir, ws) = workspace();
        let mut config = config_for("http://127.0.0.1:1");
        config.llm.api_key = String::new();
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::Refusal);
        let update = updates_rx.recv().await.expect("one chunk explaining why");
        match update {
            SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } => {
                assert!(
                    text.contains("No API key set"),
                    "unexpected message: {text}"
                );
            }
            other => panic!("expected an AgentMessageChunk, got {other:?}"),
        }
    }

    /// The whole point of this module in one test: a plain answer, with no
    /// tool calls, becomes exactly one `agent_message_chunk` and an
    /// `EndTurn` stop reason -- no approval machinery involved.
    #[tokio::test]
    async fn a_plain_answer_ends_the_turn_with_no_approvals_needed() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![text_round("Hello!")]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        session.set_event_sink(events_tx);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let update = updates_rx.recv().await.expect("one chunk");
        assert!(matches!(update, SessionUpdate::AgentMessageChunk { .. }));

        let mut events = Vec::new();
        while let Ok(event) = events_rx.try_recv() {
            events.push(event);
        }
        assert!(matches!(
            events.first(),
            Some(crate::domain::SessionEvent::TurnStarted { turn, .. }) if turn.get() == 1
        ));
        assert!(matches!(
            events.get(1),
            Some(crate::domain::SessionEvent::StatusChanged {
                status: SessionStatus::Running,
                ..
            })
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            crate::domain::SessionEvent::TextDelta { text, .. } if text == "Hello!"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            crate::domain::SessionEvent::TurnFinished {
                reason: crate::domain::TurnStopReason::EndTurn,
                ..
            }
        )));
        assert!(matches!(
            events.last(),
            Some(crate::domain::SessionEvent::StatusChanged {
                status: SessionStatus::Idle,
                ..
            })
        ));
    }

    /// A completed headless turn is recorded where `/sessions` and a restart
    /// can find it -- the same file the TUI writes.
    #[tokio::test]
    async fn a_completed_turn_is_recorded_in_the_session_store() {
        let _guard = crate::config::test_support::HOME_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let fake_home = tempfile::tempdir().expect("temp home");
        let prev_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", fake_home.path());

        let (_dir, ws) = workspace();
        let root = ws.root().to_string_lossy().into_owned();
        let endpoint = serve_rounds(vec![text_round("Hello!")]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        session.allow_persistence_for_test();
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        let recorded = crate::session::latest_for(&root)
            .map(|path| crate::session::load_chat(&path))
            .unwrap_or_default();
        match prev_home {
            Some(h) => std::env::set_var("HOME", h),
            None => std::env::remove_var("HOME"),
        }

        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(recorded.len(), 2, "the whole turn reached the session file");
        assert_eq!(recorded[0].role, "user");
        assert_eq!(recorded[0].content.as_deref(), Some("hi"));
        assert_eq!(recorded[1].role, "assistant");
        assert_eq!(recorded[1].content.as_deref(), Some("Hello!"));
    }

    /// Restoring a recording replaces the history and rebuilds turn
    /// boundaries the same way `App::resume_path` does.
    #[test]
    fn restore_rebuilds_turn_boundaries_from_user_messages() {
        let (_dir, ws) = workspace();
        let config = config_for("http://127.0.0.1:1");
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        session.restore(vec![
            ChatMessage::text("user", "one"),
            ChatMessage::text("assistant", "a"),
            ChatMessage::text("user", "two"),
            ChatMessage::text("assistant", "b"),
        ]);

        assert_eq!(session.messages.len(), 4);
        assert_eq!(session.turn, 2);
        assert_eq!(session.turn_starts, vec![0, 2]);
    }

    /// A manual `/compact` summarizes the history, replaces it, and tells
    /// event subscribers what happened.
    #[tokio::test]
    async fn manual_compaction_replaces_history_and_emits_compacted() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![text_round("the summary")]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let mut events = session.subscribe_events();
        session.restore(vec![
            ChatMessage::text("user", "one"),
            ChatMessage::text("assistant", "two"),
        ]);

        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let replaced = session.compact_now(&updates_tx).await;
        drop(updates_tx);

        assert!(replaced);
        assert_eq!(session.messages.len(), 1, "the history is the summary");
        let content = session.messages[0].content.as_deref().unwrap_or("");
        assert!(content.contains("the summary"), "{content}");
        assert!(session.turn_starts.is_empty());

        let mut compacted = None;
        while let Ok(event) = events.try_recv() {
            if let SessionEvent::Compacted {
                summary: Some(summary),
                ..
            } = event
            {
                compacted = Some(summary);
            }
        }
        assert_eq!(compacted.as_deref(), Some("the summary"));
        while updates_rx.try_recv().is_ok() {}
    }

    /// Nothing to summarize is reported as an error on the same event, not
    /// as a silent no-op.
    #[tokio::test]
    async fn compacting_a_single_message_reports_the_error() {
        let (_dir, ws) = workspace();
        let config = config_for("http://127.0.0.1:1");
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let mut events = session.subscribe_events();
        session.restore(vec![ChatMessage::text("user", "only")]);

        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let replaced = session.compact_now(&updates_tx).await;

        assert!(!replaced);
        assert_eq!(session.messages.len(), 1, "history is untouched");
        let event = events.try_recv().expect("one event");
        assert!(matches!(
            event,
            SessionEvent::Compacted { summary: None, error: Some(_), .. }
        ));
    }

    /// Multiple clients can watch the same session: every subscriber gets its
    /// own copy of the stream, and a detached one is dropped without
    /// affecting the rest. This is the foundation for a shared session
    /// manager serving more than one connection.
    #[tokio::test]
    async fn every_event_subscriber_sees_the_same_stream() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![text_round("Hello!")]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let mut first = session.subscribe_events();
        let mut second = session.subscribe_events();
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let first_events = drain_events(&mut first);
        let second_events = drain_events(&mut second);
        assert!(!first_events.is_empty());
        assert_eq!(first_events, second_events);
        assert!(first_events.iter().any(|event| matches!(
            event,
            SessionEvent::TextDelta { text, .. } if text == "Hello!"
        )));
    }

    /// The event stream must cover the tool lifecycle too, not just text and
    /// the turn envelope: a TUI client needs to show a call start and finish
    /// without speaking ACP.
    #[tokio::test]
    async fn tool_lifecycle_reaches_the_event_stream() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        session.set_event_sink(events_tx);

        let stop = session
            .prompt(
                "read the file".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut events = Vec::new();
        while let Ok(event) = events_rx.try_recv() {
            events.push(event);
        }
        assert!(events.iter().any(|event| matches!(
            event,
            SessionEvent::ToolCallStarted { tool_call_id, title, .. }
                if tool_call_id == "call_1" && !title.is_empty()
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionEvent::ToolCallUpdated {
                tool_call_id,
                status: Some(ToolStatus::Completed),
                ..
            } if tool_call_id == "call_1"
        )));
    }

    /// Two auto-approved writes to different files run as one batch: both
    /// files are written and both results come back to the client.
    #[tokio::test]
    async fn auto_approved_writes_to_distinct_files_run_together() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            two_tool_call_round(
                (
                    tools::WRITE_FILE,
                    r#"{"path":"a.txt","content":"one"}"#,
                ),
                (
                    tools::WRITE_FILE,
                    r#"{"path":"b.txt","content":"two"}"#,
                ),
            ),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "write both".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(
            std::fs::read_to_string(session.workspace.root().join("a.txt")).unwrap(),
            "one"
        );
        assert_eq!(
            std::fs::read_to_string(session.workspace.root().join("b.txt")).unwrap(),
            "two"
        );
        let mut completed = 0;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(update) = update {
                if update.status == Some(ToolCallStatus::Completed) {
                    completed += 1;
                }
            }
        }
        assert_eq!(completed, 2, "both results reach the client");
    }

    /// Two independent agent calls in one response both run, and both
    /// reports reach the parent in call order.
    #[tokio::test]
    async fn sibling_agent_calls_in_one_response_all_run() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            two_tool_call_round(
                (tools::AGENT, r#"{"task":"one","agent_type":"explore"}"#),
                (tools::AGENT, r#"{"task":"two","agent_type":"explore"}"#),
            ),
            text_round("report one"),
            text_round("report two"),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "research both".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut reports = Vec::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(update) = update {
                if let Some(ToolCallContent::Text { text }) = update.content {
                    if text.starts_with("report ") {
                        reports.push(text);
                    }
                }
            }
        }
        assert_eq!(
            reports,
            vec!["report one".to_string(), "report two".to_string()],
            "every child must report, in call order"
        );
    }

    /// The ACP engine used to answer an `agent` call with "not supported".
    /// It now runs the same read-only `explore` child loop the TUI does and
    /// hands the parent model the child's report as the tool result.
    #[tokio::test]
    async fn an_agent_call_runs_the_read_only_subagent() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::AGENT,
                r#"{"task":"read hello.txt","agent_type":"explore"}"#,
            ),
            text_round("The file says hi."),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "research this".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut report = None;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(update) = update {
                if let Some(ToolCallContent::Text { text }) = update.content {
                    if text.contains("The file says hi.") {
                        report = Some(text);
                    }
                }
            }
        }
        let report = report.expect("the parent must receive the child's report");
        assert!(
            !report.contains("not handled"),
            "the engine must run the subagent, not fall through to tools::execute: {report}"
        );
    }

    /// A background call returns to the model immediately; the child keeps
    /// running and its report is collected through the teammate registry.
    #[tokio::test]
    async fn a_background_agent_call_returns_while_the_child_keeps_running() {
        // The teammate registry persists records under the config dir.
        let _home = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            text_round("Background says hi."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        // The transport wires this channel to its long-lived update hub; the
        // test collects the same values directly.
        let (background_tx, mut background_rx) = mpsc::unbounded_channel();
        session.set_background_updates(background_tx);
        let call = ToolCall {
            id: "call_bg".to_string(),
            kind: "function".to_string(),
            function: crate::llm::FunctionCall {
                name: tools::AGENT.to_string(),
                arguments: r#"{"task":"background research","background":true}"#.to_string(),
            },
        };
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let hub = EventHub::default();
        let mut events = hub.subscribe();
        let updates = UpdateSink {
            updates: &updates_tx,
            events: hub,
            session_id: session.session_id.clone(),
            turn: TurnId::new(1),
        };

        let contents = session.run_agent_batch(&[call], &updates).await;
        assert!(
            contents[0].contains("Started background teammate"),
            "{}",
            contents[0]
        );
        let mut announced = false;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(update) = update {
                if update.status == Some(ToolCallStatus::Completed) {
                    if let Some(ToolCallContent::Text { text }) = update.content {
                        announced |= text.contains("Started background teammate");
                    }
                }
            }
        }
        assert!(announced, "the client is told the teammate started");

        let id = crate::teammates::list(session.workspace.root())
            .into_iter()
            .find(|job| job.task == "background research")
            .map(|job| job.id)
            .expect("the job is registered");
        let mut collected = None;
        for _ in 0..200 {
            match crate::teammates::report(session.workspace.root(), &id) {
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
        assert_eq!(collected.expect("a report"), "Background says hi.");

        // The teammate's progress reached the session event hub under its own
        // id, and its finish closed the trail a /agents panel renders.
        let mut saw_step = false;
        let mut saw_finished = false;
        while let Ok(event) = events.try_recv() {
            match event {
                SessionEvent::SubagentActivity { call_id, label, .. }
                    if call_id.starts_with("teammate-") =>
                {
                    saw_step |= label.contains("read hello.txt");
                }
                SessionEvent::SubagentFinished {
                    call_id, summary, ..
                } if call_id.starts_with("teammate-") => {
                    saw_finished |= summary == "done";
                }
                _ => {}
            }
        }
        assert!(saw_step, "the child's tool call was streamed");
        assert!(saw_finished, "the teammate's finish was streamed");

        // The same progress reaches an ACP client through the session-lifetime
        // channel: an announcement, then a completed update.
        let mut announced_update = false;
        let mut finished_update = false;
        while let Ok(update) = background_rx.try_recv() {
            match update {
                SessionUpdate::ToolCall(tool) if tool.tool_call_id.0.starts_with("teammate-") => {
                    announced_update = true;
                }
                SessionUpdate::ToolCallUpdate(update)
                    if update.tool_call_id.0.starts_with("teammate-")
                        && update.status == Some(ToolCallStatus::Completed) =>
                {
                    finished_update = true;
                }
                _ => {}
            }
        }
        assert!(announced_update, "the teammate is announced to the client");
        assert!(finished_update, "the client is told the teammate finished");
    }

    /// A long child turn must not look frozen: the engine forwards each child
    /// tool call as an in-progress update on the parent `agent` row, and as a
    /// `SubagentActivity` event for clients that render trails (`/subagents`).
    #[tokio::test]
    async fn engine_streams_subagent_activity_to_the_client() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::AGENT,
                r#"{"task":"read hello.txt","agent_type":"explore"}"#,
            ),
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            text_round("The file says hi."),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let mut events = session.subscribe_events();
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "research this".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut saw_progress = false;
        let mut saw_report = false;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(update) = update {
                if let Some(ToolCallContent::Text { text }) = update.content {
                    if update.status == Some(ToolCallStatus::InProgress)
                        && text.contains("(round 1)")
                    {
                        saw_progress = true;
                    }
                    if text.contains("The file says hi.") {
                        saw_report = true;
                    }
                }
            }
        }
        assert!(saw_progress, "the client must see the child working");
        assert!(saw_report, "and still receive the final report");

        let mut activity = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let SessionEvent::SubagentActivity {
                call_id,
                label,
                rounds,
                ..
            } = event
            {
                activity.push((call_id, label, rounds));
            }
        }
        assert!(
            activity
                .iter()
                .any(|(_, label, rounds)| *rounds == 1 && !label.is_empty()),
            "the transport-agnostic stream carries the same activity: {activity:?}"
        );
    }

    /// `update_todos` used to be acknowledged and dropped. It now becomes an
    /// ACP `plan` update, which is the only checklist slot the protocol has.
    #[tokio::test]
    async fn update_todos_is_sent_as_a_plan_update() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::UPDATE_TODOS,
                r#"{"todos":[{"content":"first","status":"completed"},{"content":"second","status":"in_progress"}]}"#,
            ),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "track this".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut checklist = None;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::Plan(plan) = update {
                checklist = Some(plan);
            }
        }
        let checklist = checklist.expect("todos must become a plan update");
        assert_eq!(checklist.entries.len(), 2);
        assert_eq!(checklist.entries[0].content, "first");
        assert!(matches!(
            checklist.entries[0].status,
            crate::protocol::PlanEntryStatus::Completed
        ));
        assert!(matches!(
            checklist.entries[1].status,
            crate::protocol::PlanEntryStatus::InProgress
        ));
    }

    /// `plan_progress` ticks a step on the approved plan and pushes the new
    /// checklist; the old engine answered "Noted (not tracked)".
    #[tokio::test]
    async fn plan_progress_updates_the_checklist() {
        let (dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::PLAN_PROGRESS, r#"{"step":1,"status":"done"}"#),
            text_round("done"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        session.active_plan = Some(crate::plan::Plan {
            title: "test plan".to_string(),
            summary: "do two things".to_string(),
            steps: vec![
                crate::plan::Step::new("first step"),
                crate::plan::Step::new("second step"),
            ],
            not_doing: Vec::new(),
            created: "2026-01-01".to_string(),
            updated: "2026-01-01".to_string(),
            base_commit: None,
            model: "test-model".to_string(),
            path: dir.path().join("plan.md"),
        });
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "carry on".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert!(
            session.active_plan.as_ref().unwrap().steps[0].done,
            "the plan itself must be ticked"
        );
        let mut checklist = None;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::Plan(plan) = update {
                checklist = Some(plan);
            }
        }
        let checklist = checklist.expect("progress must push a plan update");
        assert_eq!(checklist.entries.len(), 2);
        assert!(matches!(
            checklist.entries[0].status,
            crate::protocol::PlanEntryStatus::Completed
        ));
        assert!(matches!(
            checklist.entries[1].status,
            crate::protocol::PlanEntryStatus::InProgress
        ));
    }

    #[tokio::test]
    async fn cancel_handle_is_level_triggered_and_resettable() {
        let handle = CancelHandle::default();
        assert!(!handle.is_cancelled());
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle.cancelled().await;
            })
        };
        handle.cancel();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancel must wake the waiter")
            .expect("waiter task");
        assert!(handle.is_cancelled());
        handle.reset();
        assert!(!handle.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_stops_a_turn_before_the_response_arrives() {
        let (_dir, ws) = workspace();
        let endpoint = serve_delayed(Duration::from_secs(30), text_round("too late")).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let cancel = session.cancel_handle();
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "hi".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let stop = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .expect("cancel must not hang")
            .expect("prompt task");
        assert_eq!(stop, StopReason::Cancelled);
    }

    /// The truthfulness gap the cost meter closes: exact usage reported by the
    /// endpoint must reach the client as a real `usage_update`, not a silent
    /// zero. One streamed answer carrying a `usage` block becomes exactly one
    /// `usage_update` with `used = prompt + completion`, sent after the token
    /// chunk and before the turn ends. `size` is the model's context window;
    /// it is `0` here because the test config has no provider/`context_window`.
    #[tokio::test]
    async fn a_streamed_answer_with_usage_emits_a_usage_update() {
        let (_dir, ws) = workspace();
        let body = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hello!\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n\
             data: [DONE]\n\n",
        );
        let endpoint = serve_rounds(vec![body]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        // The token chunk arrives first, then the `usage_update` the meter
        // sums into "N tokens this turn".
        let chunk = updates_rx.recv().await.expect("token chunk");
        assert!(matches!(chunk, SessionUpdate::AgentMessageChunk { .. }));
        let usage = updates_rx.recv().await.expect("usage update");
        assert_eq!(
            usage,
            SessionUpdate::UsageUpdate {
                used: 15,
                size: 0,
                prompt_tokens: Some(10),
            }
        );
    }

    /// The other half of the size fix: once covexcode knows the model's context
    /// window, it must carry that through to `usage_update.size` so the IDE can
    /// render "N of M" occupancy rather than a bare count.
    #[tokio::test]
    async fn a_configured_context_window_is_reported_as_usage_size() {
        let (_dir, ws) = workspace();
        let body = sse(
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n\
             data: [DONE]\n\n",
        );
        let endpoint = serve_rounds(vec![body]).await;
        let mut config = config_for(&endpoint);
        config.llm.context_window = 131_072;
        // The reply budget is reserved inside the window; keep it well under
        // this deliberately small window so the request passes the preflight.
        config.llm.max_tokens = 1_024;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let usage = updates_rx.recv().await.expect("usage update");
        assert_eq!(
            usage,
            SessionUpdate::UsageUpdate {
                used: 15,
                size: 131_072,
                prompt_tokens: Some(10),
            }
        );
    }

    /// The TUI shows `StreamEvent::Notice` (truncation today) on its own; an
    /// ACP client only sees `AgentMessageChunk`s. Forwarding the notice is
    /// what stops a cut-off reply from looking like a finished one in the IDE.
    #[tokio::test]
    async fn a_truncated_reply_reaches_the_acp_client_as_a_visible_notice() {
        let (_dir, ws) = workspace();
        let body = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"half a sen\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n\
             data: [DONE]\n\n",
        );
        let endpoint = serve_rounds(vec![body]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut visible = String::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                visible.push_str(&text);
            }
        }
        assert!(visible.contains("half a sen"), "{visible}");
        assert!(visible.contains("cut off"), "{visible}");
    }

    /// The other silent-end case: a completion with no text and no tool calls.
    /// Without this the chat just stops after its last step with nothing to
    /// tell the user whether the model finished or the endpoint returned junk.
    #[tokio::test]
    async fn an_empty_answer_is_reported_instead_of_ending_silently() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![sse("data: [DONE]\n\n")]).await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let update = updates_rx.recv().await.expect("empty-response notice");
        match update {
            SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } => assert!(text.contains("empty response"), "{text}"),
            other => panic!("expected a text chunk, got {other:?}"),
        }
    }

    /// The TUI's `[compact] auto` behavior, now in the headless path: after a
    /// completed turn whose context passed the threshold, the conversation is
    /// replaced by the model's summary and the client sees what happened.
    #[tokio::test]
    async fn auto_compaction_replaces_history_once_the_context_passes_the_threshold() {
        let (_dir, ws) = workspace();
        let answer = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5000,\"completion_tokens\":10}}\n\n\
             data: [DONE]\n\n",
        );
        let summary = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Notes about the work.\"}}]}\n\n\
             data: [DONE]\n\n",
        );
        let endpoint = serve_rounds(vec![answer, summary]).await;
        let mut config = config_for(&endpoint);
        config.compact.auto = true;
        config.compact.auto_at_tokens = 4_000;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(64);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(session.messages.len(), 1, "history must be the summary only");
        assert_eq!(session.messages[0].role, "system");
        let content = session.messages[0].content.as_deref().unwrap_or("");
        assert!(content.contains("Summary of the conversation so far"), "{content}");
        assert!(content.contains("Notes about the work."), "{content}");
        assert!(
            session
                .messages
                .iter()
                .all(|m| m.content.as_deref() != Some("answer")),
            "the pre-compaction answer must not survive"
        );

        let mut visible = String::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                visible.push_str(&text);
            }
        }
        assert!(visible.contains("auto-compact"), "{visible}");
        assert!(visible.contains("Conversation compacted"), "{visible}");
    }

    #[tokio::test]
    async fn auto_compaction_can_be_turned_off() {
        let (_dir, ws) = workspace();
        let answer = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5000,\"completion_tokens\":10}}\n\n\
             data: [DONE]\n\n",
        );
        let summary = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"unused\"}}]}\n\n\
             data: [DONE]\n\n",
        );
        let endpoint = serve_rounds(vec![answer, summary]).await;
        let mut config = config_for(&endpoint);
        config.compact.auto = false;
        config.compact.auto_at_tokens = 4_000;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(64);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(session.messages.len(), 2, "user + assistant must remain");
        let mut visible = String::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                visible.push_str(&text);
            }
        }
        assert!(!visible.contains("auto-compact"), "{visible}");
    }

    /// A round refused by the context guard is recoverable while compaction is
    /// on. Before this, the error ended the turn, so auto-compaction -- which
    /// only runs after a successful turn -- could never run, and every retry
    /// hit the same preflight. The only escape the message offered was a new
    /// chat.
    #[tokio::test]
    async fn a_refused_round_compacts_and_retries_instead_of_ending_the_turn() {
        let (_dir, ws) = workspace();
        let summary = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"Notes about the work.\"}}]}\n\n\
             data: [DONE]\n\n",
        );
        let answer = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n\
             data: [DONE]\n\n",
        );
        // The refused round never reaches the network, so the first response is
        // the summary and the second belongs to the retried round.
        let endpoint = serve_rounds(vec![summary, answer]).await;
        let mut config = config_for(&endpoint);
        config.llm.context_window = 100_000;
        config.llm.max_tokens = 80_000;
        config.compact.auto = true;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        // The long history is what crosses the ceiling; the new prompt ("hi")
        // is small enough to fit once the history has been replaced.
        session
            .messages
            .push(ChatMessage::text("assistant", "x".repeat(100_000)));
        let (updates_tx, mut updates_rx) = mpsc::channel(128);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let mut visible = String::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                visible.push_str(&text);
            }
        }
        assert!(
            visible.contains("past the auto-compact threshold"),
            "{visible}"
        );
        assert!(visible.contains("Conversation compacted"), "{visible}");
        assert!(visible.contains("answer"), "{visible}");
        assert_eq!(session.messages[0].role, "system");
        assert!(
            session.messages[0]
                .content
                .as_deref()
                .unwrap_or("")
                .contains("Summary of the conversation so far"),
            "{:?}",
            session.messages[0].content
        );
        assert_eq!(session.messages[1].role, "user");
        assert_eq!(session.messages[1].content.as_deref(), Some("hi"));
        assert_eq!(session.messages[2].content.as_deref(), Some("answer"));
    }

    #[tokio::test]
    async fn an_empty_compaction_summary_leaves_history_unchanged() {
        let (_dir, ws) = workspace();
        let answer = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5000,\"completion_tokens\":10}}\n\n\
             data: [DONE]\n\n",
        );
        let empty = sse("data: [DONE]\n\n");
        let endpoint = serve_rounds(vec![answer, empty]).await;
        let mut config = config_for(&endpoint);
        config.compact.auto = true;
        config.compact.auto_at_tokens = 4_000;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(64);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(session.messages.len(), 2, "a failed summary must not discard history");
        let mut visible = String::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                visible.push_str(&text);
            }
        }
        assert!(visible.contains("empty summary"), "{visible}");
    }

    #[tokio::test]
    async fn a_turn_below_the_threshold_does_not_compact() {
        let (_dir, ws) = workspace();
        let answer = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1000,\"completion_tokens\":10}}\n\n\
             data: [DONE]\n\n",
        );
        let summary = sse(
            "data: {\"choices\":[{\"delta\":{\"content\":\"unused\"}}]}\n\n\
             data: [DONE]\n\n",
        );
        let endpoint = serve_rounds(vec![answer, summary]).await;
        let mut config = config_for(&endpoint);
        config.compact.auto = true;
        config.compact.auto_at_tokens = 4_000;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(64);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "hi".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(session.messages.len(), 2);
        let mut visible = String::new();
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                visible.push_str(&text);
            }
        }
        assert!(!visible.contains("auto-compact"), "{visible}");
    }

    #[test]
    fn images_are_dropped_only_for_text_only_models() {
        let image = llm::ImageAttachment {
            mime_type: "image/png".to_string(),
            data_base64: "aGVsbG8=".to_string(),
        };
        assert!(images_for_model("deepseek", "deepseek-v4-pro", vec![image.clone()]).is_empty());
        assert!(
            images_for_model("", "deepseek-v4-pro", vec![image.clone()]).is_empty(),
            "a proxy login has no provider id; v4-pro is still text-only"
        );
        assert_eq!(
            images_for_model("deepseek", "deepseek-v4-flash", vec![image.clone()]).len(),
            1,
            "v4-flash reads images"
        );
        assert_eq!(
            images_for_model("", "deepseek-v4-flash", vec![image.clone()]).len(),
            1,
            "the model family grants vision through the proxy too"
        );
        assert_eq!(images_for_model("openai", "gpt-5.6-sol", vec![image]).len(), 1);
    }

    /// The actual end-to-end proof for `ContentBlock::Image`: an image
    /// passed into `prompt()` must reach the real outbound LLM request as a
    /// base64 data URL (llm.rs's `ChatMessage` vision-content-block
    /// serialization), not just get stored somewhere internal. Doesn't use
    /// `serve_rounds` (it discards the request body) -- captures the raw
    /// request text directly, the same primitives that helper is built on.
    #[tokio::test]
    async fn an_attached_image_reaches_the_real_llm_request() {
        let (_dir, ws) = workspace();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut socket).await;
            let _ = captured_tx.send(request);
            let _ = socket
                .write_all(text_round("I see a red button.").as_bytes())
                .await;
            let _ = socket.shutdown().await;
        });
        let config = config_for(&format!("http://{addr}"));
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);
        let images = vec![llm::ImageAttachment {
            mime_type: "image/png".to_string(),
            data_base64: "aGVsbG8=".to_string(),
        }];

        let stop = session
            .prompt(
                "what's wrong with this button?".to_string(),
                images,
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let request = captured_rx.await.expect("request was captured");
        assert!(
            request.contains("data:image/png;base64,aGVsbG8="),
            "the image never reached the real LLM request body: {request}"
        );
    }

    /// The counterpart to the test above: a text-only provider must never
    /// receive the image, and the user must be told why it was omitted.
    #[tokio::test]
    async fn a_text_only_provider_never_receives_an_image() {
        let (_dir, ws) = workspace();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut socket).await;
            let _ = captured_tx.send(request);
            let _ = socket.write_all(text_round("I cannot see images.").as_bytes()).await;
            let _ = socket.shutdown().await;
        });
        let mut config = config_for(&format!("http://{addr}"));
        config.llm.provider = "deepseek".to_string();
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);
        let images = vec![llm::ImageAttachment {
            mime_type: "image/png".to_string(),
            data_base64: "aGVsbG8=".to_string(),
        }];

        let stop = session
            .prompt(
                "what's wrong with this button?".to_string(),
                images,
                None, &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        let request = captured_rx.await.expect("request was captured");
        assert!(
            !request.contains("data:image/png"),
            "a text-only provider must not receive an image: {request}"
        );
        let mut saw_notice = false;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::AgentMessageChunk {
                content: ContentBlock::Text { text },
                ..
            } = update
            {
                if text.contains("can't read images") {
                    saw_notice = true;
                }
            }
        }
        assert!(saw_notice, "the user must be told why the image was omitted");
    }

    /// A read (auto-approved, per `verdict_for`) never reaches the
    /// permission channel, and its result is fed back to the model, which
    /// then answers.
    #[tokio::test]
    async fn an_auto_approved_read_runs_without_asking_permission() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            text_round("It says hi."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let stop = session
            .prompt(
                "what does hello.txt say?".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;

        assert_eq!(stop, StopReason::EndTurn);
        assert!(
            permissions_rx.try_recv().is_err(),
            "a read must never ask permission"
        );
        let mut saw_tool_call = false;
        while let Ok(update) = updates_rx.try_recv() {
            if matches!(update, SessionUpdate::ToolCall(_)) {
                saw_tool_call = true;
            }
        }
        assert!(
            saw_tool_call,
            "the auto-approved call should still be reported"
        );
    }

    /// A dangerous command (needs approval under every policy, per
    /// `verdict_for` -- a plain write does not, since writes are
    /// "ordinary" under the default `Destructive` policy) blocks on the
    /// permission channel; refusing it feeds a refusal back to the model
    /// instead of running anything.
    #[tokio::test]
    async fn a_dangerous_command_asks_permission_and_a_refusal_is_honored() {
        let (dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::RUN_COMMAND, r#"{"command":"rm -rf build"}"#),
            text_round("Understood, not running that."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        session.set_event_sink(events_tx);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "clean the build dir".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let ask = permissions_rx.recv().await.expect("a permission ask");
        assert_eq!(ask.request.session_id, SessionId("s1".to_string()));
        let _ = ask.approvals.respond(
            ask.request_id,
            RequestPermissionOutcome::Selected {
                option_id: PermissionOptionId("reject".to_string()),
            }
            .into(),
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);
        let mut statuses = Vec::new();
        let mut approval_request = None;
        let mut approval_resolution = None;
        while let Ok(event) = events_rx.try_recv() {
            match event {
                SessionEvent::StatusChanged { status, .. } => statuses.push(status),
                SessionEvent::ApprovalRequested {
                    request_id,
                    tool_call_id,
                    options,
                    ..
                } => approval_request = Some((request_id, tool_call_id, options)),
                SessionEvent::ApprovalResolved {
                    request_id,
                    resolution,
                    ..
                } => approval_resolution = Some((request_id, resolution)),
                _ => {}
            }
        }
        assert_eq!(
            statuses,
            vec![
                SessionStatus::Running,
                SessionStatus::WaitingForApproval,
                SessionStatus::Running,
                SessionStatus::Idle,
            ],
            "the client must see why the turn is paused and when it resumes"
        );
        let (request_id, tool_call_id, options) =
            approval_request.expect("the approval must be on the event stream");
        assert_eq!(request_id, 1);
        assert_eq!(tool_call_id, "call_1");
        assert!(options.iter().any(|option| option.id == "reject"));
        assert_eq!(
            approval_resolution,
            Some((
                request_id,
                ApprovalResolution::Selected {
                    option_id: "reject".to_string()
                }
            ))
        );
        // rm -rf on a directory that never existed is a no-op either way,
        // so the real proof this was refused (not just answered oddly) is
        // that the model's second round ran at all: refusing must feed a
        // `tool` message back so the transcript stays valid and the model
        // gets to respond, exactly like `App::advance_approvals`'s own
        // refusal path -- if that hadn't happened, `serve_rounds`' second
        // queued response would never have been consumed and this task
        // would hang instead of returning `EndTurn`.
        let _ = dir; // kept for the tempdir's own lifetime, not asserted on
    }

    /// An approval can be answered through `ApprovalHandle` without anyone
    /// touching the ACP permission channel -- the command-side seam a TUI or
    /// daemon will use.
    #[tokio::test]
    async fn an_approval_can_be_answered_directly() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::RUN_COMMAND, r#"{"command":"rm -rf build"}"#),
            text_round("stopped"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let commands = session.command_handle();
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        session.set_event_sink(events_tx);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "clean the build dir".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let request_id = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = events_rx.recv().await {
                if let SessionEvent::ApprovalRequested { request_id, .. } = event {
                    return request_id;
                }
            }
            panic!("the event stream closed before an approval request");
        })
        .await
        .expect("the approval request must arrive promptly");

        assert!(commands.apply(SessionCommand::RespondApproval {
            request_id,
            resolution: ApprovalResolution::Cancelled,
        }));
        assert!(
            !commands.apply(SessionCommand::RespondApproval {
                request_id,
                resolution: ApprovalResolution::Cancelled,
            }),
            "an id is answered once"
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);

        let mut resolution = None;
        while let Ok(event) = events_rx.try_recv() {
            if let SessionEvent::ApprovalResolved { resolution: r, .. } = event {
                resolution = Some(r);
            }
        }
        assert_eq!(resolution, Some(ApprovalResolution::Cancelled));
    }

    /// The actual point of wiring `tools::preview_change_text` into
    /// `ask_permission`: a write that needs asking about carries the real
    /// before/after text on the wire, not just a title string -- an ACP
    /// client can show the developer what would change before they approve
    /// it, instead of approving blind.
    #[tokio::test]
    async fn a_pending_write_carries_its_diff_in_the_permission_request() {
        let (dir, ws) = workspace(); // hello.txt already contains "hi\n"
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"bye\n"}"#,
            ),
            text_round("Updated it."),
        ])
        .await;
        let mut config = config_for(&endpoint);
        // The default policy auto-approves an ordinary write (see
        // `an_auto_approved_read_runs_without_asking_permission`'s own
        // sibling reasoning in app.rs's characterization tests) -- `Always`
        // is what actually reaches the permission-asking path for a plain
        // write, which is the path under test here.
        config.tools.approval = crate::config::ApprovalMode::Always;
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "say hi in a different way".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let ask = permissions_rx.recv().await.expect("a permission ask");
        match ask.request.tool_call.content {
            Some(ToolCallContent::Diff {
                path,
                old_text,
                new_text,
                hunks,
            }) => {
                assert_eq!(path, "hello.txt");
                assert_eq!(old_text.as_deref(), Some("hi\n"));
                assert_eq!(new_text, "bye\n");
                // A small, LF-only change carries per-hunk review data too.
                assert!(hunks.is_some());
            }
            other => panic!("expected a Diff, got {other:?}"),
        }
        let _ = ask.approvals.respond(
            ask.request_id,
            RequestPermissionOutcome::Selected {
                option_id: PermissionOptionId("allow".to_string()),
            }
            .into(),
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "bye\n"
        );
    }

    /// The actual point of `check_in_browser`: it goes out over the
    /// `browser` channel, not `permissions` -- auto-approved like a read
    /// (see `is_read_only_action`), and fulfilled entirely by whatever the
    /// client sends back, since `tools::execute` has no way to take a
    /// screenshot at all.
    #[tokio::test]
    async fn check_in_browser_asks_the_client_and_reports_what_it_sees() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::CHECK_IN_BROWSER,
                r#"{"url":"http://localhost:3000"}"#,
            ),
            text_round("It renders correctly."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, mut browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "does the homepage render?".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let ask = browser_rx.recv().await.expect("a browser check ask");
        assert_eq!(ask.session_id, SessionId("s1".to_string()));
        assert_eq!(ask.url, "http://localhost:3000");
        assert!(
            permissions_rx.try_recv().is_err(),
            "check_in_browser must never ask permission -- it's read-only, like a read"
        );
        let _ = ask.answers.respond(
            ask.request_id,
            BrowserCheckResult::Screenshot {
                mime_type: "image/png".to_string(),
                data: "aGVsbG8=".to_string(),
                ax_tree: None,
            },
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);

        let mut saw_image = false;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                content: Some(ToolCallContent::Image { mime_type, data }),
                ..
            }) = update
            {
                assert_eq!(mime_type, "image/png");
                assert_eq!(data, "aGVsbG8=");
                saw_image = true;
            }
        }
        assert!(
            saw_image,
            "the screenshot must reach the client as an Image, for the human to see"
        );
    }

    /// The token-efficient half of `check_in_browser`: when the client also
    /// sends an `ax_tree`, that text -- not a bare "screenshot captured"
    /// receipt -- must reach the real outbound LLM request as the tool
    /// result, so the model can actually *read* the page. Doesn't use
    /// `serve_rounds` (it discards the request body); captures the second
    /// round's request directly, the same primitives
    /// `an_attached_image_reaches_the_real_llm_request` is built on.
    #[tokio::test]
    async fn check_in_browser_puts_the_ax_tree_text_in_the_tool_result() {
        let (_dir, ws) = workspace();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            // Round 1: the user prompt, answered with the tool call.
            let (mut socket, _) = listener.accept().await.expect("accept 1");
            let _ = read_request(&mut socket).await;
            let _ = socket
                .write_all(
                    tool_call_round(
                        tools::CHECK_IN_BROWSER,
                        r#"{"url":"http://localhost:3000"}"#,
                    )
                    .as_bytes(),
                )
                .await;
            let _ = socket.shutdown().await;

            // Round 2: the tool result, which is what we want to inspect.
            let (mut socket, _) = listener.accept().await.expect("accept 2");
            let request = read_request(&mut socket).await;
            let _ = captured_tx.send(request);
            let _ = socket
                .write_all(text_round("It renders correctly.").as_bytes())
                .await;
            let _ = socket.shutdown().await;
        });
        let config = config_for(&format!("http://{addr}"));
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, mut browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "does the homepage render?".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let ask = browser_rx.recv().await.expect("a browser check ask");
        let _ = ask.answers.respond(
            ask.request_id,
            BrowserCheckResult::Screenshot {
                mime_type: "image/png".to_string(),
                data: "aGVsbG8=".to_string(),
                ax_tree: Some("rootwebarea MyApp\n  button Submit".to_string()),
            },
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);

        let request = captured_rx.await.expect("tool-result request was captured");
        assert!(
            request.contains("Page structure of http://localhost:3000 (accessibility tree):"),
            "the tool result must be framed as page structure, not a bare receipt: {request}"
        );
        assert!(
            request.contains("rootwebarea MyApp"),
            "the AX tree text never reached the real LLM request body: {request}"
        );
    }

    /// Unlike `check_in_browser`, `interact_in_browser` is `Risk::Dangerous`
    /// -- it must go over `permissions` first, and only after an explicit
    /// "allow" does it go out over `browser_interact` (not `browser`, and
    /// not `tools::execute`, which has no local implementation for it).
    #[tokio::test]
    async fn interact_in_browser_asks_permission_then_the_client_before_running() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::INTERACT_IN_BROWSER,
                r#"{"url":"http://localhost:3000","action":"click","x":10,"y":20}"#,
            ),
            text_round("Clicked it."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, mut browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, mut browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "click the login button".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let permission_ask = permissions_rx
            .recv()
            .await
            .expect("a permission ask, unlike check_in_browser");
        assert_eq!(
            permission_ask.request.session_id,
            SessionId("s1".to_string())
        );
        assert!(
            browser_interact_rx.try_recv().is_err(),
            "must not act before the user answers the permission ask"
        );
        let _ = permission_ask.approvals.respond(
            permission_ask.request_id,
            RequestPermissionOutcome::Selected {
                option_id: PermissionOptionId("allow".to_string()),
            }
            .into(),
        );

        let interact_ask = browser_interact_rx
            .recv()
            .await
            .expect("an interact ask, after approval");
        assert_eq!(interact_ask.session_id, SessionId("s1".to_string()));
        assert_eq!(interact_ask.url, "http://localhost:3000");
        assert_eq!(
            interact_ask.interaction,
            BrowserInteraction::Click { x: 10.0, y: 20.0 }
        );
        assert!(
            browser_rx.try_recv().is_err(),
            "an interaction must go out over browser_interact, not the plain check_in_browser relay"
        );
        let _ = interact_ask.answers.respond(
            interact_ask.request_id,
            BrowserInteractResult::Screenshot {
                mime_type: "image/png".to_string(),
                data: "aGVsbG8=".to_string(),
                ax_tree: None,
            },
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);

        let mut saw_image = false;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                content: Some(ToolCallContent::Image { mime_type, data }),
                ..
            }) = update
            {
                assert_eq!(mime_type, "image/png");
                assert_eq!(data, "aGVsbG8=");
                saw_image = true;
            }
        }
        assert!(
            saw_image,
            "the post-interaction screenshot must reach the client as an Image"
        );
    }

    /// A refused `interact_in_browser` must never reach `browser_interact` at
    /// all -- same posture as any other refused dangerous action.
    #[tokio::test]
    async fn a_refused_interact_in_browser_never_reaches_the_client() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::INTERACT_IN_BROWSER,
                r#"{"url":"http://localhost:3000","action":"type","text":"hunter2"}"#,
            ),
            text_round("Understood, not typing that."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, mut browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            session
                .prompt(
                    "type the password into the login form".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await
        });

        let permission_ask = permissions_rx.recv().await.expect("a permission ask");
        let _ = permission_ask.approvals.respond(
            permission_ask.request_id,
            RequestPermissionOutcome::Selected {
                option_id: PermissionOptionId("reject".to_string()),
            }
            .into(),
        );

        let stop = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);
        assert!(
            browser_interact_rx.try_recv().is_err(),
            "a refused interaction must never reach the client at all"
        );
    }

    /// The actual point of wiring `Journal` into `HeadlessSession`: a write
    /// the model made this session can be undone through the exact same
    /// mechanism `App`'s own `/rollback` uses, not a parallel
    /// reimplementation that could drift from it.
    #[tokio::test]
    async fn rollback_restores_a_file_the_session_wrote() {
        let (dir, ws) = workspace(); // hello.txt already contains "hi\n"
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"bye\n"}"#,
            ),
            text_round("Updated it."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        // Ordinary write, default policy -- auto-approved, same as
        // `an_auto_approved_read_runs_without_asking_permission`'s own
        // sibling reasoning, just for a write instead of a read.
        let stop = session
            .prompt(
                "say hi in a different way".to_string(),
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &browser_interact_tx,
            )
            .await;
        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "bye\n"
        );

        let response = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: None,
        });
        assert!(
            response.summary.contains("Rolled back 1 file"),
            "{}",
            response.summary
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\n",
            "rollback must put the file back to what the session found, not leave the edit"
        );

        // The journal clears once acted on -- a second rollback right after
        // must be a genuine no-op, not undo the same write again. Both
        // outcomes' summaries start with the literal words "Rolled back"
        // ("Rolled back nothing -- ..." is the no-op case's own wording),
        // so the real distinguishing check is the file count, not just
        // whether that substring appears at all.
        let second = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: None,
        });
        assert_eq!(
            second.summary,
            "Rolled back nothing — every file was already as the session found it."
        );
    }

    /// The selective-rollback half of the feature end to end: two turns each
    /// write one new file, `list_changes` reports both with their turn
    /// numbers, and a turn-filtered rollback undoes only the turn named --
    /// leaving the other file for a later full rollback.
    #[tokio::test]
    async fn selective_rollback_undoes_only_the_requested_turn() {
        let (dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::WRITE_FILE, r#"{"path":"a.txt","content":"A\n"}"#),
            text_round("Wrote A."),
            tool_call_round(tools::WRITE_FILE, r#"{"path":"b.txt","content":"B\n"}"#),
            text_round("Wrote B."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        assert_eq!(
            session
                .prompt(
                    "write A".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await,
            StopReason::EndTurn
        );
        assert_eq!(
            session
                .prompt(
                    "write B".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await,
            StopReason::EndTurn
        );

        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
            "A\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("b.txt")).unwrap(),
            "B\n"
        );

        // Both files are undoable, each attributed to its own turn.
        let changes = session.list_changes();
        assert_eq!(changes.changes.len(), 2);
        let by_name = |p: &str| {
            changes
                .changes
                .iter()
                .find(|c| c.path == p)
                .expect("listed")
        };
        assert_eq!(by_name("a.txt").turn, 1);
        assert_eq!(by_name("b.txt").turn, 2);
        assert_eq!(by_name("a.txt").action, "delete");
        assert_eq!(by_name("b.txt").action, "delete");

        // Undo turn 2 only: b.txt goes, a.txt survives.
        let response = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: Some(2),
            restore_before_turn: None,
        });
        assert!(
            response.summary.contains("Rolled back 1 file"),
            "{}",
            response.summary
        );
        assert!(
            dir.path().join("a.txt").exists(),
            "turn 1's file must survive a turn-2 rollback"
        );
        assert!(
            !dir.path().join("b.txt").exists(),
            "turn 2's file must be undone"
        );

        // A later full rollback still offers the surviving file.
        let rest = session.list_changes();
        assert_eq!(rest.changes.len(), 1);
        assert_eq!(rest.changes[0].path, "a.txt");

        let full = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: None,
        });
        assert!(
            full.summary.contains("Rolled back 1 file"),
            "{}",
            full.summary
        );
        assert!(
            !dir.path().join("a.txt").exists(),
            "the full rollback finishes the job"
        );
    }

    /// The restore-checkpoint button's exact scenario end to end: turn 1 adds a
    /// line, turn 2 adds another, "restore before turn 2" keeps turn 1 and
    /// drops turn 2, a later turn re-adds that same line, and "restore before
    /// turn 3" drops it again -- never a no-op.
    #[tokio::test]
    async fn restore_checkpoint_then_reedit_then_restore_again() {
        let (dir, ws) = workspace(); // hello.txt already contains "hi\n"
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\n"}"#,
            ),
            text_round("Added line 1."),
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\nline2\n"}"#,
            ),
            text_round("Added line 2."),
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\nline2\n"}"#,
            ),
            text_round("Re-added line 2."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        assert_eq!(
            session
                .prompt(
                    "add line 1".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await,
            StopReason::EndTurn
        );
        assert_eq!(
            session
                .prompt(
                    "add line 2".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await,
            StopReason::EndTurn
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\nline1\nline2\n"
        );

        // Restore before turn 2: drop line 2, keep line 1.
        let r2 = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: Some(2),
        });
        assert!(r2.summary.contains("Rolled back 1 file"), "{}", r2.summary);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\nline1\n"
        );

        // Turn 3 re-adds line 2.
        assert_eq!(
            session
                .prompt(
                    "add line 2 again".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await,
            StopReason::EndTurn
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\nline1\nline2\n"
        );

        // Restore before turn 3: line 2 comes off again.
        let r3 = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: Some(3),
        });
        assert!(r3.summary.contains("Rolled back 1 file"), "{}", r3.summary);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\nline1\n"
        );
    }

    /// The native "Start Over" button (shown on the first request) maps to
    /// `restoreBeforeTurn: 1`: after two turns each edited the same file, it
    /// must undo *both*, back to the session-start content -- not just the
    /// last turn.
    #[tokio::test]
    async fn start_over_restores_before_turn_one() {
        let (dir, ws) = workspace(); // hello.txt already contains "hi\n"
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\n"}"#,
            ),
            text_round("Added line 1."),
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\nline2\n"}"#,
            ),
            text_round("Added line 2."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        for text in ["add line 1", "add line 2"] {
            assert_eq!(
                session
                    .prompt(
                        text.to_string(),
                        Vec::new(),
                        None,
                        &updates_tx,
                        &permissions_tx,
                        &browser_tx,
                        &browser_interact_tx,
                    )
                    .await,
                StopReason::EndTurn
            );
        }
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\nline1\nline2\n"
        );

        let response = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: Some(1),
        });
        assert!(
            response.summary.contains("Rolled back 1 file"),
            "{}",
            response.summary
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\n",
            "Start Over must undo turn 1's line too, back to the session-start content"
        );
    }

    /// A checkpoint restore must also rewind the conversation and turn counter:
    /// the chat window drops the later turns, so the model's history has to too,
    /// and the next prompt must be numbered consistently with the shortened chat.
    #[tokio::test]
    async fn restore_checkpoint_rewinds_conversation_and_turn() {
        let (dir, ws) = workspace(); // hello.txt already contains "hi\n"
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\n"}"#,
            ),
            text_round("Added line 1."),
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\nline2\n"}"#,
            ),
            text_round("Added line 2."),
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"hello.txt","content":"hi\nline1\nline2\nline3\n"}"#,
            ),
            text_round("Added line 3."),
            text_round("Done."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        for text in ["add line 1", "add line 2", "add line 3"] {
            assert_eq!(
                session
                    .prompt(
                        text.to_string(),
                        Vec::new(),
                        None,
                        &updates_tx,
                        &permissions_tx,
                        &browser_tx,
                        &browser_interact_tx,
                    )
                    .await,
                StopReason::EndTurn
            );
        }
        assert_eq!(session.current_turn(), 3);
        assert!(
            session
                .messages
                .iter()
                .any(|m| m.role == "user" && m.content.as_deref() == Some("add line 3")),
            "turn 3's prompt should be in history before the restore"
        );

        // Restore before turn 2: undo turns 2 and 3, and cut history back to
        // turn 2's own user message (turn 3 drops out entirely).
        let response = session.rollback(&RollbackRequest {
            session_id: SessionId("s1".to_string()),
            files: None,
            turn: None,
            restore_before_turn: Some(2),
        });
        assert!(
            response.summary.contains("Rolled back 1 file"),
            "{}",
            response.summary
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\nline1\n"
        );
        assert_eq!(
            session.current_turn(),
            2,
            "turn counter must rewind to the restored turn"
        );
        assert!(
            !session
                .messages
                .iter()
                .any(|m| m.role == "user" && m.content.as_deref() == Some("add line 3")),
            "restore must drop turn 3 from the conversation, not just its files"
        );

        // A follow-up prompt is turn 3 again, staying aligned with the shortened
        // chat window (which now shows turns 1, 2, and this new one).
        assert_eq!(
            session
                .prompt(
                    "add line 4".to_string(),
                    Vec::new(),
                    None,
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await,
            StopReason::EndTurn
        );
        assert_eq!(session.current_turn(), 3);
    }

    /// `exit_plan_mode` in a plan-mode turn proposes a plan over the
    /// permission channel as `ToolCallContent::Plan` (not a file diff); an
    /// "allow" writes `plan.md`, leaves plan mode, and records the active
    /// plan. The whole point of the headless plan-mode plumbing: the mode is
    /// per-turn, the proposal rides the ordinary approval wire, and approval
    /// is what flips the session back to `Normal`.
    #[tokio::test]
    async fn approving_a_plan_writes_plan_md_and_leaves_plan_mode() {
        let (dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::EXIT_PLAN_MODE,
                r#"{"title":"Add tests","summary":"Test the thing.","steps":["write a test","run it"],"not_doing":["shipping"]}"#,
            ),
            text_round("Implementing step 1."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            let stop = session
                .prompt(
                    "plan how to add tests".to_string(),
                    Vec::new(),
                    Some(Mode::Plan),
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await;
            (stop, session)
        });

        let ask = permissions_rx
            .recv()
            .await
            .expect("a permission ask carrying the plan");
        match ask.request.tool_call.content {
            Some(ToolCallContent::Plan {
                title,
                summary,
                steps,
                not_doing,
            }) => {
                assert_eq!(title, "Add tests");
                assert_eq!(summary, "Test the thing.");
                assert_eq!(
                    steps,
                    vec!["write a test".to_string(), "run it".to_string()]
                );
                assert_eq!(not_doing, vec!["shipping".to_string()]);
            }
            other => panic!("expected a Plan, got {other:?}"),
        }
        let _ = ask.approvals.respond(
            ask.request_id,
            RequestPermissionOutcome::Selected {
                option_id: PermissionOptionId("allow".to_string()),
            }
            .into(),
        );

        let (stop, session) = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(session.mode, Mode::Normal, "approving must leave plan mode");
        assert!(
            session.active_plan.is_some(),
            "approving must record the active plan"
        );
        assert!(
            dir.path().join("plan.md").exists(),
            "approving must write plan.md"
        );
    }

    /// The decline half: "reject" keeps the session in plan mode, records no
    /// active plan, and writes nothing to disk -- the model is told to revise,
    /// and the very next prompt is still read-only.
    #[tokio::test]
    async fn rejecting_a_plan_stays_in_plan_mode_and_writes_nothing() {
        let (dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(
                tools::EXIT_PLAN_MODE,
                r#"{"title":"Add tests","summary":"Test the thing.","steps":["write a test"]}"#,
            ),
            text_round("Understood, revising."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, _updates_rx) = mpsc::channel(16);
        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            let stop = session
                .prompt(
                    "plan how to add tests".to_string(),
                    Vec::new(),
                    Some(Mode::Plan),
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await;
            (stop, session)
        });

        let ask = permissions_rx
            .recv()
            .await
            .expect("a permission ask carrying the plan");
        assert!(
            matches!(
                ask.request.tool_call.content,
                Some(ToolCallContent::Plan { .. })
            ),
            "the proposal must ride the plan wire shape, not a diff"
        );
        let _ = ask.approvals.respond(
            ask.request_id,
            RequestPermissionOutcome::Selected {
                option_id: PermissionOptionId("reject".to_string()),
            }
            .into(),
        );

        let (stop, session) = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);
        assert_eq!(
            session.mode,
            Mode::Plan,
            "rejecting must leave the session in plan mode"
        );
        assert!(
            session.active_plan.is_none(),
            "a rejected plan must not become active"
        );
        assert!(
            !dir.path().join("plan.md").exists(),
            "rejecting must write nothing"
        );
    }

    /// The whole point of inline-edit mode in one test: a `write_file` call
    /// proposes a `Diff` for the client to render as an accept/reject inline
    /// diff, writes nothing to disk, and still lets the turn end normally.
    #[tokio::test]
    async fn inline_edit_proposes_a_diff_without_writing_disk() {
        let (dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round(tools::WRITE_FILE, r#"{"path":"new.txt","content":"hello"}"#),
            text_round("Done."),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut session = HeadlessSession::new(SessionId("s1".to_string()), ws, config);
        let (updates_tx, mut updates_rx) = mpsc::channel(16);
        let (permissions_tx, _permissions_rx) = mpsc::channel(16);
        let (browser_tx, _browser_rx) = mpsc::channel(16);
        let (browser_interact_tx, _browser_interact_rx) = mpsc::channel(16);

        let prompt = tokio::spawn(async move {
            let stop = session
                .prompt(
                    "add new.txt".to_string(),
                    Vec::new(),
                    Some(Mode::Edit),
                    &updates_tx,
                    &permissions_tx,
                    &browser_tx,
                    &browser_interact_tx,
                )
                .await;
            (stop, session)
        });

        let (stop, _session) = prompt.await.expect("task joins");
        assert_eq!(stop, StopReason::EndTurn);

        let mut saw_diff = false;
        while let Ok(update) = updates_rx.try_recv() {
            if let SessionUpdate::ToolCallUpdate(ToolCallUpdate {
                content: Some(ToolCallContent::Diff { path, new_text, .. }),
                ..
            }) = update
            {
                assert_eq!(path, "new.txt");
                assert_eq!(new_text, "hello");
                saw_diff = true;
            }
        }
        assert!(
            saw_diff,
            "the write_file proposal must ride a Diff tool update"
        );
        assert!(
            !dir.path().join("new.txt").exists(),
            "inline edit must propose without writing disk"
        );
    }
}
