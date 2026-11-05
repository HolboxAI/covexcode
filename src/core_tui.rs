//! The opt-in bridge that drives the TUI through [`crate::session_client`]
//! instead of `App`'s own agent loop.
//!
//! Enabled by `COVEXCODE_CORE_SESSION=1`. Deliberately partial in this first
//! slice: the engine owns the turn, so text, tool results, usage and approval
//! prompts are real and stream in normally, but the TUI's own surfaces that
//! assume it owns the agent have not all moved yet:
//!
//! - `/new`, `/compact` and `/rollback` still operate on `App`'s local
//!   history, not the engine's;
//! - `--resume` loads App's history only, so the engine starts empty.
//!
//! The old path stays the default and is untouched; with the variable unset
//! nothing in this module runs.

use crate::app::{App, AppState};
use crate::approval::ApprovalRequest;
use crate::domain::{
    ApprovalDetail, ApprovalOption, ApprovalOptionKind, ApprovalResolution, SessionCommand,
    SessionEvent, ToolStatus,
};
use crate::llm::{ApiUsage, FunctionCall, ToolCall};
use crate::message::Role;
use crate::session_client::{EngineClient, SpawnedSession};
use crate::tools::Action;
use crate::workspace::Workspace;
use std::collections::HashMap;

/// The option ids to send for "yes" and "no". Looked up from the request's
/// own options rather than hardcoded, so a future engine option set does not
/// silently break the prompt.
#[derive(Default)]
struct ApprovalChoice {
    allow: String,
    reject: String,
}

impl ApprovalChoice {
    fn from_options(options: &[ApprovalOption]) -> Self {
        let find = |allow: bool| {
            options
                .iter()
                .find(|option| {
                    matches!(
                        option.kind,
                        ApprovalOptionKind::AllowOnce | ApprovalOptionKind::AllowAlways
                    ) == allow
                })
                .map(|option| option.id.clone())
        };
        Self {
            allow: find(true).unwrap_or_else(|| "allow".to_string()),
            reject: find(false).unwrap_or_else(|| "reject".to_string()),
        }
    }
}

/// One running engine plus the TUI-side state needed to render its events.
pub struct CoreTui {
    client: EngineClient,
    events: tokio::sync::mpsc::UnboundedReceiver<SessionEvent>,
    /// Engine call id -> the title shown when it finishes. `ToolCallUpdated`
    /// does not repeat the title, so it is remembered from the start event.
    titles: HashMap<String, String>,
    /// Engine approval id -> the option ids its prompt can answer with.
    pending_approvals: HashMap<u64, ApprovalChoice>,
    /// Whether the events currently arriving belong to the turn the TUI is
    /// showing. Cleared on cancel and turn end so late events from a stopped
    /// turn cannot paint into the next one.
    active: bool,
}

impl CoreTui {
    pub fn enabled() -> bool {
        matches!(
            std::env::var("COVEXCODE_CORE_SESSION").ok().as_deref(),
            Some("1") | Some("true")
        )
    }

    pub fn start(workspace: Workspace, config: crate::config::Config) -> Self {
        let SpawnedSession { client, events } = EngineClient::spawn(workspace, config);
        tracing::info!(
            session = %client.session_id(),
            "driving the TUI through the session engine"
        );
        Self {
            client,
            events,
            titles: HashMap::new(),
            pending_approvals: HashMap::new(),
            active: false,
        }
    }

    /// Start the turn `App::submit` queued. A no-op unless the app is in
    /// `Sending`, mirroring `agent::fire_request`.
    pub fn fire(&mut self, app: &mut App) {
        if app.state != AppState::Sending {
            return;
        }
        let Some(message) = app.messages.iter().rev().find(|m| m.role == Role::User) else {
            return;
        };
        let text = message.content.clone();
        let images = message.images.clone();
        let mode = Some(app.mode);

        // Same bookkeeping `agent::fire_request` performs, so the spinner,
        // elapsed clock and streaming output behave identically.
        app.request_id += 1;
        app.request_started = Some(std::time::Instant::now());
        app.state = AppState::Streaming;
        self.active = true;
        self.titles.clear();
        self.pending_approvals.clear();

        // The turn runs on the engine actor, not an abortable App task. Esc is
        // handled explicitly by `cancel` below.
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.prompt(text, images, mode).await;
        });
    }

    /// Stop the engine turn. Called when the app leaves a busy state without
    /// completing on its own (Esc, Ctrl-C's confirm path).
    pub fn cancel(&mut self, app: &mut App) {
        self.active = false;
        self.pending_approvals.clear();
        app.core_approval = None;
        app.core_decision = None;
        self.client.apply(SessionCommand::Cancel);
    }

    /// Send the answer `App::decide` just recorded, if any. The engine owns
    /// the call, so the decision travels as a `SessionCommand` rather than
    /// through App's own tool runner.
    pub fn answer_queued_decision(&mut self, app: &mut App) {
        let Some((request_id, allowed)) = app.core_decision.take() else {
            return;
        };
        let choice = self
            .pending_approvals
            .remove(&request_id)
            .unwrap_or_default();
        let option_id = if allowed { choice.allow } else { choice.reject };
        self.client.apply(SessionCommand::RespondApproval {
            request_id,
            resolution: ApprovalResolution::Selected { option_id },
        });
    }

    /// Move every queued engine event into the app. Non-blocking, like the
    /// stream channel drain it sits beside.
    pub fn drain(&mut self, app: &mut App) {
        while let Ok(event) = self.events.try_recv() {
            self.apply(app, event);
        }
    }

    fn apply(&mut self, app: &mut App, event: SessionEvent) {
        if !self.active {
            return;
        }
        match event {
            SessionEvent::TextDelta { text, .. } => app.append_token(&text),
            SessionEvent::ToolCallStarted {
                tool_call_id,
                title,
                ..
            } => {
                self.titles.insert(tool_call_id, title);
            }
            SessionEvent::ToolCallUpdated {
                tool_call_id,
                status,
                text,
                ..
            } => {
                let Some(status) = status else { return };
                if !matches!(status, ToolStatus::Completed | ToolStatus::Failed) {
                    return;
                }
                let title = self
                    .titles
                    .remove(&tool_call_id)
                    .unwrap_or_else(|| tool_call_id.clone());
                let display = match status {
                    ToolStatus::Failed => format!("{title} — failed"),
                    _ => format!("{title} — done"),
                };
                app.push_tool_outcome(crate::tools::ToolOutcome {
                    call_id: tool_call_id,
                    display,
                    content: text.unwrap_or_default(),
                    diff: None,
                    rollback: None,
                });
            }
            SessionEvent::UsageUpdated {
                used,
                prompt_tokens,
                ..
            } => {
                let prompt = prompt_tokens.unwrap_or(0) as usize;
                app.record_exact_usage(ApiUsage {
                    prompt_tokens: prompt,
                    completion_tokens: (used as usize).saturating_sub(prompt),
                    ..Default::default()
                });
            }
            SessionEvent::ApprovalRequested {
                request_id,
                tool_call_id,
                title,
                detail,
                options,
                ..
            } => {
                // The ask is the only event that carries this call's title for
                // the dangerous path (no `ToolCallStarted` is sent before an
                // approval), so remember it for the completion update.
                self.titles.insert(tool_call_id.clone(), title.clone());
                self.pending_approvals
                    .insert(request_id, ApprovalChoice::from_options(&options));
                app.show_core_approval(request_id, approval_request(tool_call_id, title, detail));
            }
            SessionEvent::ApprovalResolved {
                request_id,
                resolution,
                ..
            } => {
                self.pending_approvals.remove(&request_id);
                // The request may have ended without an answer from this UI
                // (engine timeout, cancellation): close the prompt rather than
                // leave a question on screen the engine is no longer asking.
                if app.core_approval == Some(request_id) {
                    app.core_approval = None;
                    app.overlay = None;
                    app.state = AppState::Streaming;
                    if matches!(
                        resolution,
                        ApprovalResolution::Unavailable | ApprovalResolution::Cancelled
                    ) {
                        app.note("The engine cancelled that approval request.".to_string());
                    }
                }
            }
            SessionEvent::TurnFinished { .. } => {
                self.active = false;
                self.pending_approvals.clear();
                app.finish_stream();
            }
            // TurnStarted/StatusChanged carry no rendering this slice needs;
            // the status is implied by App's own state.
            SessionEvent::TurnStarted { .. } | SessionEvent::StatusChanged { .. } => {}
        }
    }
}

/// Build the popup's view of an engine ask. The engine event carries the
/// interpreted detail rather than the raw tool call, so the action is
/// reconstructed for display: a diff is shown as a write preview, and
/// everything else falls back to the title the engine already formatted
/// (for a command that title is `$ <command>`).
fn approval_request(
    tool_call_id: String,
    title: String,
    detail: Option<ApprovalDetail>,
) -> ApprovalRequest {
    let (action, preview) = match detail {
        Some(ApprovalDetail::Diff {
            path,
            old_text,
            new_text,
        }) => {
            let preview = crate::diff::diff(old_text.as_deref().unwrap_or(""), &new_text);
            (
                Action::Write {
                    path,
                    content: new_text,
                },
                Some(preview),
            )
        }
        Some(ApprovalDetail::Text { text }) => (
            Action::Command {
                command: text,
                purpose: None,
            },
            None,
        ),
        _ => {
            let command = title.strip_prefix("$ ").unwrap_or(&title).to_string();
            (
                Action::Command {
                    command,
                    purpose: None,
                },
                None,
            )
        }
    };
    ApprovalRequest {
        // The popup renders `action`, not the raw call; the engine already
        // holds the real one and executes it after this answer.
        call: ToolCall {
            id: tool_call_id,
            kind: "function".to_string(),
            function: FunctionCall {
                name: String::new(),
                arguments: "{}".to_string(),
            },
        },
        action,
        remaining: 0,
        preview,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TurnId;
    use crate::message::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn workspace() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("temp dir");
        let ws = Workspace::new(dir.path()).expect("workspace");
        (dir, ws)
    }

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

    async fn read_request(socket: &mut tokio::net::TcpStream) {
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
    }

    async fn serve_rounds(responses: Vec<String>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener.accept().await.expect("accept");
                read_request(&mut socket).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{addr}")
    }

    fn config_for(endpoint: &str) -> crate::config::Config {
        let mut config = crate::config::Config::default();
        config.llm.endpoint = endpoint.to_string();
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();
        config
    }

    fn core_with_workspace() -> (tempfile::TempDir, CoreTui, App) {
        let (dir, ws) = workspace();
        let mut core = CoreTui::start(ws, crate::config::Config::default());
        core.active = true;
        let mut app = App::new(crate::config::Config::default());
        app.state = AppState::Streaming;
        (dir, core, app)
    }

    fn event_id() -> crate::domain::SessionId {
        crate::domain::SessionId::new("local_test")
    }

    #[tokio::test]
    async fn text_deltas_stream_into_the_app_and_turn_finish_settles_it() {
        let (_dir, mut core, mut app) = core_with_workspace();

        core.apply(
            &mut app,
            SessionEvent::TextDelta {
                session_id: event_id(),
                turn: TurnId::new(1),
                text: "Hello!".to_string(),
            },
        );
        assert_eq!(app.streaming_response, "Hello!");

        core.apply(
            &mut app,
            SessionEvent::TurnFinished {
                session_id: event_id(),
                turn: TurnId::new(1),
                reason: crate::domain::TurnStopReason::EndTurn,
            },
        );
        assert_eq!(app.state, AppState::AwaitingInput);
        assert!(app
            .messages
            .iter()
            .any(|m| m.role == Role::Assistant && m.content == "Hello!"));
        assert!(!core.active, "late events are ignored after the turn");
    }

    #[tokio::test]
    async fn a_finished_tool_call_becomes_a_transcript_entry() {
        let (_dir, mut core, mut app) = core_with_workspace();

        core.apply(
            &mut app,
            SessionEvent::ToolCallStarted {
                session_id: event_id(),
                turn: TurnId::new(1),
                tool_call_id: "call_1".to_string(),
                title: "$ ls".to_string(),
            },
        );
        core.apply(
            &mut app,
            SessionEvent::ToolCallUpdated {
                session_id: event_id(),
                turn: TurnId::new(1),
                tool_call_id: "call_1".to_string(),
                status: Some(ToolStatus::Completed),
                text: Some("a.rs\nb.rs".to_string()),
            },
        );

        let last = app.messages.last().expect("a tool message");
        assert!(last.role == Role::Tool);
        assert_eq!(last.display.as_deref(), Some("$ ls — done"));
        assert_eq!(last.content, "a.rs\nb.rs");
        assert_eq!(last.tool_call_id.as_deref(), Some("call_1"));
    }

    fn approval_event(request_id: u64, detail: Option<ApprovalDetail>) -> SessionEvent {
        SessionEvent::ApprovalRequested {
            session_id: event_id(),
            turn: TurnId::new(1),
            request_id,
            tool_call_id: "call_1".to_string(),
            title: "$ rm -rf build".to_string(),
            detail,
            options: vec![
                ApprovalOption {
                    id: "allow".to_string(),
                    label: "Allow".to_string(),
                    kind: ApprovalOptionKind::AllowOnce,
                },
                ApprovalOption {
                    id: "reject".to_string(),
                    label: "Reject".to_string(),
                    kind: ApprovalOptionKind::RejectOnce,
                },
            ],
        }
    }

    #[tokio::test]
    async fn an_approval_request_opens_the_tui_prompt() {
        let (_dir, mut core, mut app) = core_with_workspace();

        core.apply(&mut app, approval_event(7, None));

        assert_eq!(app.state, AppState::AwaitingApproval);
        assert_eq!(app.core_approval, Some(7));
        match app.overlay.as_ref() {
            Some(crate::app::Overlay::ToolApproval(request)) => {
                assert_eq!(request.action.label(), "$ rm -rf build");
            }
            other => panic!("expected the approval overlay, got {other:?}"),
        }
    }

    /// A write's diff reaches the popup as the same hunked preview the App
    /// path would have computed.
    #[tokio::test]
    async fn an_approval_diff_becomes_the_popup_preview() {
        let (_dir, mut core, mut app) = core_with_workspace();

        core.apply(
            &mut app,
            approval_event(
                7,
                Some(ApprovalDetail::Diff {
                    path: "a.rs".to_string(),
                    old_text: Some("old\n".to_string()),
                    new_text: "new\n".to_string(),
                }),
            ),
        );

        match app.overlay.as_ref() {
            Some(crate::app::Overlay::ToolApproval(request)) => {
                assert_eq!(request.action.label(), "write a.rs");
                let preview = request.preview.as_ref().expect("the diff is shown");
                assert_eq!(preview.added, 1);
                assert_eq!(preview.removed, 1);
            }
            other => panic!("expected the approval overlay, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn approving_records_the_allow_option_for_the_engine() {
        let (_dir, mut core, mut app) = core_with_workspace();
        core.apply(&mut app, approval_event(7, None));

        app.decide(crate::approval::Decision::Allowed);

        assert_eq!(app.core_decision, Some((7, true)));
        assert_eq!(app.core_approval, None, "the prompt closes on answer");
        assert_eq!(app.state, AppState::Streaming, "the turn continues");
        core.answer_queued_decision(&mut app);
        assert_eq!(app.core_decision, None, "the answer is sent once");
        assert!(core.pending_approvals.is_empty());
    }

    #[tokio::test]
    async fn refusing_records_the_reject_option_for_the_engine() {
        let (_dir, mut core, mut app) = core_with_workspace();
        core.apply(&mut app, approval_event(7, None));

        app.decide(crate::approval::Decision::Refused);

        assert_eq!(app.core_decision, Some((7, false)));
        core.answer_queued_decision(&mut app);
        assert!(core.pending_approvals.is_empty());
    }

    /// If the engine ends the ask without this UI answering, the prompt must
    /// not stay on screen for a question nobody is asking anymore.
    #[tokio::test]
    async fn an_engine_cancelled_approval_closes_the_prompt() {
        let (_dir, mut core, mut app) = core_with_workspace();
        core.apply(&mut app, approval_event(7, None));

        core.apply(
            &mut app,
            SessionEvent::ApprovalResolved {
                session_id: event_id(),
                turn: TurnId::new(1),
                request_id: 7,
                resolution: ApprovalResolution::Cancelled,
            },
        );

        assert_eq!(app.core_approval, None);
        assert!(app.overlay.is_none());
        assert_eq!(app.state, AppState::Streaming);
        assert!(app
            .messages
            .iter()
            .any(|m| m.role == Role::System && m.content.contains("cancelled that approval")));
    }

    #[tokio::test]
    async fn usage_updates_reach_the_apps_meters() {
        let (_dir, mut core, mut app) = core_with_workspace();

        core.apply(
            &mut app,
            SessionEvent::UsageUpdated {
                session_id: event_id(),
                turn: TurnId::new(1),
                used: 300,
                size: 131_072,
                prompt_tokens: Some(200),
            },
        );

        let usage = app.exact_usage.expect("exact usage recorded");
        assert_eq!(usage.prompt_tokens, 200);
        assert_eq!(usage.completion_tokens, 100);
        assert_eq!(app.last_prompt_tokens, Some(200));
    }

    #[tokio::test]
    async fn a_user_message_is_the_prompt_sent_to_the_engine() {
        // `fire` itself only reads App state; the engine actor is spawned by
        // `start` but receives nothing until `fire` sends it.
        let (_dir, ws) = workspace();
        let mut core = CoreTui::start(ws, crate::config::Config::default());
        let mut app = App::new(crate::config::Config::default());
        app.messages.push(Message::new(Role::User, "hi there"));
        app.state = AppState::Sending;

        core.fire(&mut app);

        assert_eq!(app.state, AppState::Streaming);
        assert!(core.active);
        assert_eq!(app.request_id, 1);
    }

    /// The seam the unit tests above cannot cover: the engine asks, the TUI
    /// shows it, the user's answer reaches `SessionCommand`, and the engine
    /// finishes the turn. A real mock endpoint drives the first round.
    #[tokio::test]
    async fn an_approval_answered_in_the_tui_reaches_the_engine() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round("run_command", r#"{"command":"rm -rf build"}"#),
            text_round("stopped"),
        ])
        .await;
        let config = config_for(&endpoint);
        let mut core = CoreTui::start(ws, config.clone());
        let mut app = App::new(config);
        app.messages
            .push(Message::new(Role::User, "clean the build dir"));
        app.state = AppState::Sending;

        core.fire(&mut app);

        let mut answered = false;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                core.drain(&mut app);
                core.answer_queued_decision(&mut app);
                if !answered && app.state == AppState::AwaitingApproval {
                    answered = true;
                    app.decide(crate::approval::Decision::Allowed);
                    core.answer_queued_decision(&mut app);
                }
                if answered && !core.active {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the turn finished after the approval");

        assert!(answered, "the engine asked and the TUI answered");
        assert!(
            app.messages.iter().any(|m| m.role == Role::Tool
                && m.display.as_deref().unwrap_or("").contains("rm -rf build")),
            "the executed call is in the transcript"
        );
        assert!(app
            .messages
            .iter()
            .any(|m| m.role == Role::Assistant && m.content == "stopped"));
    }
}
