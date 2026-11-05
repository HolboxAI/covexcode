//! The opt-in bridge that drives the TUI through [`crate::session_client`]
//! instead of `App`'s own agent loop.
//!
//! Enabled by `COVEXCODE_CORE_SESSION=1`. Deliberately partial in this first
//! slice: the engine owns the turn, so text, tool results and usage are real
//! and stream in normally, but the TUI's own surfaces that assume it owns the
//! agent have not moved yet:
//!
//! - approval prompts are refused with a transcript note (there is no
//!   key-driven approval UI wired to [`crate::domain::SessionCommand`] yet);
//! - `/new`, `/compact` and `/rollback` still operate on `App`'s local
//!   history, not the engine's;
//! - `--resume` loads App's history only, so the engine starts empty.
//!
//! The old path stays the default and is untouched; with the variable unset
//! nothing in this module runs.

use crate::app::{App, AppState};
use crate::domain::{ApprovalResolution, SessionCommand, SessionEvent, ToolStatus};
use crate::llm::ApiUsage;
use crate::message::Role;
use crate::session_client::{EngineClient, SpawnedSession};
use crate::workspace::Workspace;
use std::collections::HashMap;

/// One running engine plus the TUI-side state needed to render its events.
pub struct CoreTui {
    client: EngineClient,
    events: tokio::sync::mpsc::UnboundedReceiver<SessionEvent>,
    /// Engine call id -> the title shown when it finishes. `ToolCallUpdated`
    /// does not repeat the title, so it is remembered from the start event.
    titles: HashMap<String, String>,
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

        // The turn runs on the engine actor, not an abortable App task. Esc is
        // handled explicitly by `cancel` below.
        let client = self.client.clone();
        tokio::spawn(async move {
            let _ = client.prompt(text, images, mode).await;
        });
    }

    /// Stop the engine turn. Called when the app leaves a busy state without
    /// completing on its own (Esc, Ctrl-C's confirm path).
    pub fn cancel(&mut self) {
        self.active = false;
        self.client.apply(SessionCommand::Cancel);
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
                request_id, title, ..
            } => {
                // No key-driven approval UI is wired to the engine yet. Refuse
                // instead of leaving the turn waiting on a prompt nobody can
                // answer, and say so where the user will see it.
                self.client.apply(SessionCommand::RespondApproval {
                    request_id,
                    resolution: ApprovalResolution::Cancelled,
                });
                app.note(format!(
                    "{title} was refused: core-session mode has no approval UI yet."
                ));
            }
            SessionEvent::TurnFinished { .. } => {
                self.active = false;
                app.finish_stream();
            }
            // TurnStarted/StatusChanged/ApprovalResolved carry no rendering
            // this slice needs; the status is implied by App's own state.
            SessionEvent::TurnStarted { .. }
            | SessionEvent::StatusChanged { .. }
            | SessionEvent::ApprovalResolved { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TurnId;
    use crate::message::Message;

    fn workspace() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("temp dir");
        let ws = Workspace::new(dir.path()).expect("workspace");
        (dir, ws)
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

    #[tokio::test]
    async fn an_approval_is_refused_instead_of_leaving_the_turn_waiting() {
        let (_dir, mut core, mut app) = core_with_workspace();

        core.apply(
            &mut app,
            SessionEvent::ApprovalRequested {
                session_id: event_id(),
                turn: TurnId::new(1),
                request_id: 1,
                tool_call_id: "call_1".to_string(),
                title: "rm -rf build".to_string(),
                detail: None,
                options: Vec::new(),
            },
        );

        let last = app.messages.last().expect("a note");
        assert!(last.role == Role::System);
        assert!(last.content.contains("refused"), "{}", last.content);
        assert!(last.content.contains("rm -rf build"), "{}", last.content);
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
}
