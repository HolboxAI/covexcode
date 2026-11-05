//! In-process client for [`HeadlessSession`] -- the seam a frontend uses
//! instead of ACP.
//!
//! `transport` speaks ACP over a byte stream and keeps its own actor; this
//! module speaks the domain types (`SessionEvent`, `SessionCommand`) directly,
//! so a frontend can drive one engine without a serialized protocol between
//! itself and its own agent loop. It is the piece the TUI migration needs:
//! render [`SessionEvent`]s, send prompts, answer approvals, cancel.
//!
//! Deliberately not a second engine. It owns exactly one actor task, which
//! runs [`HeadlessSession::prompt`] one call at a time -- the same rule the
//! ACP path follows ("turns are serialized per session").
//!
//! Browser asks are answered with a failure here instead of being surfaced:
//! a terminal client has no page to check, and leaving the engine waiting for
//! its 90s timeout would be a worse answer than saying so immediately. A
//! frontend that does have a browser surface (the IDE) keeps using the ACP
//! ask path.

use crate::config::Config;
use crate::domain::{SessionCommand, SessionEvent, SessionId};
use crate::headless::{
    BrowserCheckAsk, BrowserCheckResult, BrowserInteractAsk, BrowserInteractResult,
    HeadlessSession, SessionCommandHandle,
};
use crate::llm::ImageAttachment;
use crate::protocol::StopReason;
use crate::tools::Mode;
use crate::workspace::Workspace;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};

/// One prompt sent to the client's actor.
enum ClientMsg {
    Prompt {
        text: String,
        images: Vec<ImageAttachment>,
        mode: Option<Mode>,
        respond: oneshot::Sender<StopReason>,
    },
}

/// A frontend's handle to one running engine. Dropping it ends the actor
/// after the current turn, the same way dropping a transport's command
/// sender does. Cloneable so a UI can hand one copy to a prompt task while
/// keeping its own for cancel and approval answers.
#[derive(Clone)]
pub struct EngineClient {
    session_id: SessionId,
    commands: mpsc::Sender<ClientMsg>,
    control: SessionCommandHandle,
}

/// The two halves a frontend drives: commands go through `client`, progress
/// arrives on `events`. Separate values rather than one struct so a UI can
/// hold the event receiver mutably while awaiting a prompt.
pub struct SpawnedSession {
    pub client: EngineClient,
    pub events: mpsc::UnboundedReceiver<SessionEvent>,
}

impl EngineClient {
    /// Start one engine on its own task, in `workspace`, with `config`.
    ///
    /// A process-local id, not a wire id: the TUI runs one session per
    /// process, and the ACP server's registry owns the ids that travel.
    pub fn spawn(workspace: Workspace, config: Config) -> SpawnedSession {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let session_id = SessionId::new(format!("local_{id}"));

        let mut session = HeadlessSession::new(session_id.clone(), workspace, config);
        let events = session.subscribe_events();
        let control = session.command_handle();

        let (commands, mut rx) = mpsc::channel::<ClientMsg>(8);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ClientMsg::Prompt {
                        text,
                        images,
                        mode,
                        respond,
                    } => {
                        let stop = run_prompt(&mut session, text, images, mode).await;
                        let _ = respond.send(stop);
                    }
                }
            }
        });

        SpawnedSession {
            client: Self {
                session_id,
                commands,
                control,
            },
            events,
        }
    }

    /// The id this client's engine was created with. Used once the TUI logs
    /// which local session it is driving.
    #[allow(dead_code)]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    /// Apply a [`SessionCommand`] without waiting for the actor -- cancel and
    /// approval answers must reach an in-flight turn, which is exactly the
    /// actor that is busy.
    pub fn apply(&self, command: SessionCommand) -> bool {
        self.control.apply(command)
    }

    /// Send one prompt and wait for its turn to finish. Progress streams on
    /// the event channel while this waits.
    pub async fn prompt(
        &self,
        text: impl Into<String>,
        images: Vec<ImageAttachment>,
        mode: Option<Mode>,
    ) -> Option<StopReason> {
        let (respond, receive) = oneshot::channel();
        self.commands
            .send(ClientMsg::Prompt {
                text: text.into(),
                images,
                mode,
                respond,
            })
            .await
            .ok()?;
        receive.await.ok()
    }
}

/// One turn through the engine, with the ACP-shaped side channels drained.
///
/// The engine reports progress two ways: `SessionEvent`s (what a frontend
/// renders) and `SessionUpdate`s (the ACP wire). The client uses the former;
/// the latter is drained and dropped so the engine's sends never block on a
/// receiver nobody reads.
async fn run_prompt(
    session: &mut HeadlessSession,
    text: String,
    images: Vec<ImageAttachment>,
    mode: Option<Mode>,
) -> StopReason {
    let (updates_tx, mut updates_rx) = mpsc::channel(64);
    let updates_drain = tokio::spawn(async move { while updates_rx.recv().await.is_some() {} });

    // Approvals are surfaced as events and answered through the control
    // handle, so the ACP-shaped ask channel is drained too.
    let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
    let permissions_drain =
        tokio::spawn(async move { while permissions_rx.recv().await.is_some() {} });

    let (browser_tx, mut browser_rx) = mpsc::channel::<BrowserCheckAsk>(16);
    let browser_drain = tokio::spawn(async move {
        while let Some(ask) = browser_rx.recv().await {
            let _ = ask.answers.respond(
                ask.request_id,
                BrowserCheckResult::Failed(BROWSER_UNAVAILABLE.to_string()),
            );
        }
    });
    let (browser_interact_tx, mut browser_interact_rx) = mpsc::channel::<BrowserInteractAsk>(16);
    let browser_interact_drain = tokio::spawn(async move {
        while let Some(ask) = browser_interact_rx.recv().await {
            let _ = ask.answers.respond(
                ask.request_id,
                BrowserInteractResult::Failed(BROWSER_UNAVAILABLE.to_string()),
            );
        }
    });

    let stop = session
        .prompt(
            text,
            images,
            mode,
            &updates_tx,
            &permissions_tx,
            &browser_tx,
            &browser_interact_tx,
        )
        .await;

    // Let each drain see its channel close, the same order the ACP actor
    // uses, so no event is still queued when the prompt's response resolves.
    drop(updates_tx);
    let _ = updates_drain.await;
    drop(permissions_tx);
    let _ = permissions_drain.await;
    drop(browser_tx);
    let _ = browser_drain.await;
    drop(browser_interact_tx);
    let _ = browser_interact_drain.await;
    stop
}

const BROWSER_UNAVAILABLE: &str = "browser checks are not available in this client yet";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ApprovalResolution;
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

    async fn serve_delayed(delay: std::time::Duration, response: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            read_request(&mut socket).await;
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
        format!("http://{addr}")
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

    /// Drain events until the engine reports the turn finished, returning
    /// everything seen along the way.
    async fn collect_turn(events: &mut mpsc::UnboundedReceiver<SessionEvent>) -> Vec<SessionEvent> {
        let mut seen = Vec::new();
        while let Some(event) = events.recv().await {
            let finished = matches!(event, SessionEvent::TurnFinished { .. });
            seen.push(event);
            if finished {
                return seen;
            }
        }
        panic!("the event stream closed before the turn finished");
    }

    #[tokio::test]
    async fn a_prompt_streams_events_and_finishes() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![text_round("Hello!")]).await;
        let SpawnedSession { client, mut events } = EngineClient::spawn(ws, config_for(&endpoint));

        let (stop, seen) = tokio::join!(
            client.prompt("hi", Vec::new(), None),
            collect_turn(&mut events)
        );

        assert_eq!(stop, Some(StopReason::EndTurn));
        assert!(matches!(
            seen.first(),
            Some(SessionEvent::TurnStarted { turn, .. }) if turn.get() == 1
        ));
        assert!(seen.iter().any(|event| matches!(
            event,
            SessionEvent::TextDelta { text, .. } if text == "Hello!"
        )));
        assert!(seen.iter().any(|event| matches!(
            event,
            SessionEvent::TurnFinished {
                reason: crate::domain::TurnStopReason::EndTurn,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn a_control_command_cancels_the_running_turn() {
        let (_dir, ws) = workspace();
        let endpoint = serve_delayed(std::time::Duration::from_secs(30), text_round("late")).await;
        let SpawnedSession { client, mut events } = EngineClient::spawn(ws, config_for(&endpoint));

        let cancel = async {
            // Cancel only once the turn is genuinely in flight.
            while let Some(event) = events.recv().await {
                if matches!(event, SessionEvent::TurnStarted { .. }) {
                    break;
                }
            }
            assert!(client.apply(SessionCommand::Cancel));
            while let Some(event) = events.recv().await {
                if matches!(event, SessionEvent::TurnFinished { .. }) {
                    break;
                }
            }
        };

        let (stop, ()) = tokio::join!(client.prompt("hi", Vec::new(), None), cancel);
        assert_eq!(stop, Some(StopReason::Cancelled));
    }

    #[tokio::test]
    async fn an_approval_is_answered_through_the_client() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round("run_command", r#"{"command":"rm -rf build"}"#),
            text_round("Done."),
        ])
        .await;
        let SpawnedSession { client, mut events } = EngineClient::spawn(ws, config_for(&endpoint));

        let approve = async {
            while let Some(event) = events.recv().await {
                if let SessionEvent::ApprovalRequested { request_id, .. } = event {
                    return client.apply(SessionCommand::RespondApproval {
                        request_id,
                        resolution: ApprovalResolution::Selected {
                            option_id: "allow".to_string(),
                        },
                    });
                }
            }
            panic!("the event stream closed before an approval request");
        };

        let (stop, approved) = tokio::join!(client.prompt("run it", Vec::new(), None), approve);
        assert!(approved, "the approval id was outstanding");
        assert_eq!(stop, Some(StopReason::EndTurn));
    }

    /// A browser ask this client cannot fulfill is answered immediately with
    /// a failure instead of stalling for the engine's 90s timeout.
    #[tokio::test]
    async fn a_browser_ask_fails_fast_instead_of_hanging() {
        let (_dir, ws) = workspace();
        let endpoint = serve_rounds(vec![
            tool_call_round("check_in_browser", r#"{"url":"http://localhost:3000"}"#),
            text_round("No browser here."),
        ])
        .await;
        let SpawnedSession { client, mut events } = EngineClient::spawn(ws, config_for(&endpoint));

        let (stop, seen) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                client.prompt("check it", Vec::new(), None),
                collect_turn(&mut events)
            )
        })
        .await
        .expect("the browser ask did not stall the turn");

        assert_eq!(stop, Some(StopReason::EndTurn));
        assert!(seen.iter().any(|event| matches!(
            event,
            SessionEvent::ToolCallUpdated {
                status: Some(crate::domain::ToolStatus::Failed),
                ..
            }
        )));
    }
}
