//! The real stdio wiring for [`crate::headless::HeadlessSession`] -- reads
//! ACP JSON-RPC off stdin, dispatches to sessions, writes responses and
//! `session/update`/`session/request_permission` messages to stdout. This
//! is the last piece between "the turn loop works" (`headless.rs`, tested
//! end to end already) and "you can spawn `covexcode --acp` as a subprocess
//! and talk to it."
//!
//! **Why this is the hard part, precisely:** ACP is bidirectional over one
//! connection. Requests flow both ways -- the client sends `session/prompt`
//! and expects a response; the agent (this process), mid-`session/prompt`,
//! sends its own `session/request_permission` request and needs *its*
//! response, correlated by an id this process assigns, while the same
//! stdin-reading loop keeps running so that response can actually arrive.
//!
//! That means `session/prompt`'s own JSON-RPC response can't be produced
//! synchronously inside the read loop's `handle()` call, even though
//! `HeadlessSession::prompt` (deliberately, per ACP v1) blocks until the
//! whole turn -- including any permission round trip -- is done. If
//! `handle()` awaited that inline, the read loop would be stuck inside one
//! `await` for the entire turn and could never reach the branch that writes
//! the outgoing `session/request_permission` line or the branch that reads
//! the client's answer to it: a real deadlock, not a hypothetical one.
//! `dispatch_prompt_deferred` avoids it by handing the wait for the turn's
//! result to its own spawned task, which reports back through the same
//! `Outgoing` channel [`SessionActor`]'s update-draining task already
//! writes into, instead of blocking `handle`'s own caller.
//!
//! Routing/classification logic is kept separate from real I/O
//! ([`classify`], [`Router`]'s methods) so it's unit-testable without a
//! real stdin/stdout pipe -- only [`run`] itself touches real I/O, and it
//! isn't unit tested for the same reason `main`'s own top-level loop isn't.

use crate::config::Config;
use crate::domain::{ApprovalResolution, SessionEvent, SessionStatus};
use crate::headless::{
    ApprovalHandle, AskHandle, BrowserCheckAsk, BrowserCheckResult, BrowserInteractAsk,
    BrowserInteractResult, CancelHandle, HeadlessSession, PermissionAsk,
};
use crate::protocol::{
    CheckInBrowserOutcome, CheckInBrowserRequest, Implementation, InitializeRequest,
    InitializeResponse, InteractInBrowserOutcome, InteractInBrowserRequest, JsonRpcVersion,
    ListChangesRequest, ListChangesResponse, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, RequestId, RequestPermissionOutcome, RollbackRequest,
    RollbackResponse, RpcError, RpcErrorObject, RpcRequest, RpcResponse, SessionId,
    SessionNotification, SessionUpdate, PROTOCOL_VERSION,
};
use crate::workspace::Workspace;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

/// One incoming line, classified. ACP's own JSON-RPC envelope doesn't carry
/// an explicit discriminant for this -- a request has `id` *and* `method`,
/// a notification has `method` alone, and a response to one of *our own*
/// outgoing requests has `id` alone (`result` or `error`, never `method`).
/// Classifying by which fields are present, rather than guessing from
/// shape, is what the JSON-RPC 2.0 spec itself says a receiver does.
#[derive(Debug, Clone, PartialEq)]
enum Incoming {
    Request {
        id: RequestId,
        method: String,
        params: serde_json::Value,
    },
    Notification {
        method: String,
        params: serde_json::Value,
    },
    Response {
        id: RequestId,
        result: serde_json::Value,
    },
    Error {
        id: RequestId,
        error: serde_json::Value,
    },
    /// Present but not a well-formed JSON-RPC message of any of the above
    /// shapes -- kept as its own case rather than silently dropped, so a
    /// caller can decide whether to log it.
    Malformed,
}

fn classify(value: &serde_json::Value) -> Incoming {
    let id = value.get("id").cloned();
    let method = value
        .get("method")
        .and_then(|m| m.as_str())
        .map(str::to_string);
    match (id, method) {
        (Some(id), Some(method)) => match serde_json::from_value::<RequestId>(id) {
            Ok(id) => Incoming::Request {
                id,
                method,
                params: value.get("params").cloned().unwrap_or(json!({})),
            },
            Err(_) => Incoming::Malformed,
        },
        (None, Some(method)) => Incoming::Notification {
            method,
            params: value.get("params").cloned().unwrap_or(json!({})),
        },
        (Some(id), None) => match serde_json::from_value::<RequestId>(id) {
            Ok(id) => {
                if let Some(result) = value.get("result") {
                    Incoming::Response {
                        id,
                        result: result.clone(),
                    }
                } else if let Some(error) = value.get("error") {
                    Incoming::Error {
                        id,
                        error: error.clone(),
                    }
                } else {
                    Incoming::Malformed
                }
            }
            Err(_) => Incoming::Malformed,
        },
        (None, None) => Incoming::Malformed,
    }
}

/// The custom session methods that take one id: attach and close.
#[derive(serde::Deserialize)]
struct SessionIdRequest {
    #[serde(rename = "sessionId")]
    session_id: SessionId,
}

/// A message sent to one session's own task.
enum SessionMsg {
    Prompt {
        text: String,
        images: Vec<crate::llm::ImageAttachment>,
        mode: Option<crate::tools::Mode>,
        respond: oneshot::Sender<PromptResponse>,
    },
    /// `session/rollback`. Queued behind whatever `Prompt` this actor's
    /// task is already working through -- one message at a time, same as
    /// `Prompt` itself -- which is exactly right: rolling back files the
    /// model might still be mid-turn writing to would be undoing work out
    /// from under it, not after it. Carries the request (with its optional
    /// `files`/`turn` filters) through to the session.
    Rollback {
        req: RollbackRequest,
        respond: oneshot::Sender<RollbackResponse>,
    },
    /// `session/list_changes` -- a read-only peek at the journal, answered
    /// synchronously like `Rollback`.
    ListChanges {
        respond: oneshot::Sender<ListChangesResponse>,
    },
}

/// Everything a spawned task can produce for the outgoing stdout stream,
/// asynchronously, outside of `Router::handle`'s own synchronous return
/// value -- see this module's own docs for why `session/prompt`'s response
/// has to travel this way rather than being returned directly.
#[derive(Clone)]
enum Outgoing {
    /// A `session/update` notification, still needing its envelope built.
    Update(SessionNotification),
    /// An already-serialized JSON-RPC line (a deferred request's response),
    /// ready to write as-is.
    Line(String),
}

/// Broadcasts one session's ACP updates to every connection attached to it.
///
/// Backpressure is deliberate: `send` awaits each connection's bounded
/// outgoing queue, and the actor answers `session/prompt` only after its
/// per-turn drain has pushed every update, so no client sees the response
/// before the updates that belong to it.
#[derive(Clone)]
struct UpdateHub {
    session_id: SessionId,
    subscribers: Arc<tokio::sync::Mutex<Vec<mpsc::Sender<Outgoing>>>>,
}

impl UpdateHub {
    fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            subscribers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    async fn subscribe(&self, outgoing: mpsc::Sender<Outgoing>) {
        self.subscribers.lock().await.push(outgoing);
    }

    async fn send(&self, update: SessionUpdate) {
        let message = Outgoing::Update(SessionNotification {
            session_id: self.session_id.clone(),
            update,
        });
        let mut subscribers = self.subscribers.lock().await;
        let mut index = 0;
        while index < subscribers.len() {
            if subscribers[index].send(message.clone()).await.is_err() {
                // A closed connection: drop it rather than failing the turn
                // for every other subscriber.
                subscribers.remove(index);
            } else {
                index += 1;
            }
        }
    }
}

/// Broadcasts one kind of engine ask to every attached connection. Each
/// connection sends its own ACP request; the ask's handle makes the first
/// answer win and the rest no-ops.
#[derive(Clone)]
struct RelayHub<T> {
    subscribers: Arc<tokio::sync::Mutex<Vec<mpsc::Sender<T>>>>,
}

impl<T: Clone + Send> RelayHub<T> {
    fn new() -> Self {
        Self {
            subscribers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    async fn subscribe(&self, relay: mpsc::Sender<T>) {
        self.subscribers.lock().await.push(relay);
    }

    async fn send(&self, item: T) {
        let mut subscribers = self.subscribers.lock().await;
        let mut index = 0;
        while index < subscribers.len() {
            if subscribers[index].send(item.clone()).await.is_err() {
                subscribers.remove(index);
            } else {
                index += 1;
            }
        }
    }
}

type ApprovalHub = RelayHub<PermissionAsk>;
type BrowserCheckHub = RelayHub<BrowserCheckAsk>;
type BrowserInteractHub = RelayHub<BrowserInteractAsk>;

/// Everything a session fans out to the connections attached to it. Grouped
/// so the registry carries one field, not four parallel ones.
#[derive(Clone)]
struct SessionChannels {
    updates: UpdateHub,
    approvals: ApprovalHub,
    browser_checks: BrowserCheckHub,
    browser_interactions: BrowserInteractHub,
}

impl SessionChannels {
    fn new(session_id: SessionId) -> Self {
        Self {
            updates: UpdateHub::new(session_id),
            approvals: ApprovalHub::new(),
            browser_checks: BrowserCheckHub::new(),
            browser_interactions: BrowserInteractHub::new(),
        }
    }
}

/// Owns one [`HeadlessSession`] and processes its `session/prompt` calls
/// one at a time on its own spawned task -- see this module's own docs for
/// why that has to be a separate task from the main read loop.
struct SessionActor;

impl SessionActor {
    fn spawn(
        mut session: HeadlessSession,
        channels: SessionChannels,
        mcp_configs: Vec<crate::mcp::McpServerConfig>,
    ) -> mpsc::Sender<SessionMsg> {
        let (tx, mut rx) = mpsc::channel::<SessionMsg>(8);
        tokio::spawn(async move {
            // Connect MCP servers once, before the first prompt. Off the request
            // path on purpose: spawning a server and handshaking takes seconds, so
            // session/new replied already. Problems are logged, not fatal -- a
            // server that will not start should not cost you the ones that work.
            for problem in session.connect_mcp(&mcp_configs).await {
                tracing::warn!(target: "mcp", "{}", problem);
            }
            tracing::info!(target: "mcp",
                servers = session.mcp().server_names().count(),
                tools = session.mcp().tool_count(),
                "connected mcp servers");
            while let Some(msg) = rx.recv().await {
                match msg {
                    SessionMsg::Prompt {
                        text,
                        images,
                        mode,
                        respond,
                    } => {
                        // A fresh channel per prompt, not one shared for
                        // the session's whole lifetime: a session answers
                        // many `session/prompt` calls over time, and each
                        // one's own drain task below has to see its own
                        // channel actually close (via the `drop` below) to
                        // know that specific turn is over, rather than the
                        // one from three turns ago.
                        let (updates_tx, mut updates_rx) = mpsc::channel(64);
                        // Drain this prompt's own update channel into the
                        // session's hub, which fans it out to every attached
                        // connection. HeadlessSession itself does not know
                        // about the wire, only about SessionUpdate values (see
                        // headless.rs's own module docs).
                        let hub = channels.updates.clone();
                        let update_drain = tokio::spawn(async move {
                            while let Some(update) = updates_rx.recv().await {
                                hub.send(update).await;
                            }
                        });
                        // The three ask kinds follow the same pattern: a fresh
                        // channel per prompt, drained into its hub so every
                        // attached connection sees the ask.
                        let (permissions_tx, mut permissions_rx) = mpsc::channel(16);
                        let approval_hub = channels.approvals.clone();
                        let approval_drain = tokio::spawn(async move {
                            while let Some(ask) = permissions_rx.recv().await {
                                approval_hub.send(ask).await;
                            }
                        });
                        let (browser_tx, mut browser_rx) = mpsc::channel(16);
                        let browser_hub = channels.browser_checks.clone();
                        let browser_drain = tokio::spawn(async move {
                            while let Some(ask) = browser_rx.recv().await {
                                browser_hub.send(ask).await;
                            }
                        });
                        let (browser_interact_tx, mut browser_interact_rx) = mpsc::channel(16);
                        let browser_interact_hub = channels.browser_interactions.clone();
                        let browser_interact_drain = tokio::spawn(async move {
                            while let Some(ask) = browser_interact_rx.recv().await {
                                browser_interact_hub.send(ask).await;
                            }
                        });
                        let stop_reason = session
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
                        drop(updates_tx); // let the drain task see the channel close
                        let _ = update_drain.await;
                        drop(permissions_tx);
                        let _ = approval_drain.await;
                        drop(browser_tx);
                        let _ = browser_drain.await;
                        drop(browser_interact_tx);
                        let _ = browser_interact_drain.await;
                        let _ = respond.send(PromptResponse {
                            stop_reason,
                            turn: session.current_turn(),
                        });
                    }
                    SessionMsg::Rollback { req, respond } => {
                        let _ = respond.send(session.rollback(&req));
                    }
                    SessionMsg::ListChanges { respond } => {
                        let _ = respond.send(session.list_changes());
                    }
                }
            }
        });
        tx
    }
}

/// One live session on this connection: the queue its actor reads and the
/// handle that can stop the turn it is currently awaiting.
#[derive(Clone)]
struct SessionHandle {
    commands: mpsc::Sender<SessionMsg>,
    cancel: CancelHandle,
    /// Where another connection subscribes when it attaches to this session.
    channels: SessionChannels,
    /// Last status the engine reported. Starts `Idle`; a tracker task keeps
    /// it current from `SessionEvent::StatusChanged`.
    status: SessionStatus,
}

/// The sessions one connection owns.
///
/// Ids are allocated here rather than derived from `len() + 1`: the old
/// scheme would hand out a duplicate id as soon as a close/removal path
/// exists, and a session id is the address every later request uses.
struct SessionRegistry {
    sessions: HashMap<SessionId, SessionHandle>,
    next_id: u64,
}

impl SessionRegistry {
    fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            next_id: 0,
        }
    }

    fn allocate_id(&mut self) -> SessionId {
        self.next_id += 1;
        SessionId(format!("sess_{}", self.next_id))
    }

    fn insert(
        &mut self,
        id: SessionId,
        commands: mpsc::Sender<SessionMsg>,
        cancel: CancelHandle,
        channels: SessionChannels,
    ) {
        self.sessions.insert(
            id,
            SessionHandle {
                commands,
                cancel,
                channels,
                status: SessionStatus::Idle,
            },
        );
    }

    fn get(&self, id: &SessionId) -> Option<&SessionHandle> {
        self.sessions.get(id)
    }

    /// Signal the live turn directly; the actor is awaiting it, so a queued
    /// message could not be read in time.
    fn cancel(&self, id: &SessionId) {
        if let Some(handle) = self.sessions.get(id) {
            handle.cancel.cancel();
        }
    }

    /// Every live session with its last reported status, sorted so a client's
    /// list is stable between calls.
    fn list(&self) -> Vec<(SessionId, SessionStatus)> {
        let mut entries: Vec<(SessionId, SessionStatus)> = self
            .sessions
            .iter()
            .map(|(id, handle)| (id.clone(), handle.status))
            .collect();
        entries.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        entries
    }

    fn set_status(&mut self, id: &SessionId, status: SessionStatus) {
        if let Some(handle) = self.sessions.get_mut(id) {
            handle.status = status;
        }
    }

    /// Remove a session and hand its handle to the caller. Dropping the handle
    /// closes the actor's command channel, which ends the actor once its
    /// current turn finishes.
    fn remove(&mut self, id: &SessionId) -> Option<SessionHandle> {
        self.sessions.remove(id)
    }
}

/// The session table shared by every connection a server accepts. A plain
/// stdio run gets its own private one; `serve` creates a single table and
/// hands clones to each connection.
type SharedSessions = Arc<tokio::sync::Mutex<SessionRegistry>>;

/// Keeps the registry's status for one session current. The engine's event hub
/// closes when the actor ends, which ends this task too.
fn spawn_status_tracker(
    sessions: SharedSessions,
    session_id: SessionId,
    mut events: mpsc::UnboundedReceiver<SessionEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let SessionEvent::StatusChanged { status, .. } = event {
                sessions.lock().await.set_status(&session_id, status);
            }
        }
    });
}

/// Owns everything the read loop needs across the life of the connection:
/// the active session registry, and the tables correlating a
/// `session/request_permission`, `session/checkInBrowser` or
/// `session/interactInBrowser` response back to the specific pending ask
/// that's waiting for it. Three separate tables, not one keyed by a shared
/// enum -- same reasoning as `headless.rs`'s own
/// `PermissionAsk`/`BrowserCheckAsk`/`BrowserInteractAsk` split: a small,
/// explicit type (and table) per real use.
struct Router {
    sessions: SharedSessions,
    /// ACP request id -> the engine-local id and handle that answer it. The
    /// engine waits on the handle, so the transport never owns a oneshot for
    /// a permission ask.
    pending_permission_responses: HashMap<RequestId, (u64, ApprovalHandle)>,
    /// Same shape as the permission table: the engine id and the cloneable
    /// answer handle for that ask.
    pending_browser_responses: HashMap<RequestId, (u64, AskHandle<BrowserCheckResult>)>,
    pending_browser_interact_responses: HashMap<RequestId, (u64, AskHandle<BrowserInteractResult>)>,
    next_outgoing_id: i64,
    outgoing_tx: mpsc::Sender<Outgoing>,
    permission_relay_in: mpsc::Receiver<PermissionAsk>,
    permission_relay_tx: mpsc::Sender<PermissionAsk>,
    browser_relay_in: mpsc::Receiver<BrowserCheckAsk>,
    browser_relay_tx: mpsc::Sender<BrowserCheckAsk>,
    browser_interact_relay_in: mpsc::Receiver<BrowserInteractAsk>,
    browser_interact_relay_tx: mpsc::Sender<BrowserInteractAsk>,
    /// The one field genuinely needed at runtime that a unit test doesn't
    /// have to fill in with anything meaningful -- see `run`'s own
    /// construction of the real `Config::load()` value.
    config: Config,
}

impl Router {
    fn new(config: Config, outgoing_tx: mpsc::Sender<Outgoing>) -> Self {
        Self::with_sessions(
            config,
            outgoing_tx,
            Arc::new(tokio::sync::Mutex::new(SessionRegistry::new())),
        )
    }

    fn with_sessions(
        config: Config,
        outgoing_tx: mpsc::Sender<Outgoing>,
        sessions: SharedSessions,
    ) -> Self {
        let (permission_relay_tx, permission_relay_in) = mpsc::channel(64);
        let (browser_relay_tx, browser_relay_in) = mpsc::channel(64);
        let (browser_interact_relay_tx, browser_interact_relay_in) = mpsc::channel(64);
        Self {
            sessions,
            pending_permission_responses: HashMap::new(),
            pending_browser_responses: HashMap::new(),
            pending_browser_interact_responses: HashMap::new(),
            next_outgoing_id: 1,
            outgoing_tx,
            permission_relay_in,
            permission_relay_tx,
            browser_relay_in,
            browser_relay_tx,
            browser_interact_relay_in,
            browser_interact_relay_tx,
            config,
        }
    }

    /// Handles one classified incoming line, returning zero or more lines
    /// to write back immediately (a response to a request, or nothing for
    /// a notification/response). `session/prompt` is the one request whose
    /// response never comes back through this return value -- see this
    /// module's own docs -- so it always yields `None` here even though
    /// it's a request.
    async fn handle(&mut self, incoming: Incoming) -> Option<String> {
        match incoming {
            Incoming::Request { id, method, params } if method == "session/prompt" => {
                self.dispatch_prompt_deferred(id, params).await;
                None
            }
            Incoming::Request { id, method, params } => {
                let result = self.dispatch_request(&method, params).await;
                let line = match result {
                    Ok(value) => serde_json::to_string(&RpcResponse {
                        jsonrpc: JsonRpcVersion,
                        id,
                        result: value,
                    }),
                    Err((code, message)) => serde_json::to_string(&RpcError {
                        jsonrpc: JsonRpcVersion,
                        id,
                        error: RpcErrorObject {
                            code,
                            message,
                            data: None,
                        },
                    }),
                };
                Some(format!("{}\n", line.expect("response always serializes")))
            }
            Incoming::Notification { method, params } => {
                if method == "session/cancel" {
                    // Signal the live turn directly. The actor is awaiting
                    // `prompt`, so a queued SessionMsg::Cancel would not be
                    // seen until the work it needs to stop had finished.
                    if let Ok(notification) =
                        serde_json::from_value::<crate::protocol::CancelNotification>(params)
                    {
                        self.sessions
                            .lock()
                            .await
                            .cancel(&notification.session_id);
                    }
                }
                None
            }
            Incoming::Response { id, result } => {
                if let Some((request_id, approvals)) = self.pending_permission_responses.remove(&id)
                {
                    if let Ok(outcome) = serde_json::from_value::<RequestPermissionOutcome>(result)
                    {
                        approvals.respond(request_id, outcome.into());
                    }
                } else if let Some((request_id, answers)) =
                    self.pending_browser_responses.remove(&id)
                {
                    if let Ok(outcome) = serde_json::from_value::<CheckInBrowserOutcome>(result) {
                        answers.respond(request_id, outcome.into());
                    }
                } else if let Some((request_id, answers)) =
                    self.pending_browser_interact_responses.remove(&id)
                {
                    if let Ok(outcome) = serde_json::from_value::<InteractInBrowserOutcome>(result)
                    {
                        answers.respond(request_id, outcome.into());
                    }
                }
                None
            }
            Incoming::Error { id, .. } => {
                if let Some((request_id, approvals)) = self.pending_permission_responses.remove(&id)
                {
                    approvals.respond(request_id, ApprovalResolution::Cancelled);
                } else if let Some((request_id, answers)) =
                    self.pending_browser_responses.remove(&id)
                {
                    answers.respond(
                        request_id,
                        BrowserCheckResult::Failed(
                            "the client returned an error instead of a result".to_string(),
                        ),
                    );
                } else if let Some((request_id, answers)) =
                    self.pending_browser_interact_responses.remove(&id)
                {
                    answers.respond(
                        request_id,
                        BrowserInteractResult::Failed(
                            "the client returned an error instead of a result".to_string(),
                        ),
                    );
                }
                None
            }
            Incoming::Malformed => None,
        }
    }

    async fn dispatch_request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, (i64, String)> {
        match method {
            "initialize" => {
                let _req: InitializeRequest = serde_json::from_value(params)
                    .map_err(|e| (-32602, format!("invalid params: {e}")))?;
                let images = crate::providers::supports_images(
                    &self.config.llm.provider,
                    &self.config.llm.model,
                );
                let resp = InitializeResponse {
                    protocol_version: PROTOCOL_VERSION,
                    // `promptCapabilities.image` is the opt-in
                    // `ContentBlock::Image` requires per the ACP v1 schema
                    // (protocol.rs's own doc comment on that variant). It
                    // follows the configured provider and model: advertising it
                    // for a text-only model invites a client to send
                    // screenshots the endpoint will reject or garble.
                    // `mcp` is not part of the ACP v1 schema, which is exactly
                    // why it has to be advertised explicitly: the schema makes
                    // `mcpServers` a required field on `session/new`, so a build
                    // that does not understand it still accepts the field and
                    // returns a session id while connecting nothing. A client
                    // cannot tell that apart from real support. This capability
                    // is the positive signal to gate on -- absent it, a client
                    // should refuse to send servers rather than report success.
                    agent_capabilities: json!({ "session": {}, "mcp": true, "promptCapabilities": { "image": images } }),
                    auth_methods: Vec::new(),
                    agent_info: Some(Implementation {
                        name: "covexcode".to_string(),
                        title: Some("covexcode".to_string()),
                        version: crate::VERSION.to_string(),
                    }),
                };
                Ok(serde_json::to_value(resp).expect("serializes"))
            }
            "session/new" => {
                let req: NewSessionRequest = serde_json::from_value(params)
                    .map_err(|e| (-32602, format!("invalid params: {e}")))?;
                let workspace = Workspace::new(&req.cwd).map_err(|e| (-32000, e))?;

                // Validate the requested MCP servers before creating a session.
                // Doing this synchronously is deliberate: a malformed server
                // config is a caller error and must come back as an error on
                // session/new, not as a session that silently connects nothing.
                // Connecting them is a separate, asynchronous step -- it spawns
                // processes and can take seconds, which this handler must not do
                // inline (see the session/prompt comment below on why blocking
                // here is a deadlock risk).
                let mcp_configs = crate::mcp::parse_server_configs(&req.mcp_servers)
                    .map_err(|e| (-32602, format!("invalid mcpServers: {e}")))?;

                let session_id = self.sessions.lock().await.allocate_id();
                let channels = SessionChannels::new(session_id.clone());
                // The creating connection is the first subscriber to every
                // hub; attach adds another.
                channels
                    .updates
                    .subscribe(self.outgoing_tx.clone())
                    .await;
                channels
                    .approvals
                    .subscribe(self.permission_relay_tx.clone())
                    .await;
                channels
                    .browser_checks
                    .subscribe(self.browser_relay_tx.clone())
                    .await;
                channels
                    .browser_interactions
                    .subscribe(self.browser_interact_relay_tx.clone())
                    .await;
                let session =
                    HeadlessSession::new(session_id.clone(), workspace, self.config.clone());
                let events = session.subscribe_events();
                let cancel = session.cancel_handle();
                let handle = SessionActor::spawn(session, channels.clone(), mcp_configs.clone());
                self.sessions
                    .lock()
                    .await
                    .insert(session_id.clone(), handle, cancel, channels);
                spawn_status_tracker(self.sessions.clone(), session_id.clone(), events);

                // Configs are validated here; the handshake itself happens in
                // SessionActor::spawn, which calls session.connect_mcp() off the
                // request path. That is deliberate: connecting takes seconds, so
                // session/new must reply before the servers are up.
                if !mcp_configs.is_empty() {
                    tracing::info!(
                        target: "mcp",
                        session = %session_id.0,
                        count = mcp_configs.len(),
                        "mcp servers validated; connecting off the request path"
                    );
                }

                Ok(serde_json::to_value(NewSessionResponse { session_id }).expect("serializes"))
            }
            // Unlike session/prompt, this can be handled synchronously right
            // here: rollback::apply is local disk I/O, never an LLM round
            // trip, so there's no risk of the deadlock class that makes
            // session/prompt's own response need deferring (see that
            // method's own doc comment).
            //
            // Custom (not ACP v1): every live session on this server.
            "session/list" => {
                let entries = self.sessions.lock().await.list();
                let sessions: Vec<serde_json::Value> = entries
                    .into_iter()
                    .map(|(id, status)| serde_json::json!({ "sessionId": id, "status": status }))
                    .collect();
                Ok(serde_json::json!({ "sessions": sessions }))
            }
            // Custom: stop the turn and end the session's actor. Dropping the
            // handle closes the last command sender, which ends the actor once
            // its current turn unwinds.
            "session/close" => {
                let req: SessionIdRequest = serde_json::from_value(params)
                    .map_err(|e| (-32602, format!("invalid params: {e}")))?;
                let removed = self.sessions.lock().await.remove(&req.session_id);
                let Some(handle) = removed else {
                    return Err((-32001, "no such session".to_string()));
                };
                handle.cancel.cancel();
                drop(handle);
                Ok(serde_json::json!({}))
            }
            // Custom (not ACP v1): attach this connection to a session that
            // another connection owns. It starts receiving that session's
            // updates immediately; history is not replayed.
            "session/attach" => {
                let req: SessionIdRequest = serde_json::from_value(params)
                    .map_err(|e| (-32602, format!("invalid params: {e}")))?;
                let channels = {
                    let sessions = self.sessions.lock().await;
                    sessions
                        .get(&req.session_id)
                        .map(|handle| handle.channels.clone())
                };
                let Some(channels) = channels else {
                    return Err((-32001, "no such session".to_string()));
                };
                channels
                    .updates
                    .subscribe(self.outgoing_tx.clone())
                    .await;
                channels
                    .approvals
                    .subscribe(self.permission_relay_tx.clone())
                    .await;
                channels
                    .browser_checks
                    .subscribe(self.browser_relay_tx.clone())
                    .await;
                channels
                    .browser_interactions
                    .subscribe(self.browser_interact_relay_tx.clone())
                    .await;
                Ok(serde_json::json!({}))
            }
            "session/rollback" => {
                let req: RollbackRequest = serde_json::from_value(params)
                    .map_err(|e| (-32602, format!("invalid params: {e}")))?;
                let handle = self
                    .sessions
                    .lock()
                    .await
                    .get(&req.session_id)
                    .ok_or_else(|| (-32001, "no such session".to_string()))?
                    .commands
                    .clone();
                let (respond, receive) = oneshot::channel();
                handle
                    .send(SessionMsg::Rollback { req, respond })
                    .await
                    .map_err(|_| (-32002, "session actor gone".to_string()))?;
                let response = receive
                    .await
                    .map_err(|_| (-32002, "session actor gone".to_string()))?;
                Ok(serde_json::to_value(response).expect("serializes"))
            }
            // Same synchronous, local-read shape as session/rollback: a peek
            // at in-memory journal state, never an LLM round trip, so it can
            // be answered right here rather than deferred like session/prompt.
            "session/list_changes" => {
                let req: ListChangesRequest = serde_json::from_value(params)
                    .map_err(|e| (-32602, format!("invalid params: {e}")))?;
                let handle = self
                    .sessions
                    .lock()
                    .await
                    .get(&req.session_id)
                    .ok_or_else(|| (-32001, "no such session".to_string()))?
                    .commands
                    .clone();
                let (respond, receive) = oneshot::channel();
                handle
                    .send(SessionMsg::ListChanges { respond })
                    .await
                    .map_err(|_| (-32002, "session actor gone".to_string()))?;
                let response = receive
                    .await
                    .map_err(|_| (-32002, "session actor gone".to_string()))?;
                Ok(serde_json::to_value(response).expect("serializes"))
            }
            other => Err((-32601, format!("method not found: {other}"))),
        }
    }

    /// `session/prompt`'s response can only be known once the whole turn
    /// finishes -- including, potentially, a full
    /// `session/request_permission` round trip back through this same
    /// connection. Awaiting that inline here (the way every other request
    /// is handled above) would block the read loop from ever reaching the
    /// code that sends that permission request or reads its answer. So
    /// this spawns its own task to wait for the result and report it
    /// through `outgoing_tx` -- the same channel [`SessionActor`]'s update
    /// drain already uses -- instead of returning it from `handle`.
    async fn dispatch_prompt_deferred(&mut self, id: RequestId, params: serde_json::Value) {
        let outgoing_tx = self.outgoing_tx.clone();
        let error_line = |id: RequestId, code: i64, message: String| {
            format!(
                "{}\n",
                serde_json::to_string(&RpcError {
                    jsonrpc: JsonRpcVersion,
                    id,
                    error: RpcErrorObject {
                        code,
                        message,
                        data: None
                    },
                })
                .expect("error response always serializes")
            )
        };

        let req: PromptRequest = match serde_json::from_value(params) {
            Ok(req) => req,
            Err(e) => {
                let line = error_line(id, -32602, format!("invalid params: {e}"));
                let _ = outgoing_tx.send(Outgoing::Line(line)).await;
                return;
            }
        };
        let Some(commands) = self
            .sessions
            .lock()
            .await
            .get(&req.session_id)
            .map(|handle| handle.commands.clone())
        else {
            let line = error_line(id, -32001, "no such session".to_string());
            let _ = outgoing_tx.send(Outgoing::Line(line)).await;
            return;
        };
        let mut text_parts = Vec::new();
        let mut images = Vec::new();
        for block in req.prompt {
            match block {
                crate::protocol::ContentBlock::Text { text } => text_parts.push(text),
                crate::protocol::ContentBlock::Image { data, mime_type } => {
                    images.push(crate::llm::ImageAttachment {
                        mime_type,
                        data_base64: data,
                    });
                }
            }
        }
        let text = text_parts.join("\n");

        let (respond, receive) = oneshot::channel();
        if commands
            .send(SessionMsg::Prompt {
                text,
                images,
                mode: req.mode,
                respond,
            })
            .await
            .is_err()
        {
            let line = error_line(id, -32002, "session actor gone".to_string());
            let _ = outgoing_tx.send(Outgoing::Line(line)).await;
            return;
        }

        tokio::spawn(async move {
            let line = match receive.await {
                Ok(response) => format!(
                    "{}\n",
                    serde_json::to_string(&RpcResponse {
                        jsonrpc: JsonRpcVersion,
                        id,
                        result: serde_json::to_value(response).expect("serializes"),
                    })
                    .expect("response always serializes")
                ),
                Err(_) => error_line(id, -32002, "session actor gone".to_string()),
            };
            let _ = outgoing_tx.send(Outgoing::Line(line)).await;
        });
    }

    /// Pulls the next queued ask from *any* of the three relays -- a
    /// [`PermissionAsk`], a [`BrowserCheckAsk`] or a [`BrowserInteractAsk`],
    /// whichever arrives first -- assigns it a fresh outgoing id from one
    /// shared counter (a single id space for everything this process sends,
    /// not three that could collide), registers where its eventual response
    /// should go, and returns the line to write to stdout. `None` only once
    /// *all three* relays have closed (every session finished); one relay
    /// closing while the others are still live keeps this selecting on the
    /// live ones.
    ///
    /// One method rather than three separate ones each taking `&mut self`:
    /// `run`'s own `select!` cannot hold three simultaneous mutable borrows
    /// of the same `Router`, but a single `tokio::select!` *inside* this
    /// method can borrow `self.permission_relay_in`, `self.browser_relay_in`
    /// and `self.browser_interact_relay_in` disjointly, since the borrow
    /// checker sees those as three different fields, not three calls each
    /// asking for the whole `&mut self`.
    async fn next_outgoing_client_request(&mut self) -> Option<String> {
        enum Ready {
            Permission(PermissionAsk),
            Browser(BrowserCheckAsk),
            BrowserInteract(BrowserInteractAsk),
        }
        let ready = tokio::select! {
            ask = self.permission_relay_in.recv() => ask.map(Ready::Permission),
            ask = self.browser_relay_in.recv() => ask.map(Ready::Browser),
            ask = self.browser_interact_relay_in.recv() => ask.map(Ready::BrowserInteract),
        }?;
        let id = RequestId::Number(self.next_outgoing_id);
        self.next_outgoing_id += 1;
        let request = match ready {
            Ready::Permission(ask) => {
                self.pending_permission_responses
                    .insert(id.clone(), (ask.request_id, ask.approvals));
                RpcRequest {
                    jsonrpc: JsonRpcVersion,
                    id,
                    method: "session/request_permission".to_string(),
                    params: Some(serde_json::to_value(ask.request).expect("serializes")),
                }
            }
            Ready::Browser(ask) => {
                self.pending_browser_responses
                    .insert(id.clone(), (ask.request_id, ask.answers));
                RpcRequest {
                    jsonrpc: JsonRpcVersion,
                    id,
                    method: "session/checkInBrowser".to_string(),
                    params: Some(
                        serde_json::to_value(CheckInBrowserRequest {
                            session_id: ask.session_id,
                            url: ask.url,
                        })
                        .expect("serializes"),
                    ),
                }
            }
            Ready::BrowserInteract(ask) => {
                self.pending_browser_interact_responses
                    .insert(id.clone(), (ask.request_id, ask.answers));
                RpcRequest {
                    jsonrpc: JsonRpcVersion,
                    id,
                    method: "session/interactInBrowser".to_string(),
                    params: Some(
                        serde_json::to_value(InteractInBrowserRequest {
                            session_id: ask.session_id,
                            url: ask.url,
                            interaction: ask.interaction,
                        })
                        .expect("serializes"),
                    ),
                }
            }
        };
        Some(request.to_ndjson_line())
    }
}

/// The real entry point -- reads stdin, writes stdout, runs until stdin
/// closes. Selected by `main.rs` via `covexcode --acp`, mutually exclusive
/// with the normal TUI (this never touches a terminal, only pipes).
pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    run_stream(config, tokio::io::stdin(), tokio::io::stdout()).await
}

/// The ACP loop over any byte stream. `run` is the stdio instantiation;
/// a TCP daemon or an in-process test can drive the identical protocol
/// without touching stdin/stdout.
pub async fn run_stream<R, W>(
    config: Config,
    reader: R,
    writer: W,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let sessions = Arc::new(tokio::sync::Mutex::new(SessionRegistry::new()));
    run_stream_with_sessions(config, reader, writer, sessions).await
}

/// The same loop with a session table supplied by the caller. `serve` uses
/// this so every accepted connection sees the same sessions.
async fn run_stream_with_sessions<R, W>(
    config: Config,
    reader: R,
    writer: W,
    sessions: SharedSessions,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    let mut writer = writer;

    let (outgoing_tx, mut outgoing_in) = mpsc::channel::<Outgoing>(256);
    let mut router = Router::with_sessions(config, outgoing_tx, sessions);

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { break }; // stream closed
                if line.trim().is_empty() {
                    continue;
                }
                let value: serde_json::Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue, // malformed line, not this process's job to crash over
                };
                if let Some(out) = router.handle(classify(&value)).await {
                    writer.write_all(out.as_bytes()).await?;
                    writer.flush().await?;
                }
            }
            Some(outgoing) = outgoing_in.recv() => {
                let line = match outgoing {
                    Outgoing::Update(notification) => crate::protocol::RpcNotification {
                        jsonrpc: JsonRpcVersion,
                        method: "session/update".to_string(),
                        params: Some(serde_json::to_value(&notification).expect("serializes")),
                    }
                    .to_ndjson_line(),
                    Outgoing::Line(line) => line,
                };
                writer.write_all(line.as_bytes()).await?;
                writer.flush().await?;
            }
            Some(out) = router.next_outgoing_client_request() => {
                writer.write_all(out.as_bytes()).await?;
                writer.flush().await?;
            }
        }
    }
    Ok(())
}

/// Serve ACP over TCP on `addr`, one [`Router`] per connection but one shared
/// session table, so a connection can attach to a session created on another.
/// Refuses a non-loopback bind: this process can run shell commands and there
/// is no transport authentication yet, so it must not be reachable from the
/// network.
pub async fn serve(config: Config, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    if !local.ip().is_loopback() {
        return Err(format!(
            "refusing to listen on {local}: the agent can run commands and this build has no \
             transport authentication. Bind a loopback address instead."
        )
        .into());
    }
    eprintln!("covexcode-agent: listening on {local}");
    let sessions: SharedSessions = Arc::new(tokio::sync::Mutex::new(SessionRegistry::new()));
    loop {
        let (stream, _) = listener.accept().await?;
        let config = config.clone();
        let sessions = sessions.clone();
        tokio::spawn(async move {
            let (reader, writer) = stream.into_split();
            if let Err(error) = run_stream_with_sessions(config, reader, writer, sessions).await {
                eprintln!("covexcode-agent: connection ended: {error}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_request_needs_both_id_and_method() {
        let value = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} });
        assert_eq!(
            classify(&value),
            Incoming::Request {
                id: RequestId::Number(1),
                method: "initialize".to_string(),
                params: json!({})
            }
        );
    }

    #[test]
    fn a_notification_has_method_but_no_id() {
        let value = json!({ "jsonrpc": "2.0", "method": "session/cancel", "params": { "sessionId": "s1" } });
        assert_eq!(
            classify(&value),
            Incoming::Notification {
                method: "session/cancel".to_string(),
                params: json!({ "sessionId": "s1" })
            }
        );
    }

    #[test]
    fn a_response_to_our_own_request_has_id_and_result_but_no_method() {
        let value = json!({ "jsonrpc": "2.0", "id": 7, "result": { "outcome": "cancelled" } });
        assert_eq!(
            classify(&value),
            Incoming::Response {
                id: RequestId::Number(7),
                result: json!({ "outcome": "cancelled" })
            }
        );
    }

    #[test]
    fn an_error_response_is_distinguished_from_a_result() {
        let value =
            json!({ "jsonrpc": "2.0", "id": 7, "error": { "code": -32000, "message": "boom" } });
        assert_eq!(
            classify(&value),
            Incoming::Error {
                id: RequestId::Number(7),
                error: json!({ "code": -32000, "message": "boom" })
            }
        );
    }

    #[test]
    fn a_message_with_neither_id_nor_method_is_malformed_not_guessed_at() {
        assert_eq!(classify(&json!({ "jsonrpc": "2.0" })), Incoming::Malformed);
    }

    #[tokio::test]
    async fn initialize_advertises_image_only_for_a_vision_provider() {
        // Real regression coverage: a spec-compliant client is only
        // permitted to send ContentBlock::Image if this advertises support
        // for it -- see ContentBlock::Image's own doc comment on why this
        // is required, not optional polish. It must follow the provider and
        // model, or a text-only model is handed screenshots it cannot use.
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
        let mut text_only = Config::default();
        text_only.llm.provider = "deepseek".to_string();
        let mut text_router = Router::new(text_only, outgoing_tx.clone());
        let response = text_router
            .dispatch_request("initialize", json!({ "protocolVersion": 1 }))
            .await
            .expect("initialize always succeeds");
        assert_eq!(response["agentCapabilities"]["promptCapabilities"]["image"], json!(false));

        let mut proxy = Config::default();
        proxy.llm.provider = String::new();
        proxy.llm.model = "deepseek-v4-pro".to_string();
        let mut proxy_router = Router::new(proxy, outgoing_tx.clone());
        let response = proxy_router
            .dispatch_request("initialize", json!({ "protocolVersion": 1 }))
            .await
            .expect("initialize always succeeds");
        assert_eq!(
            response["agentCapabilities"]["promptCapabilities"]["image"],
            json!(false),
            "a proxy login has no provider id; v4-pro is still text-only"
        );

        let mut proxy_flash = Config::default();
        proxy_flash.llm.provider = String::new();
        proxy_flash.llm.model = "deepseek-v4-flash".to_string();
        let mut proxy_flash_router = Router::new(proxy_flash, outgoing_tx.clone());
        let response = proxy_flash_router
            .dispatch_request("initialize", json!({ "protocolVersion": 1 }))
            .await
            .expect("initialize always succeeds");
        assert_eq!(
            response["agentCapabilities"]["promptCapabilities"]["image"],
            json!(true),
            "the model family grants vision through the proxy"
        );

        let mut vision = Config::default();
        vision.llm.provider = "openai".to_string();
        let mut vision_router = Router::new(vision, outgoing_tx);
        let response = vision_router
            .dispatch_request("initialize", json!({ "protocolVersion": 1 }))
            .await
            .expect("initialize always succeeds");
        assert_eq!(
            response["agentCapabilities"]["promptCapabilities"]["image"],
            json!(true)
        );
    }

    /// The actual end-to-end proof: initialize, open a session, send a
    /// prompt that triggers a permission request, answer it, and see the
    /// turn actually complete -- through `Router`, not just through
    /// `HeadlessSession` directly (that's already covered in
    /// `headless.rs`'s own tests; this test is specifically about the
    /// routing/id-correlation logic this module adds on top, and about
    /// `session/prompt` genuinely not blocking the router the way this
    /// module's own docs say it must not).
    #[tokio::test]
    async fn a_full_round_trip_through_the_router_completes_a_turn() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let dir = tempfile::tempdir().expect("temp dir");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                 data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\
                 \"type\":\"function\",\"function\":{\"name\":\"run_command\",\"arguments\":\
                 \"{\\\"command\\\":\\\"rm -rf build\\\"}\"}}]}}]}\n\ndata: [DONE]\n\n",
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                 data: {\"choices\":[{\"delta\":{\"content\":\"Understood.\"}}]}\n\ndata: [DONE]\n\n",
            ] {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let mut config = Config::default();
        config.llm.endpoint = format!("http://{addr}");
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();

        let (outgoing_tx, mut outgoing_in) = mpsc::channel(64);
        let mut router = Router::new(config, outgoing_tx);

        let init = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": 1 }
            })))
            .await;
        assert!(init.unwrap().contains("\"protocolVersion\":1"));

        let new_session = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "session/new",
                "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
            })))
            .await
            .unwrap();
        assert!(new_session.contains("sess_1"));

        // session/prompt must return None immediately -- its own response
        // is deferred, not synchronous -- exactly the property that keeps
        // the router free to handle the permission round trip below on the
        // very same task, with no extra `tokio::spawn` needed on the test's
        // side either. That itself is proof the deadlock this module's own
        // docs describe doesn't happen: if `handle` blocked here instead,
        // this call would never return and the test would time out.
        let prompt_ack = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/prompt",
                "params": { "sessionId": "sess_1", "prompt": [{ "type": "text", "text": "clean the build dir" }] }
            })))
            .await;
        assert_eq!(prompt_ack, None);

        // Drain the permission request the dangerous `rm -rf build` call
        // produces, and answer it as a rejection.
        let permission_line = router
            .next_outgoing_client_request()
            .await
            .expect("the dangerous command asks permission");
        let permission_value: serde_json::Value =
            serde_json::from_str(permission_line.trim()).expect("valid JSON line");
        assert_eq!(permission_value["method"], "session/request_permission");
        let request_id = permission_value["id"].as_i64().expect("numeric id");
        let reject_option = permission_value["params"]["options"]
            .as_array()
            .expect("options array")
            .iter()
            .find(|opt| opt["optionId"] == "reject")
            .expect("a reject option is offered")["optionId"]
            .as_str()
            .expect("optionId is a string")
            .to_string();

        let response_ack = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": request_id,
                "result": { "outcome": "selected", "optionId": reject_option }
            })))
            .await;
        assert_eq!(
            response_ack, None,
            "a response to our own request never produces a line"
        );

        // Now the deferred session/prompt response arrives on the outgoing
        // channel, proving the whole turn -- including the permission round
        // trip that just ran through this same Router -- actually finished.
        // It's not necessarily the very next thing on the channel: ordinary
        // `session/update` notifications (the tool call's pending/failed
        // status, the model's follow-up message chunk) legitimately
        // interleave ahead of it, same as a real client would see.
        let final_line = loop {
            match outgoing_in
                .recv()
                .await
                .expect("the deferred response arrives")
            {
                Outgoing::Line(line) => break line,
                Outgoing::Update(_) => continue,
            }
        };
        let final_value: serde_json::Value =
            serde_json::from_str(final_line.trim()).expect("valid JSON line");
        assert_eq!(final_value["id"], 3);
        assert_eq!(final_value["result"]["stopReason"], "end_turn");
        assert_eq!(final_value["result"]["turn"], 1);
    }

    /// `session/cancel` must reach the session that owns the live turn, not
    /// just be accepted by the router. The actor is awaiting the prompt, so a
    /// queued message could never be read; the router signals the engine's
    /// cancel handle directly. The server reports when the request is
    /// actually in flight, so this cannot race the prompt's start.
    #[tokio::test]
    async fn session_cancel_stops_a_prompt_through_the_router() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let dir = tempfile::tempdir().expect("temp dir");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let (in_flight_tx, in_flight_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let _ = in_flight_tx.send(());
            // Hold the response past the test's timeout: only cancellation
            // can finish this turn in time.
            tokio::time::sleep(Duration::from_secs(30)).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                      data: {\"choices\":[{\"delta\":{\"content\":\"too late\"}}]}\n\ndata: [DONE]\n\n",
                )
                .await;
            let _ = socket.shutdown().await;
        });

        let mut config = Config::default();
        config.llm.endpoint = format!("http://{addr}");
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();

        let (outgoing_tx, mut outgoing_in) = mpsc::channel(64);
        let mut router = Router::new(config, outgoing_tx);

        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": 1 }
            })))
            .await;
        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "session/new",
                "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
            })))
            .await;
        let prompt_ack = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/prompt",
                "params": { "sessionId": "sess_1", "prompt": [{ "type": "text", "text": "hello" }] }
            })))
            .await;
        assert_eq!(prompt_ack, None);

        tokio::time::timeout(Duration::from_secs(5), in_flight_rx)
            .await
            .expect("the model request must reach the endpoint")
            .expect("the server signals when the request is in flight");

        let cancel_ack = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "method": "session/cancel",
                "params": { "sessionId": "sess_1" }
            })))
            .await;
        assert_eq!(cancel_ack, None, "a notification never produces a line");

        let final_line = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match outgoing_in
                    .recv()
                    .await
                    .expect("the deferred response arrives")
                {
                    Outgoing::Line(line) => break line,
                    Outgoing::Update(_) => continue,
                }
            }
        })
        .await
        .expect("cancel must end the turn promptly");
        let final_value: serde_json::Value =
            serde_json::from_str(final_line.trim()).expect("valid JSON line");
        assert_eq!(final_value["id"], 3);
        assert_eq!(final_value["result"]["stopReason"], "cancelled");
    }

    /// The other half of `next_outgoing_client_request`: proves the merged
    /// select actually reaches the browser relay too, not just the
    /// permission one it shares a method with -- and that the two relays
    /// don't interfere (the browser ask goes out with its own real method
    /// name and shape, is answered on the same connection, and the turn
    /// completes normally with no permission round trip at all, since
    /// `check_in_browser` is auto-approved).
    #[tokio::test]
    async fn a_check_in_browser_call_round_trips_through_the_router() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let dir = tempfile::tempdir().expect("temp dir");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                 data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\
                 \"type\":\"function\",\"function\":{\"name\":\"check_in_browser\",\"arguments\":\
                 \"{\\\"url\\\":\\\"http://localhost:3000\\\"}\"}}]}}]}\n\ndata: [DONE]\n\n",
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                 data: {\"choices\":[{\"delta\":{\"content\":\"Looks right.\"}}]}\n\ndata: [DONE]\n\n",
            ] {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let mut config = Config::default();
        config.llm.endpoint = format!("http://{addr}");
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();

        let (outgoing_tx, mut outgoing_in) = mpsc::channel(64);
        let mut router = Router::new(config, outgoing_tx);

        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": 1 }
            })))
            .await;
        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "session/new",
                "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
            })))
            .await;

        let prompt_ack = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/prompt",
                "params": { "sessionId": "sess_1", "prompt": [{ "type": "text", "text": "does it render?" }] }
            })))
            .await;
        assert_eq!(prompt_ack, None);

        let browser_line = router
            .next_outgoing_client_request()
            .await
            .expect("check_in_browser goes out as a real request");
        let browser_value: serde_json::Value =
            serde_json::from_str(browser_line.trim()).expect("valid JSON line");
        assert_eq!(browser_value["method"], "session/checkInBrowser");
        assert_eq!(browser_value["params"]["sessionId"], "sess_1");
        assert_eq!(browser_value["params"]["url"], "http://localhost:3000");
        let request_id = browser_value["id"].as_i64().expect("numeric id");

        let response_ack = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": request_id,
                "result": { "outcome": "screenshot", "mimeType": "image/png", "data": "aGVsbG8=" }
            })))
            .await;
        assert_eq!(
            response_ack, None,
            "a response to our own request never produces a line"
        );

        let final_line = loop {
            match outgoing_in
                .recv()
                .await
                .expect("the deferred response arrives")
            {
                Outgoing::Line(line) => break line,
                Outgoing::Update(_) => continue,
            }
        };
        let final_value: serde_json::Value =
            serde_json::from_str(final_line.trim()).expect("valid JSON line");
        assert_eq!(final_value["id"], 3);
        assert_eq!(final_value["result"]["stopReason"], "end_turn");
        assert_eq!(final_value["result"]["turn"], 1);
    }

    /// `session/rollback` is a plain, synchronous request -- unlike
    /// `session/prompt` and the two agent-initiated methods above, it never
    /// needs `next_outgoing_client_request` at all, since `router.handle`
    /// can return its result directly the same way `session/new` does.
    /// Proves the whole path end to end: a real write happens through a
    /// full `session/prompt` turn, then `session/rollback` actually puts
    /// the file back, through the router, not just through
    /// `HeadlessSession` directly.
    #[tokio::test]
    async fn a_rollback_request_round_trips_through_the_router() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("hello.txt"), "hi\n").expect("seed file");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                 data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\
                 \"type\":\"function\",\"function\":{\"name\":\"write_file\",\"arguments\":\
                 \"{\\\"path\\\":\\\"hello.txt\\\",\\\"content\\\":\\\"bye\\\\n\\\"}\"}}]}}]}\n\ndata: [DONE]\n\n",
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                 data: {\"choices\":[{\"delta\":{\"content\":\"Updated it.\"}}]}\n\ndata: [DONE]\n\n",
            ] {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let mut config = Config::default();
        config.llm.endpoint = format!("http://{addr}");
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();

        let (outgoing_tx, mut outgoing_in) = mpsc::channel(64);
        let mut router = Router::new(config, outgoing_tx);

        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": 1 }
            })))
            .await;
        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "session/new",
                "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
            })))
            .await;
        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/prompt",
                "params": { "sessionId": "sess_1", "prompt": [{ "type": "text", "text": "say hi differently" }] }
            })))
            .await;

        // Wait for the write to actually finish -- session/prompt's own
        // response is deferred, so the write is only guaranteed done once
        // that deferred response arrives.
        loop {
            match outgoing_in
                .recv()
                .await
                .expect("the deferred prompt response arrives")
            {
                Outgoing::Line(_) => break,
                Outgoing::Update(_) => continue,
            }
        }
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "bye\n"
        );

        let rollback_line = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 4, "method": "session/rollback",
                "params": { "sessionId": "sess_1" }
            })))
            .await
            .expect("session/rollback answers synchronously, unlike session/prompt");
        let rollback_value: serde_json::Value =
            serde_json::from_str(rollback_line.trim()).expect("valid JSON line");
        assert_eq!(rollback_value["id"], 4);
        assert!(
            rollback_value["result"]["summary"]
                .as_str()
                .unwrap()
                .contains("Rolled back 1 file"),
            "{rollback_value}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "hi\n"
        );
    }

    /// The listen-mode seam in one test: the same ACP loop runs over any
    /// async byte stream, not just stdin/stdout. A daemon connection and a
    /// real client are exactly this shape.
    #[tokio::test]
    async fn run_stream_serves_acp_over_any_byte_stream() {
        use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (client, server) = duplex(1 << 16);
        let (server_read, server_write) = tokio::io::split(server);
        let dir = tempfile::tempdir().expect("temp dir");
        let config = Config::default();
        // `Box<dyn Error>` is not `Send`, so map it to a string before the
        // result crosses the task boundary.
        let server = tokio::spawn(async move {
            run_stream(config, server_read, server_write)
                .await
                .map_err(|error| error.to_string())
        });

        let (client_read, mut client_write) = tokio::io::split(client);
        let mut lines = BufReader::new(client_read).lines();
        client_write
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "jsonrpc": "2.0", "id": 1, "method": "initialize",
                        "params": { "protocolVersion": 1 }
                    })
                )
                .as_bytes(),
            )
            .await
            .expect("write initialize");
        let response = lines.next_line().await.expect("read").expect("a line");
        let value: serde_json::Value = serde_json::from_str(&response).expect("valid JSON");
        assert_eq!(value["id"], 1);
        assert_eq!(value["result"]["protocolVersion"], 1);

        client_write
            .write_all(
                format!(
                    "{}\n",
                    json!({
                        "jsonrpc": "2.0", "id": 2, "method": "session/new",
                        "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
                    })
                )
                .as_bytes(),
            )
            .await
            .expect("write session/new");
        let response = lines.next_line().await.expect("read").expect("a line");
        let value: serde_json::Value = serde_json::from_str(&response).expect("valid JSON");
        assert_eq!(value["id"], 2);
        assert_eq!(value["result"]["sessionId"], "sess_1");

        // `tokio::io::split` keeps the underlying stream alive until *both*
        // halves drop, so dropping the write half alone would leave the
        // server's `next_line` waiting forever. Shut the write direction down
        // explicitly so it observes EOF.
        client_write
            .shutdown()
            .await
            .expect("shut the client write half");
        drop(client_write);
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("the server must observe EOF promptly")
            .expect("server task")
            .expect("the stream loop ends cleanly on EOF");
    }

    /// The shared-session milestone: a session created on one connection can
    /// be attached from another, and the second connection receives its live
    /// updates.
    #[tokio::test]
    async fn a_second_connection_can_attach_and_see_updates() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n\
                      data: {\"choices\":[{\"delta\":{\"content\":\"hello from A\"}}]}\n\ndata: [DONE]\n\n",
                )
                .await;
            let _ = socket.shutdown().await;
        });

        let mut config = Config::default();
        config.llm.endpoint = format!("http://{addr}");
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();
        let dir = tempfile::tempdir().expect("temp dir");

        let sessions: SharedSessions = Arc::new(tokio::sync::Mutex::new(SessionRegistry::new()));
        let (out_a, _in_a) = mpsc::channel(64);
        let mut a = Router::with_sessions(config.clone(), out_a, sessions.clone());
        let (out_b, mut in_b) = mpsc::channel(64);
        let mut b = Router::with_sessions(config, out_b, sessions);

        a.handle(classify(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": 1 }
        })))
        .await;
        let created = a
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "session/new",
                "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
            })))
            .await
            .expect("session/new answers");
        assert!(created.contains("sess_1"), "{created}");

        let attached = b
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/attach",
                "params": { "sessionId": "sess_1" }
            })))
            .await
            .expect("attach answers synchronously");
        assert!(attached.contains("\"result\""), "{attached}");

        let prompt_ack = a
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 4, "method": "session/prompt",
                "params": { "sessionId": "sess_1", "prompt": [{ "type": "text", "text": "hi" }] }
            })))
            .await;
        assert_eq!(prompt_ack, None);

        let notification = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match in_b.recv().await.expect("the attached connection stays open") {
                    Outgoing::Update(notification) => break notification,
                    Outgoing::Line(_) => continue,
                }
            }
        })
        .await
        .expect("the attached connection must receive live updates");
        let value = serde_json::to_value(&notification).expect("serializes");
        assert_eq!(value["sessionId"], "sess_1");
    }

    /// Custom lifecycle methods: a client can discover live sessions and end
    /// one. Closing drops the last command sender, so a later attach/close
    /// reports "no such session".
    #[tokio::test]
    async fn sessions_can_be_listed_and_closed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (outgoing_tx, _outgoing_in) = mpsc::channel(64);
        let mut router = Router::new(Config::default(), outgoing_tx);

        router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": { "protocolVersion": 1 }
            })))
            .await;
        let created = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "session/new",
                "params": { "cwd": dir.path().display().to_string(), "mcpServers": [] }
            })))
            .await
            .expect("session/new answers");
        assert!(created.contains("sess_1"), "{created}");

        let listed = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "session/list", "params": {}
            })))
            .await
            .expect("session/list answers");
        let value: serde_json::Value = serde_json::from_str(listed.trim()).unwrap();
        assert_eq!(value["result"]["sessions"][0]["sessionId"], "sess_1");
        assert_eq!(value["result"]["sessions"][0]["status"], "idle");

        let closed = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 4, "method": "session/close",
                "params": { "sessionId": "sess_1" }
            })))
            .await
            .expect("session/close answers");
        assert!(closed.contains("\"result\""), "{closed}");

        let listed = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 5, "method": "session/list", "params": {}
            })))
            .await
            .expect("session/list answers");
        let value: serde_json::Value = serde_json::from_str(listed.trim()).unwrap();
        assert_eq!(
            value["result"]["sessions"].as_array().unwrap().len(),
            0,
            "the closed session is gone"
        );

        let attach = router
            .handle(classify(&json!({
                "jsonrpc": "2.0", "id": 6, "method": "session/attach",
                "params": { "sessionId": "sess_1" }
            })))
            .await
            .expect("attach answers with an error line");
        assert!(attach.contains("no such session"), "{attach}");
    }

    /// The engine's status events must reach the shared registry; that is what
    /// `session/list` reports to a client.
    #[tokio::test]
    async fn the_status_tracker_updates_the_registry() {
        use std::time::Duration;

        let sessions: SharedSessions = Arc::new(tokio::sync::Mutex::new(SessionRegistry::new()));
        let id = {
            let mut registry = sessions.lock().await;
            let id = registry.allocate_id();
            let (commands, _rx) = mpsc::channel(1);
            registry.insert(
                id.clone(),
                commands,
                CancelHandle::default(),
                SessionChannels::new(id.clone()),
            );
            id
        };
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        spawn_status_tracker(sessions.clone(), id.clone(), events_rx);
        events_tx
            .send(SessionEvent::StatusChanged {
                session_id: id.clone(),
                status: SessionStatus::Running,
            })
            .unwrap();

        let status = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let status = sessions.lock().await.list()[0].1;
                if status == SessionStatus::Running {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the tracker must apply the event");
        assert_eq!(status, SessionStatus::Running);
    }

    /// Every attached connection gets its own copy of an approval ask, so any
    /// of them can answer and the engine's handle makes the first one win.
    #[tokio::test]
    async fn approval_asks_reach_every_attached_connection() {
        let hub = ApprovalHub::new();
        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        hub.subscribe(tx_a).await;
        hub.subscribe(tx_b).await;

        hub.send(PermissionAsk {
            request: crate::protocol::RequestPermissionRequest {
                session_id: SessionId("sess_1".to_string()),
                tool_call: crate::protocol::ToolCallUpdate {
                    tool_call_id: crate::protocol::ToolCallId("call_1".to_string()),
                    title: Some("write a.txt".to_string()),
                    kind: None,
                    status: Some(crate::protocol::ToolCallStatus::Pending),
                    content: None,
                },
                options: Vec::new(),
            },
            request_id: 1,
            approvals: ApprovalHandle::default(),
        })
        .await;

        let first = rx_a.recv().await.expect("first attachment");
        let second = rx_b.recv().await.expect("second attachment");
        assert_eq!(first.request_id, 1);
        assert_eq!(second.request_id, 1);
    }

    /// Same fan-out as approvals, for both browser ask kinds: every attached
    /// connection sees the ask (each carrying a clone of the one answer
    /// handle). Answering through a clone is covered in `headless`'s own
    /// tests, where the engine registers the waiter.
    #[tokio::test]
    async fn browser_asks_reach_every_attached_connection() {
        let checks = BrowserCheckHub::new();
        let interactions = BrowserInteractHub::new();
        let (check_tx_a, mut check_rx_a) = mpsc::channel(4);
        let (check_tx_b, mut check_rx_b) = mpsc::channel(4);
        let (interact_tx, mut interact_rx) = mpsc::channel(4);
        checks.subscribe(check_tx_a).await;
        checks.subscribe(check_tx_b).await;
        interactions.subscribe(interact_tx).await;

        let check = BrowserCheckAsk {
            session_id: SessionId("sess_1".to_string()),
            request_id: 7,
            url: "http://localhost:3000".to_string(),
            answers: AskHandle::<BrowserCheckResult>::default(),
        };
        checks.send(check.clone()).await;

        let first = check_rx_a.recv().await.expect("first attachment");
        let second = check_rx_b.recv().await.expect("second attachment");
        assert_eq!(first.request_id, 7);
        assert_eq!(second.request_id, 7);
        assert_eq!(first.url, "http://localhost:3000");
        assert_eq!(second.url, "http://localhost:3000");

        interactions
            .send(BrowserInteractAsk {
                session_id: SessionId("sess_1".to_string()),
                request_id: 8,
                url: "http://localhost:3000".to_string(),
                interaction: crate::tools::BrowserInteraction::Click { x: 1.0, y: 2.0 },
                answers: AskHandle::<BrowserInteractResult>::default(),
            })
            .await;
        let seen = interact_rx.recv().await.expect("interaction ask");
        assert_eq!(seen.request_id, 8);
        assert!(matches!(
            seen.interaction,
            crate::tools::BrowserInteraction::Click { .. }
        ));
    }

    /// The registry is the one place ids and cancellation are owned now:
    /// allocation is monotonic, and a cancel reaches only the session named.
    #[test]
    fn the_session_registry_allocates_ids_and_targets_cancels() {
        let mut registry = SessionRegistry::new();
        let first = registry.allocate_id();
        let second = registry.allocate_id();
        assert_eq!(first.as_str(), "sess_1");
        assert_eq!(second.as_str(), "sess_2");

        let (commands_a, _rx_a) = mpsc::channel(1);
        let (commands_b, _rx_b) = mpsc::channel(1);
        let cancel_a = CancelHandle::default();
        let cancel_b = CancelHandle::default();
        registry.insert(
            first.clone(),
            commands_a,
            cancel_a.clone(),
            SessionChannels::new(first.clone()),
        );
        registry.insert(
            second.clone(),
            commands_b,
            cancel_b.clone(),
            SessionChannels::new(second.clone()),
        );

        registry.cancel(&first);
        assert!(cancel_a.is_cancelled());
        assert!(!cancel_b.is_cancelled(), "the other session is untouched");
        assert!(registry.get(&first).is_some());
        assert!(registry
            .get(&SessionId("missing".to_string()))
            .is_none());
    }
}
