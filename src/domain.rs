/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! Core identities shared by the TUI, the ACP server, and the future session
//! daemon.
//!
//! `SessionId` used to live in `protocol.rs`, which made the wire format the
//! owner of a domain concept. It now lives here; `protocol::SessionId` is a
//! re-export, so every existing ACP type and JSON shape is unchanged.
//!
//! The remaining identities and `SessionStatus` are the vocabulary for the
//! session-engine and orchestration work described in
//! `docs/MULTI_AGENT_ARCHITECTURE.md`. They are defined here now so those
//! layers do not invent a second set of names. They are intentionally not
//! wired into the current `App`/`HeadlessSession` paths yet.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifies one conversation, whether it is driven by the TUI, ACP, or a
/// daemon client.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

#[allow(dead_code)]
impl SessionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies one agent inside a session tree. The root agent and every
/// subagent get one; `SessionId` alone is not enough once a parent can spawn
/// siblings and children.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentId(pub String);

#[allow(dead_code)]
impl AgentId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identifies one turn within a session. Turns are serialized per session, so
/// this is the unit a cancel or approval request targets.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TurnId(pub u64);

#[allow(dead_code)]
impl TurnId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Identifies one item on the shared task list/dependency graph.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(pub String);

#[allow(dead_code)]
impl TaskId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Coarse lifecycle state exposed to a client. The engine owns finer-grained
/// turn states internally.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    WaitingForApproval,
    Cancelled,
    Closed,
}

/// Role a child agent plays in an orchestrated task. The first implementation
/// only needs `Explore`; the others are the planned worktree agents.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Lead,
    Explore,
    Implement,
    Test,
    Review,
}

/// Whether an agent shares the parent's workspace or gets its own git
/// worktree. Read-only agents can share; writers should not.
#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    Shared,
    Worktree,
}

/// Why a turn ended, independent of the ACP wire enum. The engine emits this
/// to every client; the ACP adapter maps it back to `StopReason`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnStopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
}

/// Lifecycle of one tool call as a client needs to render it. This is the
/// transport-agnostic counterpart of the ACP tool status; `Completed` and
/// `Failed` are both terminal.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// One option a client can choose for an approval request.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ApprovalOption {
    /// Opaque engine option id, echoed back in the resolution.
    pub id: String,
    pub label: String,
    pub kind: ApprovalOptionKind,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
}

/// What the client is being asked to approve, in transport-neutral terms.
/// Mirrors the shapes the engine already builds for the ACP permission
/// request, so a non-ACP client can render the same review.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApprovalDetail {
    Text {
        text: String,
    },
    Diff {
        path: String,
        old_text: Option<String>,
        new_text: String,
    },
    Plan {
        title: String,
        summary: String,
        steps: Vec<String>,
        not_doing: Vec<String>,
    },
    Image {
        mime_type: String,
        data_base64: String,
    },
}

/// How an approval request ended.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApprovalResolution {
    Selected { option_id: String },
    /// The client accepted some hunks and returned the merged file text the
    /// engine should write instead of executing the original call.
    Partial { new_text: String },
    Cancelled,
    /// The client disconnected before answering.
    Unavailable,
}

/// A client-to-engine control command. These are the actions that must work
/// while a turn is running; a prompt itself is still the turn's entry point.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionCommand {
    /// Stop the current turn. Idempotent, and safe while idle.
    Cancel,
    /// Answer the approval request with this engine-local id.
    RespondApproval {
        request_id: u64,
        resolution: ApprovalResolution,
    },
}

/// Transport-agnostic output from a session engine.
///
/// The ACP path continues to emit its own `SessionUpdate` stream; this is the
/// stream a TUI, daemon, or orchestration client can consume without speaking
/// ACP. It covers the turn spine, status, streamed text, tool lifecycle, usage,
/// and approval requests/resolutions.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    TurnStarted {
        session_id: SessionId,
        turn: TurnId,
    },
    /// The session's coarse lifecycle state changed. A client can render
    /// "working" / "waiting for you" without inferring it from turn events.
    StatusChanged {
        session_id: SessionId,
        status: SessionStatus,
    },
    TextDelta {
        session_id: SessionId,
        turn: TurnId,
        text: String,
    },
    ToolCallStarted {
        session_id: SessionId,
        turn: TurnId,
        tool_call_id: String,
        title: String,
    },
    ToolCallUpdated {
        session_id: SessionId,
        turn: TurnId,
        tool_call_id: String,
        status: Option<ToolStatus>,
        /// Text the client should show for this call (an error, a short
        /// result). Diffs are not copied into the stream; an interactive
        /// client gets the change through the approval request.
        text: Option<String>,
    },
    /// One tool call a child agent made, reported against the parent `agent`
    /// call. A frontend can show live subagent progress (`/subagents`)
    /// without waiting for the child's final report.
    SubagentActivity {
        session_id: SessionId,
        turn: TurnId,
        /// The parent `agent` call.
        call_id: String,
        /// One `Action::label()`-style line for the child's call.
        label: String,
        rounds: usize,
    },
    /// The teammate for a parent `agent` call is done. Foreground children
    /// already close their trail through the parent call's completion; a
    /// background child has no later tool result, so this is the event that
    /// tells a `/agents` panel to stop showing it as running.
    SubagentFinished {
        session_id: SessionId,
        turn: TurnId,
        call_id: String,
        /// "done" or "failed" -- a short label, not the report itself.
        summary: String,
    },
    UsageUpdated {
        session_id: SessionId,
        turn: TurnId,
        /// Prompt + completion spend of this one response.
        used: u64,
        /// The model's context window, `0` when unknown.
        size: u64,
        prompt_tokens: Option<u64>,
    },
    ApprovalRequested {
        session_id: SessionId,
        turn: TurnId,
        /// Engine-issued id the client echoes in its resolution.
        request_id: u64,
        tool_call_id: String,
        title: String,
        detail: Option<ApprovalDetail>,
        options: Vec<ApprovalOption>,
    },
    ApprovalResolved {
        session_id: SessionId,
        turn: TurnId,
        request_id: u64,
        resolution: ApprovalResolution,
    },
    /// A manual `/compact` finished. `summary` is present when the history
    /// was actually replaced; `error` describes why it was not. Auto-
    /// compaction deliberately does not emit this -- it is not a user
    /// command and the next request already reflects the new history.
    Compacted {
        session_id: SessionId,
        turn: TurnId,
        summary: Option<String>,
        error: Option<String>,
    },
    TurnFinished {
        session_id: SessionId,
        turn: TurnId,
        reason: TurnStopReason,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_round_trips_and_displays_like_the_wire_type() {
        let id = SessionId::new("sess_123");
        assert_eq!(id.as_str(), "sess_123");
        assert_eq!(id.to_string(), "sess_123");

        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"sess_123\"");
        assert_eq!(serde_json::from_str::<SessionId>(&json).unwrap(), id);
    }

    #[test]
    fn subtree_identities_round_trip() {
        let agent = AgentId::new("agent_1");
        let turn = TurnId::new(7);
        let task = TaskId::new("task_1");

        assert_eq!(agent.as_str(), "agent_1");
        assert_eq!(turn.get(), 7);
        assert_eq!(task.as_str(), "task_1");
        assert_eq!(
            serde_json::to_string(&turn).unwrap(),
            "7",
            "TurnId is a number on the wire"
        );
    }

    #[test]
    fn session_events_carry_their_envelope_and_round_trip() {
        let event = SessionEvent::TextDelta {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(2),
            text: "hello".to_string(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"text_delta\""), "{json}");
        assert_eq!(serde_json::from_str::<SessionEvent>(&json).unwrap(), event);

        let finished = SessionEvent::TurnFinished {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(2),
            reason: TurnStopReason::EndTurn,
        };
        let json = serde_json::to_string(&finished).unwrap();
        assert!(json.contains("\"reason\":\"end_turn\""), "{json}");

        let waiting = SessionEvent::StatusChanged {
            session_id: SessionId::new("sess_1"),
            status: SessionStatus::WaitingForApproval,
        };
        let json = serde_json::to_string(&waiting).unwrap();
        assert!(json.contains("\"type\":\"status_changed\""), "{json}");
        assert!(
            json.contains("\"status\":\"waiting_for_approval\""),
            "{json}"
        );
        assert_eq!(serde_json::from_str::<SessionEvent>(&json).unwrap(), waiting);
    }

    #[test]
    fn tool_and_usage_events_round_trip() {
        let started = SessionEvent::ToolCallStarted {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(1),
            tool_call_id: "call_1".to_string(),
            title: "Read hello.txt".to_string(),
        };
        let json = serde_json::to_string(&started).unwrap();
        assert!(json.contains("\"type\":\"tool_call_started\""), "{json}");
        assert_eq!(
            serde_json::from_str::<SessionEvent>(&json).unwrap(),
            started
        );

        let updated = SessionEvent::ToolCallUpdated {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(1),
            tool_call_id: "call_1".to_string(),
            status: Some(ToolStatus::Completed),
            text: Some("hi".to_string()),
        };
        let json = serde_json::to_string(&updated).unwrap();
        assert!(json.contains("\"status\":\"completed\""), "{json}");
        assert_eq!(
            serde_json::from_str::<SessionEvent>(&json).unwrap(),
            updated
        );

        let usage = SessionEvent::UsageUpdated {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(1),
            used: 120,
            size: 200_000,
            prompt_tokens: Some(100),
        };
        let json = serde_json::to_string(&usage).unwrap();
        assert!(json.contains("\"type\":\"usage_updated\""), "{json}");
        assert_eq!(serde_json::from_str::<SessionEvent>(&json).unwrap(), usage);
    }

    #[test]
    fn approval_events_round_trip() {
        let requested = SessionEvent::ApprovalRequested {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(3),
            request_id: 7,
            tool_call_id: "call_1".to_string(),
            title: "write src/lib.rs".to_string(),
            detail: Some(ApprovalDetail::Diff {
                path: "src/lib.rs".to_string(),
                old_text: Some("old".to_string()),
                new_text: "new".to_string(),
            }),
            options: vec![ApprovalOption {
                id: "allow_once".to_string(),
                label: "Allow".to_string(),
                kind: ApprovalOptionKind::AllowOnce,
            }],
        };
        let json = serde_json::to_string(&requested).unwrap();
        assert!(json.contains("\"type\":\"approval_requested\""), "{json}");
        assert!(json.contains("\"kind\":\"allow_once\""), "{json}");
        assert_eq!(
            serde_json::from_str::<SessionEvent>(&json).unwrap(),
            requested
        );

        let resolved = SessionEvent::ApprovalResolved {
            session_id: SessionId::new("sess_1"),
            turn: TurnId::new(3),
            request_id: 7,
            resolution: ApprovalResolution::Selected {
                option_id: "allow_once".to_string(),
            },
        };
        let json = serde_json::to_string(&resolved).unwrap();
        assert!(json.contains("\"type\":\"approval_resolved\""), "{json}");
        assert_eq!(serde_json::from_str::<SessionEvent>(&json).unwrap(), resolved);
    }

    #[test]
    fn session_commands_round_trip() {
        let cancel = SessionCommand::Cancel;
        let json = serde_json::to_string(&cancel).unwrap();
        assert!(json.contains("\"type\":\"cancel\""), "{json}");
        assert_eq!(
            serde_json::from_str::<SessionCommand>(&json).unwrap(),
            cancel
        );

        let answer = SessionCommand::RespondApproval {
            request_id: 4,
            resolution: ApprovalResolution::Selected {
                option_id: "allow".to_string(),
            },
        };
        let json = serde_json::to_string(&answer).unwrap();
        assert!(json.contains("\"type\":\"respond_approval\""), "{json}");
        assert_eq!(
            serde_json::from_str::<SessionCommand>(&json).unwrap(),
            answer
        );
    }

    #[test]
    fn status_and_policy_enums_use_snake_case() {
        assert_eq!(
            serde_json::to_string(&SessionStatus::WaitingForApproval).unwrap(),
            "\"waiting_for_approval\""
        );
        assert_eq!(
            serde_json::to_string(&AgentRole::Explore).unwrap(),
            "\"explore\""
        );
        assert_eq!(
            serde_json::to_string(&Isolation::Worktree).unwrap(),
            "\"worktree\""
        );
    }
}
