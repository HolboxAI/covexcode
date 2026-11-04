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

/// Transport-agnostic output from a session engine.
///
/// The ACP path continues to emit its own `SessionUpdate` stream; this is the
/// stream a TUI, daemon, or orchestration client can consume without speaking
/// ACP. It starts with the turn/text spine; tool and approval events are
/// added when the engine loop moves behind this boundary.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    TurnStarted {
        session_id: SessionId,
        turn: TurnId,
    },
    TextDelta {
        session_id: SessionId,
        turn: TurnId,
        text: String,
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
