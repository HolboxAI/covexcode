/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

use crate::llm::{ImageAttachment, ToolCall};

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
    Error,
    /// Confirmations from `/provider` and `/model`, e.g. "Switched to deepseek /
    /// deepseek-v4-flash." Distinct from Assistant (would wrongly imply the
    /// model said it) and Error (wrong tone/color for a success message).
    System,
    /// The result of one tool call, sent back to the model as `role: "tool"`.
    Tool,
    /// Local news the model has to be told about: right now, a `/rollback`
    /// naming the files it just put back.
    ///
    /// Its own role because neither neighbour fits. `System` is commentary
    /// `history` drops, and dropping this one would leave the model editing
    /// against a disk that no longer matches anything it was told. `Summary`
    /// does reach the wire, but it means "this replaces the messages above",
    /// and borrowing it would make a rollback look like a compaction in both
    /// the transcript and the session file. So: shown like a `System` notice,
    /// sent like a `Summary`.
    Context,
    /// A `/compact` summary standing in for everything that came before it.
    ///
    /// Deliberately its own role rather than `System`: a System message is
    /// local commentary that `history` drops, and a summary that never reaches
    /// the model would mean compaction silently *erased* the conversation
    /// instead of condensing it. This one goes on the wire.
    Summary,
}

// Serialized as one line of a session file (see `session.rs`); the
// `default`s keep a file written by an older build loadable by a newer one.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub role: Role,
    /// What goes on the wire. For a tool result this is the entire file, which is
    /// why it is not what gets drawn.
    pub content: String,
    /// What the transcript shows, when that differs from `content`.
    #[serde(default)]
    pub display: Option<String>,
    /// Tool calls the assistant asked for. Only ever set on `Role::Assistant`.
    #[serde(default)]
    pub tool_calls: Vec<ToolCall>,
    /// Which call this message answers. Only ever set on `Role::Tool`.
    #[serde(default)]
    pub tool_call_id: Option<String>,
    /// What this tool call changed on disk, if it changed a file. Set on
    /// `Role::Tool` and drawn under the tool line as a `-`/`+` diff; also set
    /// on the `Role::System` messages `/diff` pushes, one per changed file,
    /// where it is drawn the same way.
    ///
    /// Carried on the message rather than re-derived at render time for the
    /// same reason `Deploy`'s summary is: the file has already been written by
    /// the time this is drawn, so asking the disk again would show the diff
    /// against the *new* contents -- which is to say, nothing. It has to be
    /// captured at the moment of the change or not at all. (`/diff` is the
    /// one exception -- it exists precisely to diff against what is on disk
    /// *right now*, so it computes fresh rather than reusing a captured one.)
    ///
    /// `#[serde(default)]`, like every field above it, so a session file
    /// written before this existed still loads.
    #[serde(default)]
    pub diff: Option<crate::diff::FileDiff>,
    /// Images attached to a `Role::User` message (Ctrl+V clipboard paste, or a
    /// pasted/dropped image path). `#[serde(skip)]`: these are transient — they
    /// ride the message's *next* trip to the LLM and then the message is
    /// evicted from `history()` (see `history`), so persisting base64 blobs into
    /// the session file would bloat it with data that can never be replayed.
    #[serde(skip)]
    pub images: Vec<ImageAttachment>,
}

impl Message {
    pub fn new(role: Role, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            display: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
            diff: None,
            images: Vec::new(),
        }
    }

    /// The text to draw, which for a tool result is a one-line summary rather
    /// than the file it fetched.
    pub fn body(&self) -> &str {
        self.display.as_deref().unwrap_or(&self.content)
    }
}

impl Role {
    pub fn label(&self) -> &'static str {
        match self {
            Role::User => "You",
            Role::Assistant => "Assistant",
            Role::Error => "Error",
            Role::System => "System",
            Role::Tool => "Tool",
            Role::Summary => "Summary",
            Role::Context => "Rollback",
        }
    }
}
