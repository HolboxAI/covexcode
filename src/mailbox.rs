/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! Mailboxes for multi-agent work, persisted across processes.
//!
//! The `mailbox` tool lets a lead leave instructions for named teammates, lets
//! a child leave a note that should not wait for its final report, and lets a
//! person working in a second covexcode process see the same conversation.
//! Messages are addressed by name (`to: "*"` broadcasts) and stay unread until
//! each recipient marks it read, so an interrupted process does not lose them.
//!
//! Storage uses the same locked-document pattern as the task DAG. The key is
//! the repository's *common* git directory, not the directory the session
//! opened: a writing subagent runs in a linked worktree at a different path,
//! and it is still on the same team, so parent and worktree must resolve to
//! one mailbox. A non-git directory is its own mailbox.
//!
//! Messages are deliberately bounded: a mailbox is coordination state for work
//! in flight, not a log. Sending past the cap prunes messages that were already
//! read, and refuses rather than silently dropping unread ones.

use crate::locked_store::{self, Document};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The cap on stored messages. Beyond it, already-read messages are pruned
/// oldest first; if every message is unread, sending is refused.
const MAX_MESSAGES: usize = 500;
/// One message is a note, not a file paste. The cap keeps a mailbox readable
/// in a context window and keeps the JSON document small.
const MAX_BODY_CHARS: usize = 4000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub body: String,
    /// UTC, `YYYY-MM-DDTHH:MM:SSZ` -- the same shape as telemetry timestamps.
    pub at: String,
    /// Names that have read this message. A list rather than a flag because a
    /// broadcast has several recipients and one of them reading it must not
    /// hide it from the others.
    pub read_by: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Mailbox {
    workspace: String,
    next_id: u64,
    messages: Vec<Message>,
}

impl Document for Mailbox {
    fn workspace(&self) -> &str {
        &self.workspace
    }

    fn set_workspace(&mut self, workspace: String) {
        self.workspace = workspace;
    }
}

impl Mailbox {
    fn new(workspace: String) -> Self {
        Self {
            workspace,
            next_id: 1,
            messages: Vec::new(),
        }
    }
}

/// The directory whose mailbox `workspace` uses.
///
/// A linked git worktree reports a different `--show-toplevel` than its main
/// checkout, but the same `--git-common-dir`. Keying on the common dir is what
/// puts a writing subagent's notes in the requester's mailbox. Any failure
/// (git missing, not a repository) falls back to the workspace itself, which
/// keeps mailboxes working outside git -- they are then per directory.
pub fn root(workspace: &Path) -> PathBuf {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "--git-common-dir"])
        .output();
    let Ok(output) = output else {
        return workspace.to_path_buf();
    };
    if !output.status.success() {
        return workspace.to_path_buf();
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        return workspace.to_path_buf();
    }
    let common = PathBuf::from(text);
    // `git -C` runs in the workspace, so a relative answer (the usual `.git`)
    // is relative to it.
    let common = if common.is_absolute() {
        common
    } else {
        workspace.join(common)
    };
    common.canonicalize().unwrap_or(common)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 40
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn checked_name(name: &str, what: &str) -> Result<String, String> {
    let name = name.trim();
    if !valid_name(name) {
        return Err(format!(
            "'{name}' is not a valid {what} (letters, digits, '-' and '_', up to 40)"
        ));
    }
    Ok(name.to_string())
}

/// Leave a message for `to`. `"*"` is a broadcast; the sender does not
/// receive its own broadcast when checking their inbox.
pub fn send(workspace: &Path, from: &str, to: &str, body: &str) -> Result<Message, String> {
    let from = checked_name(from, "sender name")?;
    let to = if to.trim() == "*" {
        "*".to_string()
    } else {
        checked_name(to, "recipient name")?
    };
    if from == to {
        return Err("a message needs different sender and recipient names".to_string());
    }
    let body = body.trim().to_string();
    if body.is_empty() {
        return Err("a message needs a body".to_string());
    }
    if body.chars().count() > MAX_BODY_CHARS {
        return Err(format!(
            "messages are capped at {MAX_BODY_CHARS} characters; summarize instead"
        ));
    }
    let root = root(workspace);
    locked_store::update("mailbox", &root, true, Mailbox::new, |mailbox| {
        let message = Message {
            id: mailbox.next_id,
            from,
            to,
            body,
            at: crate::dateutil::now_utc_timestamp(),
            read_by: Vec::new(),
        };
        mailbox.next_id += 1;
        mailbox.messages.push(message.clone());
        prune(mailbox)?;
        Ok(message)
    })
}

fn prune(mailbox: &mut Mailbox) -> Result<(), String> {
    if mailbox.messages.len() <= MAX_MESSAGES {
        return Ok(());
    }
    let mut excess = mailbox.messages.len() - MAX_MESSAGES;
    mailbox.messages.retain(|message| {
        // A broadcast is prunable only when it is addressed to one person;
        // nobody can prove every teammate has read a broadcast, so those are
        // kept until a direct message makes room instead.
        let prunable = message.to != "*" && !message.read_by.is_empty();
        if excess > 0 && prunable {
            excess -= 1;
            false
        } else {
            true
        }
    });
    if mailbox.messages.len() > MAX_MESSAGES {
        return Err(format!(
            "the mailbox is full ({MAX_MESSAGES} unread messages); read some before sending"
        ));
    }
    Ok(())
}

/// Unread messages addressed to `owner`, plus broadcasts from other senders.
/// Peeking does not mark anything read; [`read`] does that.
pub fn inbox(workspace: &Path, owner: &str) -> Result<Vec<Message>, String> {
    let owner = checked_name(owner, "mailbox owner name")?;
    let root = root(workspace);
    locked_store::update("mailbox", &root, false, Mailbox::new, |mailbox| {
        Ok(mailbox
            .messages
            .iter()
            .filter(|message| !message.read_by.iter().any(|reader| reader == &owner))
            .filter(|message| message.to == owner || message.to == "*")
            .filter(|message| message.from != owner)
            .cloned()
            .collect())
    })
}

/// Return one message in full and mark it read. Reading an already-read
/// message is idempotent. Only its recipient (or the recipient of a
/// broadcast) may read it.
pub fn read(workspace: &Path, owner: &str, id: u64) -> Result<Message, String> {
    let owner = checked_name(owner, "mailbox owner name")?;
    let root = root(workspace);
    locked_store::update("mailbox", &root, true, Mailbox::new, |mailbox| {
        let message = mailbox
            .messages
            .iter_mut()
            .find(|message| message.id == id)
            .ok_or_else(|| format!("there is no message {id}"))?;
        if message.to != owner && message.to != "*" {
            return Err(format!(
                "message {id} is addressed to '{}', not '{owner}'",
                message.to
            ));
        }
        if !message.read_by.iter().any(|reader| reader == &owner) {
            message.read_by.push(owner.clone());
        }
        Ok(message.clone())
    })
}

/// What the model sees for `mailbox { action: "inbox" }`. Bodies are
/// flattened to one line and clipped so a full mailbox stays affordable;
/// `read` is how to get one in full.
pub fn render_inbox(owner: &str, messages: &[Message]) -> String {
    if messages.is_empty() {
        return format!("No unread messages for '{owner}'.");
    }
    let mut out = format!("Mailbox '{owner}' — {} unread:", messages.len());
    for message in messages {
        let flat = message
            .body
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let mut preview: String = flat.chars().take(300).collect();
        if flat.chars().count() > 300 {
            preview.push('…');
        }
        out.push_str(&format!(
            "\n[{}] from {} · {}\n    {preview}",
            message.id, message.from, message.at
        ));
    }
    out.push_str("\n\nUse mailbox { action: \"read\", id } to read one in full.");
    out
}

pub fn render_message(message: &Message) -> String {
    let mut out = format!(
        "Message {} from {} · {}",
        message.id, message.from, message.at
    );
    if message.to == "*" {
        out.push_str(" (broadcast)");
    }
    out.push_str("\n\n");
    out.push_str(&message.body);
    if message.to == "*" {
        out.push_str("\n\nReply with mailbox { action: \"send\", to: ");
        out.push_str(&message.from);
        out.push_str(", body: \"...\" }.");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_support::with_isolated_home;

    fn workspace() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().to_path_buf();
        (dir, path)
    }

    #[test]
    fn messages_are_delivered_to_their_recipient_only() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            let sent = send(&ws, "lead", "tester", "run the suite").expect("send");
            assert_eq!(sent.id, 1);
            assert_eq!(sent.from, "lead");
            assert_eq!(sent.to, "tester");
            assert!(sent.read_by.is_empty());

            let tester = inbox(&ws, "tester").expect("inbox");
            assert_eq!(tester.len(), 1);
            assert_eq!(tester[0].body, "run the suite");
            assert!(
                inbox(&ws, "worker").expect("inbox").is_empty(),
                "another agent does not see it"
            );
            assert!(
                inbox(&ws, "lead").expect("inbox").is_empty(),
                "the sender does not see its own message"
            );

            let read = read(&ws, "tester", sent.id).expect("read");
            assert_eq!(read.body, "run the suite");
            assert_eq!(read.read_by, vec!["tester".to_string()]);
            assert!(inbox(&ws, "tester").expect("inbox").is_empty());
        });
    }

    #[test]
    fn broadcasts_reach_everyone_but_the_sender() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            let sent = send(&ws, "lead", "*", "stand-up in five").expect("broadcast");
            assert_eq!(inbox(&ws, "worker").expect("inbox").len(), 1);
            assert_eq!(inbox(&ws, "tester").expect("inbox").len(), 1);
            assert!(inbox(&ws, "lead").expect("inbox").is_empty());

            // Read state is per recipient: one teammate reading a broadcast
            // must not hide it from the others.
            read(&ws, "worker", sent.id).expect("worker reads");
            assert!(inbox(&ws, "worker").expect("inbox").is_empty());
            assert_eq!(
                inbox(&ws, "tester").expect("inbox").len(),
                1,
                "the broadcast is still unread for tester"
            );
        });
    }

    #[test]
    fn only_the_recipient_may_read_a_message() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            let sent = send(&ws, "lead", "tester", "private").expect("send");
            let refused = read(&ws, "worker", sent.id).expect_err("not the recipient");
            assert!(refused.contains("tester"), "{refused}");
            assert_eq!(inbox(&ws, "tester").expect("inbox").len(), 1);
        });
    }

    #[test]
    fn names_and_bodies_are_validated() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            assert!(send(&ws, "bad name", "lead", "x").is_err());
            assert!(send(&ws, "lead", "bad name", "x").is_err());
            assert!(send(&ws, "lead", "worker", "   ").is_err());
            assert!(send(&ws, "lead", "lead", "x").is_err());
            let long = "x".repeat(MAX_BODY_CHARS + 1);
            assert!(send(&ws, "lead", "worker", &long).is_err());
        });
    }

    #[test]
    fn mailboxes_are_per_workspace_and_survive_on_disk() {
        with_isolated_home(|| {
            let (_a, ws_a) = workspace();
            let (_b, ws_b) = workspace();
            send(&ws_a, "lead", "worker", "only here").expect("send");
            assert_eq!(inbox(&ws_a, "worker").expect("inbox").len(), 1);
            assert!(inbox(&ws_b, "worker").expect("inbox").is_empty());

            let path = locked_store::data_path("mailbox", &root(&ws_a));
            assert!(path.exists(), "the mailbox is on disk");
            let raw = std::fs::read_to_string(&path).expect("read");
            assert!(raw.contains("only here"), "{raw}");
        });
    }

    #[test]
    fn a_linked_worktree_shares_the_parent_mailbox() {
        let dir = tempfile::tempdir().expect("temp dir");
        let repo = dir.path().to_path_buf();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(args)
                .output()
                .expect("git runs");
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "test@example.com"]);
        git(&["config", "user.name", "Test"]);
        std::fs::write(repo.join("tracked.txt"), "x\n").expect("write");
        git(&["add", "tracked.txt"]);
        git(&["commit", "-qm", "initial"]);
        let base = tempfile::tempdir().expect("base");
        let worktree = base.path().join("wt");
        git(&[
            "worktree",
            "add",
            "--detach",
            worktree.to_str().expect("utf-8"),
            "HEAD",
        ]);

        assert_eq!(
            root(&worktree),
            root(&repo),
            "a writing child shares the requester's mailbox"
        );
    }
}
