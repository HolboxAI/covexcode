//! Prompt history persistence -- ~/.covexcode/prompt_history.jsonl
//!
//! Preserves complete prompts across CLI restarts, including multiline prompts,
//! using JSON Lines (JSONL) format so that newline characters inside prompts are
//! safely preserved within a single JSON record.
//!
//! Follows the same pattern as `session.rs` and `usage.rs`: an append-only file
//! that survives interruptions, tolerates malformed or truncated lines, and avoids
//! storing consecutive duplicate prompts.

use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_PROMPTS: usize = 1000;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PromptRecord {
    pub prompt: String,
    pub timestamp: u64,
}

fn history_path() -> Option<PathBuf> {
    std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    Some(crate::config::Config::config_dir().join("prompt_history.jsonl"))
}

/// Load saved prompts from the history file, in chronological order.
///
/// Malformed or unparseable lines are skipped without corrupting the rest.
/// Consecutive duplicate entries are deduplicated.
pub fn load_prompts() -> Vec<String> {
    let Some(path) = history_path() else {
        return Vec::new();
    };
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let mut prompts = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_str::<PromptRecord>(line) {
            if !record.prompt.trim().is_empty() && prompts.last() != Some(&record.prompt) {
                prompts.push(record.prompt);
            }
        }
    }
    if prompts.len() > MAX_PROMPTS {
        let skip = prompts.len() - MAX_PROMPTS;
        prompts.drain(..skip);
    }
    prompts
}

/// Append a single prompt to the persistent history file.
pub fn append_prompt(prompt: &str) {
    let trimmed = prompt.trim();
    if trimmed.is_empty() {
        return;
    }
    #[cfg(test)]
    {
        if std::env::var("HOME")
            .map(|h| !h.contains(".tmp") && !h.contains("temp"))
            .unwrap_or(true)
        {
            return;
        }
    }
    let Some(path) = history_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let record = PromptRecord {
        prompt: prompt.to_string(),
        timestamp: now,
    };
    if let Ok(line) = serde_json::to_string(&record) {
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            let _ = writeln!(file, "{line}");
        }
    }
}

/// Rewrite the history file to enforce the maximum entry ceiling.
#[allow(dead_code)]
pub fn rewrite_prompts(prompts: &[String]) {
    let Some(path) = history_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let to_write = if prompts.len() > MAX_PROMPTS {
        &prompts[prompts.len() - MAX_PROMPTS..]
    } else {
        prompts
    };
    let mut content = String::new();
    for p in to_write {
        let record = PromptRecord {
            prompt: p.clone(),
            timestamp: now,
        };
        if let Ok(line) = serde_json::to_string(&record) {
            content.push_str(&line);
            content.push('\n');
        }
    }
    let _ = std::fs::write(path, content);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multiline_prompts_roundtrip_intact() {
        crate::config::test_support::with_isolated_home(|| {
            let multiline = "first line\nsecond line\n    indented third line\n\"quoted\"";
            append_prompt(multiline);

            let loaded = load_prompts();
            assert_eq!(loaded.len(), 1);
            assert_eq!(loaded[0], multiline);
        });
    }

    #[test]
    fn malformed_lines_are_skipped() {
        crate::config::test_support::with_isolated_home(|| {
            append_prompt("valid 1");
            let path = history_path().unwrap();
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(file, "not a valid json line").unwrap();
            append_prompt("valid 2");

            let loaded = load_prompts();
            assert_eq!(loaded, vec!["valid 1".to_string(), "valid 2".to_string()]);
        });
    }

    #[test]
    fn consecutive_duplicates_are_deduplicated_on_load() {
        crate::config::test_support::with_isolated_home(|| {
            append_prompt("repeat");
            append_prompt("repeat");
            append_prompt("other");
            append_prompt("repeat");

            let loaded = load_prompts();
            assert_eq!(
                loaded,
                vec![
                    "repeat".to_string(),
                    "other".to_string(),
                    "repeat".to_string()
                ]
            );
        });
    }

    #[test]
    fn enforces_max_prompts_ceiling() {
        crate::config::test_support::with_isolated_home(|| {
            let mut list = Vec::new();
            for i in 0..1050 {
                list.push(format!("prompt {i}"));
            }
            rewrite_prompts(&list);

            let loaded = load_prompts();
            assert_eq!(loaded.len(), MAX_PROMPTS);
            assert_eq!(loaded[0], "prompt 50");
            assert_eq!(loaded[MAX_PROMPTS - 1], "prompt 1049");
        });
    }
}
