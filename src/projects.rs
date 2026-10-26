//! Project history tracking -- ~/.covexcode/projects.json
//!
//! Tracks recently and frequently opened projects across CLI launches, allowing
//! fast switching between projects via `/projects`.
//!
//! Follows the same pattern as `artifacts.json` and `quota.json`: a plain JSON file
//! in `Config::config_dir()`.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_PROJECTS: usize = 100;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ProjectEntry {
    pub path: String,
    pub name: String,
    pub last_accessed: u64,
    pub access_count: u64,
}

fn projects_path() -> Option<PathBuf> {
    std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
    Some(crate::config::Config::config_dir().join("projects.json"))
}

pub fn load_projects() -> Vec<ProjectEntry> {
    let Some(path) = projects_path() else {
        return Vec::new();
    };
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_projects(entries: &[ProjectEntry]) {
    #[cfg(test)]
    {
        if std::env::var("HOME")
            .map(|h| !h.contains(".tmp") && !h.contains("temp"))
            .unwrap_or(true)
        {
            return;
        }
    }
    let Some(path) = projects_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(json) = serde_json::to_string_pretty(entries) {
        let _ = std::fs::write(path, json);
    }
}

pub fn record_access(path: &Path) {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let canonical_str = canonical.to_string_lossy().to_string();
    let name = canonical
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| canonical_str.clone());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut entries = load_projects();
    if let Some(existing) = entries.iter_mut().find(|e| e.path == canonical_str) {
        existing.last_accessed = now;
        existing.access_count = existing.access_count.saturating_add(1);
        existing.name = name;
    } else {
        entries.push(ProjectEntry {
            path: canonical_str,
            name,
            last_accessed: now,
            access_count: 1,
        });
    }

    if entries.len() > MAX_PROJECTS {
        entries.sort_by_key(|e| std::cmp::Reverse(e.last_accessed));
        entries.truncate(MAX_PROJECTS);
    }
    save_projects(&entries);
}

pub fn recent_projects() -> Vec<ProjectEntry> {
    let mut entries = load_projects();
    entries.sort_by_key(|b| std::cmp::Reverse(b.last_accessed));
    entries
}

pub fn top_projects() -> Vec<ProjectEntry> {
    let mut entries = load_projects();
    entries.sort_by(|a, b| {
        b.access_count
            .cmp(&a.access_count)
            .then_with(|| b.last_accessed.cmp(&a.last_accessed))
    });
    entries
}

pub fn remove_project(path: &str) {
    let mut entries = load_projects();
    entries.retain(|e| e.path != path);
    save_projects(&entries);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_updates_access() {
        crate::config::test_support::with_isolated_home(|| {
            let dir = tempfile::tempdir().unwrap();
            let p = dir.path();
            record_access(p);

            let list = recent_projects();
            assert_eq!(list.len(), 1);
            let canonical = p.canonicalize().unwrap().to_string_lossy().to_string();
            assert_eq!(list[0].path, canonical);
            assert_eq!(list[0].access_count, 1);

            record_access(p);
            let list = recent_projects();
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].access_count, 2);
        });
    }

    #[test]
    fn sorts_recent_and_top_differently() {
        crate::config::test_support::with_isolated_home(|| {
            let dir_a = tempfile::tempdir().unwrap();
            let dir_b = tempfile::tempdir().unwrap();

            // Project A accessed 3 times
            record_access(dir_a.path());
            record_access(dir_a.path());
            record_access(dir_a.path());

            // Project B accessed once later
            std::thread::sleep(std::time::Duration::from_millis(10));
            record_access(dir_b.path());

            let recent = recent_projects();
            let top = top_projects();

            let canonical_a = dir_a
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let canonical_b = dir_b
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string();

            // Most recent is B
            assert_eq!(recent[0].path, canonical_b);
            assert_eq!(recent[1].path, canonical_a);

            // Top (frequent) is A
            assert_eq!(top[0].path, canonical_a);
            assert_eq!(top[1].path, canonical_b);
        });
    }

    #[test]
    fn removes_project_cleanly() {
        crate::config::test_support::with_isolated_home(|| {
            let dir = tempfile::tempdir().unwrap();
            record_access(dir.path());
            let canonical = dir
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string();

            assert_eq!(recent_projects().len(), 1);
            remove_project(&canonical);
            assert_eq!(recent_projects().len(), 0);
        });
    }
}
