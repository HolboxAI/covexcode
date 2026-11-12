//! A shared task DAG for multi-agent work, persisted across processes.
//!
//! The parent model (or a person) creates tasks with dependencies; an agent
//! claims one before working on it and completes it when done. Claims are
//! atomic under a file lock, so two processes looking at the same pending task
//! cannot both start it -- the plan's `UPDATE ... WHERE status='pending'`
//! guarantee, without a database.
//!
//! Storage: one JSON file per canonical workspace under
//! `CONFIG_DIR/tasks/<hash>.json`, guarded by a stable sibling
//! `<hash>.lock`. The lock file is what makes the pattern correct: a waiter
//! always locks the same inode, then opens the data file fresh, so replacing
//! the data file by rename cannot hand a waiter a stale copy. Every operation
//! is read-modify-write under that lock; the write goes to a temp file and is
//! renamed into place, so a crash leaves the previous store intact.
//!
//! This is coordination state for work in flight, not the session transcript:
//! the conversation that created the task list is still the record of what was
//! meant. A corrupt store is reported rather than silently reset, because
//! resetting would silently free claims another agent is working under.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pending,
    InProgress,
    Done,
}

impl Status {
    fn marker(self) -> &'static str {
        match self {
            Status::Pending => "[ ]",
            Status::InProgress => "[~]",
            Status::Done => "[x]",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub description: String,
    pub depends_on: Vec<String>,
    /// Files or directories this task owns, for a future path-conflict
    /// scheduler. Recorded now so the plan can name them.
    pub paths: Vec<String>,
    pub status: Status,
    pub owner: Option<String>,
    pub note: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Store {
    /// The workspace this file belongs to. Checked on load so a hash
    /// collision cannot silently mix two projects' task lists.
    workspace: String,
    next_id: u64,
    tasks: BTreeMap<String, Task>,
}

impl Store {
    fn new(workspace: String) -> Self {
        Self {
            workspace,
            next_id: 1,
            tasks: BTreeMap::new(),
        }
    }

    fn generate_id(&mut self) -> String {
        loop {
            let id = format!("task-{}", self.next_id);
            self.next_id += 1;
            if !self.tasks.contains_key(&id) {
                return id;
            }
        }
    }

    fn deps_done(&self, task: &Task) -> Result<(), Vec<String>> {
        let missing: Vec<String> = task
            .depends_on
            .iter()
            .filter(|id| {
                self.tasks
                    .get(*id)
                    .map(|dep| dep.status != Status::Done)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(missing)
        }
    }
}

/// Serializes threads in this process before the OS lock is even attempted.
/// The OS lock would already exclude a second open file description, but
/// making the in-process order explicit keeps tests and a busy TUI
/// deterministic without relying on that detail.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

fn store_dir() -> PathBuf {
    crate::config::Config::config_dir().join("tasks")
}

fn key(workspace: &Path) -> String {
    let digest = Sha256::digest(workspace.to_string_lossy().as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()[..16]
        .to_string()
}

fn data_path(workspace: &Path) -> PathBuf {
    store_dir().join(format!("{}.json", key(workspace)))
}

fn lock_path(workspace: &Path) -> PathBuf {
    store_dir().join(format!("{}.lock", key(workspace)))
}

fn with_store<T>(
    workspace: &Path,
    write: bool,
    f: impl FnOnce(&mut Store) -> Result<T, String>,
) -> Result<T, String> {
    let _process = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = store_dir();
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("could not create {}: {e}", dir.display()))?;

    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path(workspace))
        .map_err(|e| format!("could not open the task lock: {e}"))?;
    lock.lock()
        .map_err(|e| format!("could not lock the task store: {e}"))?;

    let data = data_path(workspace);
    let workspace_label = workspace.to_string_lossy().into_owned();
    let mut store = if data.exists() {
        let mut file = std::fs::File::open(&data)
            .map_err(|e| format!("could not read {}: {e}", data.display()))?;
        let mut text = String::new();
        file.read_to_string(&mut text)
            .map_err(|e| format!("could not read {}: {e}", data.display()))?;
        let mut store: Store = serde_json::from_str(&text).map_err(|e| {
            format!(
                "the task store {} is unreadable ({e}); fix or remove it",
                data.display()
            )
        })?;
        if !store.workspace.is_empty() && store.workspace != workspace_label {
            return Err(format!(
                "the task store {} belongs to another workspace ({}); refusing to mix them",
                data.display(),
                store.workspace
            ));
        }
        store.workspace = workspace_label;
        store
    } else {
        Store::new(workspace_label)
    };

    let result = f(&mut store);
    if result.is_ok() && write {
        let json = serde_json::to_string_pretty(&store)
            .map_err(|e| format!("could not serialize the task store: {e}"))?;
        let tmp = dir.join(format!(
            "{}.json.tmp.{}",
            key(workspace),
            std::process::id()
        ));
        std::fs::write(&tmp, json)
            .map_err(|e| format!("could not write {}: {e}", tmp.display()))?;
        // Replace in one step: Unix rename and Windows `MoveFileEx` with
        // `MOVEFILE_REPLACE_EXISTING` both swap an existing destination, so a
        // crash leaves either the old store or the new one, never neither.
        // Waiters lock the separate `.lock` file and reopen the data path
        // after acquiring it, so the replaced inode cannot be stale for them.
        std::fs::rename(&tmp, &data)
            .map_err(|e| format!("could not replace {}: {e}", data.display()))?;
    }
    let _ = lock.unlock();
    result
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 40
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

pub fn create(
    workspace: &Path,
    id: Option<String>,
    title: String,
    description: String,
    depends_on: Vec<String>,
    paths: Vec<String>,
) -> Result<Task, String> {
    let title = title.trim().to_string();
    if title.is_empty() {
        return Err("a task needs a title".to_string());
    }
    with_store(workspace, true, |store| {
        let id = match id.map(|id| id.trim().to_string()) {
            Some(id) => {
                if !valid_id(&id) {
                    return Err(format!(
                        "'{id}' is not a valid task id (letters, digits, '-' and '_', up to 40)"
                    ));
                }
                if store.tasks.contains_key(&id) {
                    return Err(format!("task '{id}' already exists"));
                }
                id
            }
            None => store.generate_id(),
        };
        for dep in &depends_on {
            if dep == &id {
                return Err("a task cannot depend on itself".to_string());
            }
            if !store.tasks.contains_key(dep) {
                return Err(format!(
                    "dependency '{dep}' does not exist; create it first"
                ));
            }
        }
        let task = Task {
            id: id.clone(),
            title,
            description: description.trim().to_string(),
            depends_on,
            paths,
            status: Status::Pending,
            owner: None,
            note: None,
        };
        store.tasks.insert(id, task.clone());
        Ok(task)
    })
}

/// Claim a pending task. Atomic across processes: only one caller gets `Ok`
/// for a given task unless it is the same owner re-claiming its own
/// in-progress work.
pub fn claim(workspace: &Path, id: &str, owner: &str) -> Result<Task, String> {
    let owner = owner.trim();
    if owner.is_empty() {
        return Err("a claim needs an owner name".to_string());
    }
    with_store(workspace, true, |store| {
        let task = store
            .tasks
            .get(id)
            .cloned()
            .ok_or_else(|| format!("there is no task '{id}'"))?;
        if task.status == Status::Done {
            return Err(format!("task '{id}' is already complete"));
        }
        if task.status == Status::InProgress {
            if task.owner.as_deref() == Some(owner) {
                return Ok(task);
            }
            return Err(format!(
                "task '{id}' is already claimed by {}",
                task.owner.as_deref().unwrap_or("another agent")
            ));
        }
        if let Err(blocked) = store.deps_done(&task) {
            return Err(format!(
                "task '{id}' is blocked by {} until {} done",
                blocked.join(", "),
                if blocked.len() == 1 {
                    "it is"
                } else {
                    "they are"
                }
            ));
        }
        let task = store.tasks.get_mut(id).expect("checked above");
        task.status = Status::InProgress;
        task.owner = Some(owner.to_string());
        Ok(task.clone())
    })
}

/// Complete an in-progress task. Returns the task plus the ids of pending
/// tasks whose dependencies just became satisfied.
pub fn complete(
    workspace: &Path,
    id: &str,
    owner: Option<&str>,
    note: Option<String>,
) -> Result<(Task, Vec<String>), String> {
    with_store(workspace, true, |store| {
        let task = store
            .tasks
            .get(id)
            .ok_or_else(|| format!("there is no task '{id}'"))?;
        if task.status == Status::Done {
            return Err(format!("task '{id}' is already complete"));
        }
        if task.status == Status::Pending {
            return Err(format!(
                "task '{id}' has not been claimed; claim it before completing it"
            ));
        }
        if let (Some(expected), Some(actual)) = (task.owner.as_deref(), owner.map(str::trim)) {
            if expected != actual {
                return Err(format!(
                    "task '{id}' is claimed by {expected}, not {actual}"
                ));
            }
        }
        let task = store.tasks.get_mut(id).expect("checked above");
        task.status = Status::Done;
        if let Some(note) = note {
            let note = note.trim().to_string();
            if !note.is_empty() {
                task.note = Some(note);
            }
        }
        let completed = task.clone();
        let unblocked: Vec<String> = store
            .tasks
            .values()
            .filter(|candidate| candidate.status == Status::Pending)
            // Only dependents of this task can be *newly* unblocked; a task
            // that never had dependencies was already claimable.
            .filter(|candidate| candidate.depends_on.iter().any(|dep| dep == id))
            .filter(|candidate| store.deps_done(candidate).is_ok())
            .map(|candidate| candidate.id.clone())
            .collect();
        Ok((completed, unblocked))
    })
}

pub fn list(workspace: &Path) -> Result<Vec<Task>, String> {
    with_store(workspace, false, |store| {
        Ok(store.tasks.values().cloned().collect())
    })
}

/// The text the model sees for `tasks { action: "list" }`.
pub fn render(workspace: &Path) -> Result<String, String> {
    let tasks = list(workspace)?;
    if tasks.is_empty() {
        return Ok("No tasks yet.".to_string());
    }
    let mut out = String::from("Tasks:");
    for task in tasks {
        let owner = task
            .owner
            .as_deref()
            .map(|owner| format!(" · {owner}"))
            .unwrap_or_default();
        let deps = if task.depends_on.is_empty() {
            String::new()
        } else {
            format!(" · after {}", task.depends_on.join(", "))
        };
        let paths = if task.paths.is_empty() {
            String::new()
        } else {
            format!(" · {}", task.paths.join(", "))
        };
        out.push_str(&format!(
            "\n{} {} — {}{owner}{deps}{paths}",
            task.status.marker(),
            task.id,
            task.title
        ));
        if !task.description.is_empty() {
            out.push_str(&format!("\n    {}", task.description));
        }
        if let Some(note) = &task.note {
            out.push_str(&format!("\n    note: {note}"));
        }
    }
    Ok(out)
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
    fn a_claim_is_atomic_and_dependencies_block_it() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            let first = create(
                &ws,
                None,
                "first".into(),
                String::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("create");
            let second = create(
                &ws,
                None,
                "second".into(),
                String::new(),
                vec![first.id.clone()],
                Vec::new(),
            )
            .expect("create");

            let blocked = claim(&ws, &second.id, "agent-a").expect_err("blocked");
            assert!(blocked.contains(&first.id), "{blocked}");

            claim(&ws, &first.id, "agent-a").expect("claim first");
            let double = claim(&ws, &first.id, "agent-b").expect_err("already claimed");
            assert!(double.contains("agent-a"), "{double}");

            let (_, unblocked) = complete(&ws, &first.id, Some("agent-a"), None).expect("complete");
            assert_eq!(unblocked, vec![second.id.clone()]);

            let claimed = claim(&ws, &second.id, "agent-b").expect("unblocked now");
            assert_eq!(claimed.status, Status::InProgress);
            assert_eq!(claimed.owner.as_deref(), Some("agent-b"));
        });
    }

    #[test]
    fn create_validates_ids_and_dependencies() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            assert!(create(
                &ws,
                Some("bad id".into()),
                "x".into(),
                String::new(),
                Vec::new(),
                Vec::new()
            )
            .is_err());
            assert!(create(
                &ws,
                Some("a".into()),
                "x".into(),
                String::new(),
                vec!["missing".into()],
                Vec::new()
            )
            .is_err());

            let created = create(
                &ws,
                Some("a".into()),
                "x".into(),
                String::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("create");
            assert_eq!(created.id, "a");
            assert!(create(
                &ws,
                Some("a".into()),
                "again".into(),
                String::new(),
                Vec::new(),
                Vec::new()
            )
            .is_err());
        });
    }

    #[test]
    fn completing_requires_a_claim_and_the_right_owner() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            let task = create(
                &ws,
                None,
                "work".into(),
                String::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("create");
            assert!(complete(&ws, &task.id, None, None).is_err(), "not claimed");
            claim(&ws, &task.id, "agent-a").expect("claim");
            assert!(complete(&ws, &task.id, Some("agent-b"), None).is_err());
            let (done, _) = complete(&ws, &task.id, Some("agent-a"), Some("finished".into()))
                .expect("complete");
            assert_eq!(done.status, Status::Done);
            assert_eq!(done.note.as_deref(), Some("finished"));
            assert!(
                complete(&ws, &task.id, Some("agent-a"), None).is_err(),
                "already done"
            );
        });
    }

    #[test]
    fn stores_are_scoped_to_one_workspace_and_survive_on_disk() {
        with_isolated_home(|| {
            let (_a, ws_a) = workspace();
            let (_b, ws_b) = workspace();
            create(
                &ws_a,
                None,
                "only a".into(),
                String::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("create");
            assert_eq!(list(&ws_a).expect("list").len(), 1);
            assert!(list(&ws_b).expect("list").is_empty());

            let data = data_path(&ws_a);
            assert!(data.exists(), "the store is on disk");
            let raw = std::fs::read_to_string(&data).expect("read");
            assert!(raw.contains("only a"), "{raw}");
            assert!(raw.contains(&ws_a.to_string_lossy().to_string()), "{raw}");
            assert!(
                !data_path(&ws_b).exists(),
                "reading another workspace does not create a store"
            );
        });
    }

    #[test]
    fn a_corrupt_or_foreign_store_is_reported_not_reset() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            create(
                &ws,
                None,
                "work".into(),
                String::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("create");
            let data = data_path(&ws);

            std::fs::write(&data, "{not json").expect("corrupt");
            let err = list(&ws).expect_err("corrupt store");
            assert!(err.contains("unreadable"), "{err}");
            assert_eq!(
                std::fs::read_to_string(&data).expect("read"),
                "{not json",
                "a corrupt store is left for the operator, not reset"
            );

            let foreign = Store {
                workspace: "/some/other/project".to_string(),
                next_id: 2,
                tasks: BTreeMap::new(),
            };
            std::fs::write(&data, serde_json::to_string(&foreign).expect("write")).expect("write");
            let err = list(&ws).expect_err("foreign store");
            assert!(err.contains("another workspace"), "{err}");
        });
    }

    #[test]
    fn render_lists_status_dependencies_and_owners() {
        with_isolated_home(|| {
            let (_dir, ws) = workspace();
            let first = create(
                &ws,
                None,
                "first".into(),
                String::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("create");
            create(
                &ws,
                None,
                "second".into(),
                String::new(),
                vec![first.id.clone()],
                Vec::new(),
            )
            .expect("create");
            claim(&ws, &first.id, "agent-a").expect("claim");
            let text = render(&ws).expect("render");
            assert!(text.contains("[~]"), "{text}");
            assert!(text.contains("[ ]"), "{text}");
            assert!(text.contains("after task-1"), "{text}");
            assert!(text.contains("agent-a"), "{text}");
        });
    }

    /// The OS primitive the store relies on: two handles to one lock file
    /// exclude each other until the holder unlocks.
    #[test]
    fn the_lock_file_excludes_a_second_handle() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("store.lock");
        let first = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("first");
        let second = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("second");
        first.lock().expect("lock first");
        assert!(
            second.try_lock().is_err(),
            "a second handle must not get the lock"
        );
        first.unlock().expect("unlock");
        assert!(
            second.try_lock().is_ok(),
            "the lock is available after release"
        );
    }
}
