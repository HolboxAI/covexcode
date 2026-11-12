//! A shared, process-wide task DAG for multi-agent work.
//!
//! The parent model (or a person) creates tasks with dependencies; an agent
//! claims one before working on it and completes it when done. Claims are
//! atomic under one mutex, so two agents looking at the same pending task
//! cannot both start it -- the plan's `UPDATE ... WHERE status='pending'`
//! guarantee, without a database.
//!
//! Scope is one workspace in one process. That is where the TUI, the ACP
//! engine, and every subagent they spawn already live; a cross-process daemon
//! would need the same claim operation backed by a file lock, which is a
//! deliberate follow-up rather than a silent second implementation here.
//!
//! The store is in memory. It is coordination state for work in flight, not
//! the session transcript: a restart loses the list, and the conversation that
//! created it is still the record of what was meant.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

struct Store {
    next_id: u64,
    tasks: BTreeMap<String, Task>,
}

impl Store {
    fn new() -> Self {
        Self {
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

static STORES: OnceLock<Mutex<HashMap<PathBuf, Store>>> = OnceLock::new();

fn stores() -> &'static Mutex<HashMap<PathBuf, Store>> {
    STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn key(workspace: &Path) -> PathBuf {
    workspace.to_path_buf()
}

fn with_store<T>(workspace: &Path, f: impl FnOnce(&mut Store) -> T) -> T {
    let mut map = stores().lock().unwrap_or_else(|e| e.into_inner());
    let store = map.entry(key(workspace)).or_insert_with(Store::new);
    f(store)
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
    with_store(workspace, |store| {
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

/// Claim a pending task. Atomic: only one caller gets `Ok` for a given task
/// unless it is the same owner re-claiming its own in-progress work.
pub fn claim(workspace: &Path, id: &str, owner: &str) -> Result<Task, String> {
    let owner = owner.trim();
    if owner.is_empty() {
        return Err("a claim needs an owner name".to_string());
    }
    with_store(workspace, |store| {
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
                if blocked.len() == 1 { "it is" } else { "they are" }
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
    with_store(workspace, |store| {
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
            .filter(|candidate| store.deps_done(candidate).is_ok())
            .map(|candidate| candidate.id.clone())
            .collect();
        Ok((completed, unblocked))
    })
}

pub fn list(workspace: &Path) -> Vec<Task> {
    with_store(workspace, |store| store.tasks.values().cloned().collect())
}

/// The text the model sees for `tasks { action: "list" }`.
pub fn render(workspace: &Path) -> String {
    let tasks = list(workspace);
    if tasks.is_empty() {
        return "No tasks yet.".to_string();
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
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().to_path_buf();
        (dir, path)
    }

    #[test]
    fn a_claim_is_atomic_and_dependencies_block_it() {
        let (_dir, ws) = workspace();
        let first = create(&ws, None, "first".into(), String::new(), Vec::new(), Vec::new())
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
    }

    #[test]
    fn create_validates_ids_and_dependencies() {
        let (_dir, ws) = workspace();
        assert!(create(&ws, Some("bad id".into()), "x".into(), String::new(), Vec::new(), Vec::new()).is_err());
        assert!(create(&ws, Some("a".into()), "x".into(), String::new(), vec!["missing".into()], Vec::new()).is_err());

        let created = create(&ws, Some("a".into()), "x".into(), String::new(), Vec::new(), Vec::new()).expect("create");
        assert_eq!(created.id, "a");
        assert!(create(&ws, Some("a".into()), "again".into(), String::new(), Vec::new(), Vec::new()).is_err());
    }

    #[test]
    fn completing_requires_a_claim_and_the_right_owner() {
        let (_dir, ws) = workspace();
        let task = create(&ws, None, "work".into(), String::new(), Vec::new(), Vec::new()).expect("create");
        assert!(complete(&ws, &task.id, None, None).is_err(), "not claimed");
        claim(&ws, &task.id, "agent-a").expect("claim");
        assert!(complete(&ws, &task.id, Some("agent-b"), None).is_err());
        let (done, _) = complete(&ws, &task.id, Some("agent-a"), Some("finished".into())).expect("complete");
        assert_eq!(done.status, Status::Done);
        assert_eq!(done.note.as_deref(), Some("finished"));
        assert!(complete(&ws, &task.id, Some("agent-a"), None).is_err(), "already done");
    }

    #[test]
    fn stores_are_scoped_to_one_workspace() {
        let (_a, ws_a) = workspace();
        let (_b, ws_b) = workspace();
        create(&ws_a, None, "only a".into(), String::new(), Vec::new(), Vec::new()).expect("create");
        assert_eq!(list(&ws_a).len(), 1);
        assert!(list(&ws_b).is_empty());
    }

    #[test]
    fn render_lists_status_dependencies_and_owners() {
        let (_dir, ws) = workspace();
        let first = create(&ws, None, "first".into(), String::new(), Vec::new(), Vec::new()).expect("create");
        create(&ws, None, "second".into(), String::new(), vec![first.id.clone()], Vec::new()).expect("create");
        claim(&ws, &first.id, "agent-a").expect("claim");
        let text = render(&ws);
        assert!(text.contains("[~]"), "{text}");
        assert!(text.contains("[ ]"), "{text}");
        assert!(text.contains("after task-1"), "{text}");
        assert!(text.contains("agent-a"), "{text}");
    }
}
