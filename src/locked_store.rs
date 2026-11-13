/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! A JSON document per workspace, mutated under an OS file lock.
//!
//! Shared by the task DAG and the mailbox: both are coordination state that
//! several processes working in one workspace must agree on. The pattern is a
//! stable `<hash>.lock` file beside a `<hash>.json` document. Locking the
//! separate file rather than the document is what makes replacement safe: a
//! waiter always locks the same inode, then opens the document path fresh, so
//! swapping the document by rename cannot hand it a stale copy.
//!
//! Every operation rereads under the lock, mutates in memory, writes a temp
//! file, and renames it over the document. A read-only operation (`list`,
//! `inbox`) takes the same lock -- it must not observe a half-committed
//! document -- but never writes or creates one.
//!
//! A corrupt document, or one recording a different workspace, is reported
//! rather than reset: resetting would silently free claims another agent is
//! working under, or discard messages nobody read.

use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// One kind of shared document ("tasks", "mailbox") is one directory under the
/// config dir, and one `PROCESS_LOCK` serializes every kind in this process.
/// One global mutex rather than one per kind: no operation nests two stores,
/// so this cannot deadlock and leaves no lock-ordering rule to remember.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

/// What a stored document must expose for the shared workspace check.
pub trait Document: Serialize + DeserializeOwned {
    fn workspace(&self) -> &str;
    fn set_workspace(&mut self, workspace: String);
}

fn store_dir(kind: &str) -> PathBuf {
    crate::config::Config::config_dir().join(kind)
}

fn key(workspace: &Path) -> String {
    let digest = Sha256::digest(workspace.to_string_lossy().as_bytes());
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()[..16]
        .to_string()
}

/// The document path for a workspace. Public within the crate so the stores'
/// tests can assert what is actually on disk, and so a future migration can
/// find the files without repeating the hash.
pub fn data_path(kind: &str, workspace: &Path) -> PathBuf {
    store_dir(kind).join(format!("{}.json", key(workspace)))
}

fn lock_path(kind: &str, workspace: &Path) -> PathBuf {
    store_dir(kind).join(format!("{}.lock", key(workspace)))
}

/// A held exclusive lock. Dropping it unlocks explicitly and then closes the
/// file, so release does not depend on close-time semantics alone.
pub struct FileLock {
    file: std::fs::File,
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Take an exclusive OS lock on `path`, creating the file and its parent if
/// needed. The lock is released when the returned guard is dropped.
///
/// For mutual exclusion around work that is not a stored JSON document --
/// notably `git apply` against a checkout, which two covexcode processes in
/// one repository must not interleave. A process already serializes its own
/// applies with a local mutex; this covers the other processes. The caller
/// chooses the path, so the lock can live beside the thing it protects (the
/// repository's git dir) rather than in the config dir.
pub fn lock_exclusive(path: &Path) -> Result<FileLock, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("could not create {}: {e}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("could not open {}: {e}", path.display()))?;
    file.lock()
        .map_err(|e| format!("could not lock {}: {e}", path.display()))?;
    Ok(FileLock { file })
}

/// Read, mutate, and (when `write`) replace the document for `workspace`.
///
/// `init` builds an empty document; it receives the workspace label so the
/// document can record which workspace it belongs to. `f` sees the document
/// only while the lock is held; returning `Err` leaves the file untouched.
pub fn update<T, R>(
    kind: &str,
    workspace: &Path,
    write: bool,
    init: impl FnOnce(String) -> T,
    f: impl FnOnce(&mut T) -> Result<R, String>,
) -> Result<R, String>
where
    T: Document,
{
    let _process = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = store_dir(kind);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("could not create {}: {e}", dir.display()))?;

    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path(kind, workspace))
        .map_err(|e| format!("could not open the {kind} lock: {e}"))?;
    lock.lock()
        .map_err(|e| format!("could not lock the {kind} store: {e}"))?;

    let path = data_path(kind, workspace);
    let label = workspace.to_string_lossy().into_owned();
    let mut document = if path.exists() {
        let mut file = std::fs::File::open(&path)
            .map_err(|e| format!("could not read {}: {e}", path.display()))?;
        let mut text = String::new();
        file.read_to_string(&mut text)
            .map_err(|e| format!("could not read {}: {e}", path.display()))?;
        let mut document: T = serde_json::from_str(&text).map_err(|e| {
            format!(
                "the {kind} store {} is unreadable ({e}); fix or remove it",
                path.display()
            )
        })?;
        if !document.workspace().is_empty() && document.workspace() != label {
            return Err(format!(
                "the {kind} store {} belongs to another workspace ({}); refusing to mix them",
                path.display(),
                document.workspace()
            ));
        }
        document.set_workspace(label);
        document
    } else {
        init(label)
    };

    let result = f(&mut document);
    if result.is_ok() && write {
        let json = serde_json::to_string_pretty(&document)
            .map_err(|e| format!("could not serialize the {kind} store: {e}"))?;
        // A per-process name is enough: PROCESS_LOCK means only one thread in
        // this process writes at a time, and the OS lock excludes other
        // processes. A stale temp from a crashed process is inert.
        let tmp = dir.join(format!(
            "{}.json.tmp.{}",
            key(workspace),
            std::process::id()
        ));
        std::fs::write(&tmp, json)
            .map_err(|e| format!("could not write {}: {e}", tmp.display()))?;
        // Replace in one step: Unix rename and Windows `MoveFileEx` with
        // `MOVEFILE_REPLACE_EXISTING` both swap an existing destination, so a
        // crash leaves either the old document or the new one, never neither.
        std::fs::rename(&tmp, &path)
            .map_err(|e| format!("could not replace {}: {e}", path.display()))?;
    }
    let _ = lock.unlock();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The OS primitive the stores rely on: two handles to one lock file
    /// exclude each other until the holder unlocks. This is what makes a
    /// claim exclusive across processes, not just across threads.
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

    #[test]
    fn lock_exclusive_creates_parents_and_holds_the_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("nested").join("apply.lock");
        let held = lock_exclusive(&path).expect("lock");
        assert!(path.exists(), "the lock file is created");

        let second = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("second handle");
        assert!(
            second.try_lock().is_err(),
            "another handle must not acquire the same checkout lock"
        );
        drop(second);
        drop(held);
        let third = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("third handle");
        let third_lock = third.try_lock();
        assert!(
            third_lock.is_ok(),
            "dropping the guard releases the lock: {third_lock:?}"
        );
    }
}
