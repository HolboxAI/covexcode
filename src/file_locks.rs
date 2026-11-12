//! Process-wide per-path write locks.
//!
//! The tool schedulers already keep same-file writes in one batch ordered, but
//! they know nothing about each other: two sessions (a TUI session and an ACP
//! client, or two attached clients' sessions) sharing a workspace can still
//! interleave one file's read-modify-write. These locks are the shared floor
//! under both runners: writers to the same resolved path serialize, writers to
//! different paths never meet.
//!
//! Held only around the actual write/edit execution, never across an approval
//! wait or an LLM round -- a lock held while asking the user would block an
//! unrelated session for an unbounded time.
//!
//! The registry holds one small entry per distinct path written in this
//! process. That is bounded by the files a person's sessions actually touch,
//! not by time; a daemon writing unboundedly many paths would want an LRU.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, OwnedMutexGuard};

static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<PathBuf, Arc<Mutex<()>>>> {
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Acquire the lock for one resolved path. The guard is `Send`, so it can be
/// held across an `.await`; it releases on drop.
pub async fn lock(path: PathBuf) -> OwnedMutexGuard<()> {
    let cell = {
        let mut map = registry().lock().await;
        map.entry(path)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    cell.lock_owned().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn same_path_writers_serialize_and_different_paths_do_not() {
        let path = std::env::temp_dir().join(format!(
            "covexcode-file-lock-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let inside = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let inside = inside.clone();
            tasks.push(tokio::spawn(async move {
                let _guard = lock(path).await;
                assert_eq!(
                    inside.fetch_add(1, Ordering::SeqCst),
                    0,
                    "two writers held the same path at once"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
                inside.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for task in tasks {
            task.await.expect("writer finished");
        }

        // Different paths have different locks: both can be held at once.
        let first = lock(path.with_extension("a")).await;
        let second =
            tokio::time::timeout(Duration::from_millis(200), lock(path.with_extension("b")))
                .await
                .expect("a different path is not blocked by this one");
        drop((first, second));
    }
}
