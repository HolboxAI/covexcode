//! Isolated git worktrees for write-capable subagents.
//!
//! A writing child gets its own checkout of the repository at HEAD, does its
//! work there, and hands back a patch. The requester's files are untouched
//! until somebody applies that patch, which is what makes parallel children
//! safe to run and a bad child safe to discard.
//!
//! Deliberately not a general merge engine. `patch` stages everything
//! (including new files) and asks git for a binary-safe diff; applying it is
//! the caller's decision. When a patch no longer applies exactly, [`apply`]
//! makes one three-way attempt against the current checkout, because two
//! independent agents commonly edit the same file in different places. If
//! that merge conflicts too, the checkout is restored and the conflict is
//! reported -- resolution stays a decision, not a guess. A worktree is only
//! removed by [`Worktree::remove`] -- abnormal exits leave it in place with
//! its path in the report, because losing a child's uncommitted work to a
//! cleanup step would be worse than a stale directory.
//!
//! Requires a git repository with at least one commit. A project without git
//! gets a clear refusal instead of a half-working isolation story.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Patches are applied to the requester's checkout one at a time, even when
/// several writing children finish together: `git apply` is not safe to run
/// concurrently against one working tree.
static APPLY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// What happened when a patch was offered to the requester's checkout.
#[derive(Debug, PartialEq)]
pub enum Applied {
    /// `--check` passed and the patch was applied.
    Clean,
    /// The patch did not apply exactly, but a three-way merge against the
    /// current checkout resolved it and the merged files were written.
    Merged,
    /// The patch does not apply to the current tree; the reason is git's own
    /// wording. Nothing was written.
    Conflict(String),
}

#[derive(Debug)]
pub struct Worktree {
    path: PathBuf,
    repo: PathBuf,
}

pub struct Patch {
    /// Repo-relative paths, as `git diff --name-only` reports them.
    pub files: Vec<String>,
    /// `git diff --cached --binary`: applies with `git apply`.
    pub diff: String,
}

impl Worktree {
    /// Create a detached worktree of `workspace_root`'s repository at HEAD.
    ///
    /// The directory lives under the covexcode state dir, not inside the
    /// project, so it never shows up in the user's own `git status`.
    pub fn create(workspace_root: &Path, label: &str) -> Result<Self, String> {
        Self::create_in(
            &crate::config::Config::config_dir().join("worktrees"),
            workspace_root,
            label,
        )
    }

    fn create_in(base: &Path, workspace_root: &Path, label: &str) -> Result<Self, String> {
        let repo = git(workspace_root, &["rev-parse", "--show-toplevel"])?;
        let repo = PathBuf::from(repo.trim());
        if git(&repo, &["rev-parse", "--verify", "HEAD"]).is_err() {
            return Err(
                "the repository has no commits yet, so an isolated worktree cannot be created"
                    .to_string(),
            );
        }
        std::fs::create_dir_all(base)
            .map_err(|e| format!("could not create {}: {e}", base.display()))?;
        let safe: String = label
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '-'
                }
            })
            .take(40)
            .collect();
        let path = base.join(format!(
            "{}-{}-{safe}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path_arg = path
            .to_str()
            .ok_or_else(|| "the worktree path is not valid UTF-8".to_string())?;
        git(&repo, &["worktree", "add", "--detach", path_arg, "HEAD"])?;
        Ok(Self { path, repo })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Stage everything and return the staged diff, including new files.
    /// Staging is confined to this worktree's own index.
    ///
    /// Rename detection is off so the patch names both the old and the new
    /// path: [`apply`](Self::apply)'s three-way fallback snapshots every path
    /// a patch touches, and a rename record names only its destination.
    pub fn patch(&self) -> Result<Patch, String> {
        git(&self.path, &["add", "-A"])?;
        let files = git(
            &self.path,
            &["diff", "--cached", "--no-renames", "--name-only"],
        )?;
        let diff = git(
            &self.path,
            &["diff", "--cached", "--no-renames", "--binary"],
        )?;
        Ok(Patch {
            files: files
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_string)
                .collect(),
            diff,
        })
    }

    /// Offer this worktree's patch to the requester's checkout. Serialized
    /// process-wide by `APPLY_LOCK` and across processes by a lock file in the
    /// repository's git dir: two covexcode processes in one checkout must not
    /// interleave `git apply` (or the three-way snapshot/restore).
    ///
    /// A plain `--check` is tried first because it is exact and never writes.
    /// If it fails, a three-way merge is attempted against the current
    /// working tree, still without touching the real index: a temporary index
    /// is synced to the checkout, and every path the patch touches is
    /// snapshotted first. A clean merge reports [`Applied::Merged`]; any
    /// conflict restores the snapshots, so the checkout is left exactly as it
    /// was and the conflict is reported instead.
    pub fn apply(&self, diff: &str) -> Result<Applied, String> {
        let _guard = APPLY_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _checkout = self.checkout_lock()?;
        let plain = git_stdin(&self.repo, &["apply", "--check", "--binary", "-"], diff);
        if let Err(conflict) = plain {
            return self.apply_three_way(diff, conflict);
        }
        git_stdin(&self.repo, &["apply", "--binary", "-"], diff)
            .map_err(|e| format!("git apply failed after --check passed: {e}"))?;
        Ok(Applied::Clean)
    }

    /// The cross-process apply lock for this repository's checkout. It lives
    /// in the common git dir, so every worktree of the same repository (and
    /// every process using any of them) locks the same file.
    fn checkout_lock(&self) -> Result<crate::locked_store::FileLock, String> {
        let common = git(&self.repo, &["rev-parse", "--git-common-dir"])?;
        let common = PathBuf::from(common.trim());
        let common = if common.is_absolute() {
            common
        } else {
            self.repo.join(common)
        };
        crate::locked_store::lock_exclusive(&common.join("covexcode-apply.lock"))
    }

    fn apply_three_way(&self, diff: &str, plain_conflict: String) -> Result<Applied, String> {
        let paths = match patch_paths(&self.repo, diff) {
            Ok(paths) => paths,
            Err(e) => {
                return Ok(Applied::Conflict(format!(
                    "{plain_conflict}\nA three-way merge could not be attempted: {e}"
                )))
            }
        };
        if paths.is_empty() {
            return Ok(Applied::Conflict(plain_conflict));
        }
        // Snapshot before merging: a failed three-way leaves conflict markers
        // in the files it touched, and those must not survive into the
        // requester's checkout.
        let snapshots: Vec<(PathBuf, Option<Vec<u8>>)> = paths
            .iter()
            .map(|relative| {
                let path = self.repo.join(relative);
                let content = std::fs::read(&path).ok();
                (path, content)
            })
            .collect();

        let Some(index) = TempIndex::new() else {
            return Ok(Applied::Conflict(format!(
                "{plain_conflict}\nA three-way merge could not prepare an index."
            )));
        };
        let merge = (|| -> Result<(), String> {
            // The temporary index must describe the current working tree,
            // not HEAD: `git apply --3way` refuses to merge against an index
            // that does not match the files it is about to change.
            git_with_index(&self.repo, &["read-tree", "HEAD"], None, index.path())?;
            git_with_index(&self.repo, &["add", "-A"], None, index.path())?;
            git_with_index(
                &self.repo,
                &["apply", "--3way", "--binary", "-"],
                Some(diff),
                index.path(),
            )?;
            Ok(())
        })();

        match merge {
            Ok(()) => Ok(Applied::Merged),
            Err(merge_error) => {
                let unrestored = restore(&snapshots);
                let mut reason = format!(
                    "{plain_conflict}\nA three-way merge did not resolve it: {merge_error}"
                );
                if !unrestored.is_empty() {
                    reason.push_str(&format!(
                        "\nSome files could not be restored automatically: {}",
                        unrestored.join(", ")
                    ));
                }
                Ok(Applied::Conflict(reason))
            }
        }
    }

    /// Run a definition's `verify` command inside this worktree, before its
    /// patch is offered to the requester. `Ok(output)` means exit 0; `Err` is
    /// the failure text (non-zero exit, timeout, or spawn error). The command
    /// is project/user-supplied and runs with the user's own permissions --
    /// the same trust boundary as `.covexcode/agents` prompts themselves.
    pub async fn verify(&self, command: &str, timeout: std::time::Duration) -> Result<String, String> {
        let (shell, flag) = if cfg!(windows) {
            ("cmd", "/C")
        } else {
            ("sh", "-c")
        };
        let mut cmd = tokio::process::Command::new(shell);
        cmd.arg(flag)
            .arg(command)
            .current_dir(&self.path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        match tokio::time::timeout(timeout, cmd.output()).await {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                let mut text = String::new();
                if !stdout.trim().is_empty() {
                    text.push_str(stdout.trim_end());
                }
                if !stderr.trim().is_empty() {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(stderr.trim_end());
                }
                if output.status.success() {
                    Ok(text)
                } else {
                    Err(format!(
                        "exit {}\n{text}",
                        output
                            .status
                            .code()
                            .map(|code| code.to_string())
                            .unwrap_or_else(|| "killed".to_string())
                    ))
                }
            }
            Ok(Err(e)) => Err(format!("could not run the verify command: {e}")),
            Err(_) => Err(format!(
                "timed out after {}s",
                timeout.as_secs()
            )),
        }
    }

    /// Best effort: a worktree already gone, or a repo that moved, must not
    /// turn a completed child into an error.
    pub fn remove(&self) {
        if let Some(path) = self.path.to_str() {
            let _ = git(&self.repo, &["worktree", "remove", "--force", path]);
        }
        let _ = git(&self.repo, &["worktree", "prune"]);
    }
}

/// Run one git command in `cwd`, returning stdout on success and stderr on
/// failure. No shell: every argument is passed as-is.
fn git(cwd: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .output()
        .map_err(|e| format!("could not run git: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        return Err(if message.is_empty() {
            format!("git {} failed", args.join(" "))
        } else {
            message.to_string()
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Same, with `input` written to git's stdin (for `git apply -`).
fn git_stdin(cwd: &Path, args: &[&str], input: &str) -> Result<String, String> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not run git: {e}"))?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(input.as_bytes())
            .map_err(|e| format!("could not send the patch to git: {e}"))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|e| format!("git did not finish: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        return Err(if message.is_empty() {
            format!("git {} failed", args.join(" "))
        } else {
            message.to_string()
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Paths a patch touches, from git's NUL-separated numstat records.
///
/// `-z` keeps spaces and other bytes literal; without it git C-quotes paths,
/// and the snapshot/restore step would write to the wrong file.
fn patch_paths(repo: &Path, diff: &str) -> Result<Vec<PathBuf>, String> {
    let output = git_stdin(repo, &["apply", "--numstat", "-z", "-"], diff)?;
    let mut paths = Vec::new();
    for record in output.split('\0') {
        if record.is_empty() {
            continue;
        }
        let Some(path) = record.splitn(3, '\t').nth(2) else {
            continue;
        };
        let path = PathBuf::from(path);
        // The patch comes from our own worktree, but the snapshot step joins
        // these paths onto the checkout: never let one escape it.
        if path.is_absolute()
            || path
                .components()
                .any(|component| component == std::path::Component::ParentDir)
        {
            return Err(format!(
                "refusing a patch path outside the repository: {}",
                path.display()
            ));
        }
        paths.push(path);
    }
    Ok(paths)
}

/// A throwaway index for the three-way attempt. The user's real index is
/// never touched: syncing it to the working tree would stage their changes.
struct TempIndex {
    path: PathBuf,
}

impl TempIndex {
    fn new() -> Option<Self> {
        let path = std::env::temp_dir().join(format!(
            "covexcode-index-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .ok()?;
        Some(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempIndex {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// `git`, with one command's index redirected and optional stdin.
fn git_with_index(
    cwd: &Path,
    args: &[&str],
    input: Option<&str>,
    index: &Path,
) -> Result<String, String> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args(args)
        .env("GIT_INDEX_FILE", index)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not run git: {e}"))?;
    if let (Some(stdin), Some(input)) = (child.stdin.as_mut(), input) {
        stdin
            .write_all(input.as_bytes())
            .map_err(|e| format!("could not send the patch to git: {e}"))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|e| format!("git did not finish: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let message = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        return Err(if message.is_empty() {
            format!("git {} failed", args.join(" "))
        } else {
            message.to_string()
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Put the snapshotted files back after a failed merge. Returns the paths
/// that could not be restored, so the caller can say so rather than claim a
/// clean rollback.
fn restore(snapshots: &[(PathBuf, Option<Vec<u8>>)]) -> Vec<String> {
    let mut failures = Vec::new();
    for (path, content) in snapshots {
        let result = match content {
            Some(bytes) => std::fs::write(path, bytes),
            None => match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            },
        };
        if let Err(e) = result {
            failures.push(format!("{} ({e})", path.display()));
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().to_path_buf();
        git(&root, &["init", "-q"]).expect("git init");
        git(&root, &["config", "user.email", "test@example.com"]).expect("email");
        git(&root, &["config", "user.name", "Test"]).expect("name");
        std::fs::write(root.join("tracked.txt"), "before\n").expect("write");
        git(&root, &["add", "tracked.txt"]).expect("add");
        git(&root, &["commit", "-qm", "initial"]).expect("commit");
        (dir, root)
    }

    #[test]
    fn a_worktree_captures_changes_without_touching_the_repo() {
        let (_dir, root) = repo();
        let base = tempfile::tempdir().expect("base");
        let worktree = Worktree::create_in(base.path(), &root, "worker").expect("worktree");

        std::fs::write(worktree.path().join("tracked.txt"), "after\n").expect("edit");
        std::fs::write(worktree.path().join("new.txt"), "created\n").expect("create");

        let patch = worktree.patch().expect("patch");
        assert_eq!(patch.files.len(), 2, "{:?}", patch.files);
        assert!(patch.diff.contains("tracked.txt"), "{}", patch.diff);
        assert!(patch.diff.contains("new.txt"), "{}", patch.diff);
        assert_eq!(
            std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
            "before\n",
            "the requester's checkout is untouched"
        );
        assert!(!root.join("new.txt").exists());

        worktree.remove();
        assert!(!worktree.path().exists(), "the worktree is gone");
    }

    #[test]
    fn a_clean_patch_applies_to_the_repository() {
        let (_dir, root) = repo();
        let base = tempfile::tempdir().expect("base");
        let worktree = Worktree::create_in(base.path(), &root, "worker").expect("worktree");
        std::fs::write(worktree.path().join("tracked.txt"), "after\n").expect("edit");
        std::fs::write(worktree.path().join("new.txt"), "created\n").expect("create");
        let patch = worktree.patch().expect("patch");

        assert_eq!(worktree.apply(&patch.diff), Ok(Applied::Clean));
        assert_eq!(
            std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
            "after\n"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("new.txt")).unwrap(),
            "created\n"
        );
        worktree.remove();
    }

    /// Two agents editing nearby lines of one file is the common collision:
    /// the exact patch no longer applies, but a three-way merge against the
    /// requester's checkout keeps both edits. Adjacent changed lines are a
    /// genuine overlap and still conflict; one line of separation merges.
    #[test]
    fn a_patch_near_a_concurrent_edit_is_three_way_merged() {
        let (_dir, root) = repo();
        let base_text = "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nten\n";
        std::fs::write(root.join("tracked.txt"), base_text).expect("rewrite");
        git(&root, &["add", "tracked.txt"]).expect("add");
        git(&root, &["commit", "-qm", "lines"]).expect("commit");

        let base = tempfile::tempdir().expect("base");
        let worktree = Worktree::create_in(base.path(), &root, "worker").expect("worktree");
        std::fs::write(
            worktree.path().join("tracked.txt"),
            base_text.replace("two", "TWO"),
        )
        .expect("child edit");
        let patch = worktree.patch().expect("patch");

        std::fs::write(root.join("tracked.txt"), base_text.replace("four", "FOUR"))
            .expect("concurrent nearby edit");

        assert_eq!(worktree.apply(&patch.diff), Ok(Applied::Merged));
        let merged = std::fs::read_to_string(root.join("tracked.txt")).unwrap();
        assert!(
            merged.contains("TWO"),
            "the child's edit survived: {merged}"
        );
        assert!(
            merged.contains("FOUR"),
            "the requester's edit survived: {merged}"
        );
        worktree.remove();
    }

    /// A patch that no longer applies writes nothing at all -- `--check` is
    /// what makes the conflict safe rather than partial.
    #[test]
    fn a_conflicting_patch_is_reported_and_writes_nothing() {
        let (_dir, root) = repo();
        let base = tempfile::tempdir().expect("base");
        let worktree = Worktree::create_in(base.path(), &root, "worker").expect("worktree");
        std::fs::write(worktree.path().join("tracked.txt"), "after\n").expect("edit");
        std::fs::write(worktree.path().join("new.txt"), "created\n").expect("create");
        let patch = worktree.patch().expect("patch");

        std::fs::write(root.join("tracked.txt"), "someone else\n").expect("concurrent edit");
        match worktree.apply(&patch.diff) {
            Ok(Applied::Conflict(_)) => {}
            other => panic!("expected a conflict, got {other:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
            "someone else\n",
            "the conflicting file is untouched"
        );
        assert!(
            !root.join("new.txt").exists(),
            "the new file from a rejected patch is not created"
        );
        worktree.remove();
    }

    /// The snapshot/restore path reads paths from `--numstat -z`; a space in
    /// a filename must not turn the restore into a no-op or the wrong file.
    #[test]
    fn a_conflict_in_a_path_with_spaces_restores_that_file() {
        let (_dir, root) = repo();
        let spaced = root.join("sp ace.txt");
        std::fs::write(&spaced, "before\n").expect("write");
        git(&root, &["add", "sp ace.txt"]).expect("add");
        git(&root, &["commit", "-qm", "spaced"]).expect("commit");

        let base = tempfile::tempdir().expect("base");
        let worktree = Worktree::create_in(base.path(), &root, "worker").expect("worktree");
        std::fs::write(worktree.path().join("sp ace.txt"), "after\n").expect("edit");
        let patch = worktree.patch().expect("patch");

        std::fs::write(&spaced, "someone else\n").expect("concurrent edit");
        match worktree.apply(&patch.diff) {
            Ok(Applied::Conflict(_)) => {}
            other => panic!("expected a conflict, got {other:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(&spaced).unwrap(),
            "someone else\n",
            "the file the merge touched was restored exactly"
        );
        worktree.remove();
    }

    /// A verify command runs with the worktree as its cwd, and a failure
    /// carries git's/command's own output back to the caller.
    #[cfg(unix)]
    #[tokio::test]
    async fn verify_runs_in_the_worktree_and_reports_failure() {
        let (_dir, root) = repo();
        let base = tempfile::tempdir().expect("base");
        let worktree = Worktree::create_in(base.path(), &root, "worker").expect("worktree");
        std::fs::write(worktree.path().join("marker.txt"), "x").expect("write");

        assert!(worktree
            .verify("test -f marker.txt", std::time::Duration::from_secs(5))
            .await
            .is_ok());
        let failure = worktree
            .verify(
                "echo nope >&2; exit 3",
                std::time::Duration::from_secs(5),
            )
            .await
            .expect_err("a non-zero exit is a failure");
        assert!(failure.contains("nope"), "{failure}");
        assert!(failure.contains("exit 3"), "{failure}");
        worktree.remove();
    }

    #[test]
    fn a_repository_without_commits_is_refused_clearly() {
        let dir = tempfile::tempdir().expect("temp dir");
        git(dir.path(), &["init", "-q"]).expect("git init");
        let base = tempfile::tempdir().expect("base");
        let error = Worktree::create_in(base.path(), dir.path(), "worker")
            .expect_err("no commits, no worktree");
        assert!(error.contains("no commits"), "{error}");
    }
}
