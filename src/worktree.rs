//! Isolated git worktrees for write-capable subagents.
//!
//! A writing child gets its own checkout of the repository at HEAD, does its
//! work there, and hands back a patch. The requester's files are untouched
//! until somebody applies that patch, which is what makes parallel children
//! safe to run and a bad child safe to discard.
//!
//! Deliberately not a merge engine. `patch` stages everything (including new
//! files) and asks git for a binary-safe diff; applying it is the caller's
//! decision. A worktree is only removed by [`Worktree::remove`] -- abnormal
//! exits leave it in place with its path in the report, because losing a
//! child's uncommitted work to a cleanup step would be worse than a stale
//! directory.
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
    pub fn patch(&self) -> Result<Patch, String> {
        git(&self.path, &["add", "-A"])?;
        let files = git(&self.path, &["diff", "--cached", "--name-only"])?;
        let diff = git(&self.path, &["diff", "--cached", "--binary"])?;
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
    /// process-wide; `--check` first, so a conflict writes nothing at all.
    pub fn apply(&self, diff: &str) -> Result<Applied, String> {
        let _guard = APPLY_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(conflict) = git_stdin(&self.repo, &["apply", "--check", "--binary", "-"], diff) {
            return Ok(Applied::Conflict(conflict));
        }
        git_stdin(&self.repo, &["apply", "--binary", "-"], diff)
            .map_err(|e| format!("git apply failed after --check passed: {e}"))?;
        Ok(Applied::Clean)
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
