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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(1);

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
    fn a_repository_without_commits_is_refused_clearly() {
        let dir = tempfile::tempdir().expect("temp dir");
        git(dir.path(), &["init", "-q"]).expect("git init");
        let base = tempfile::tempdir().expect("base");
        let error = Worktree::create_in(base.path(), dir.path(), "worker")
            .expect_err("no commits, no worktree");
        assert!(error.contains("no commits"), "{error}");
    }
}
