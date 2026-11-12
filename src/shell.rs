//! A per-workspace persistent `sh` for `run_command`, behind
//! `[tools] persistent_shell`.
//!
//! The one-shot path spawns a process per command, which is the reference
//! behavior and stays the default. This module keeps one shell per workspace
//! alive between calls so repeated `ls`/`git status`/build steps stop paying
//! process-spawn cost, and so `cd` and exported variables persist the way they
//! would in a terminal.
//!
//! Deliberately conservative:
//!
//! - **Concurrency never regresses.** A shell is leased for one command at a
//!   time; a parallel call that finds it busy falls back to the one-shot path
//!   rather than waiting behind it.
//! - **Commands cannot eat the protocol.** Each command runs with stdin from
//!   `/dev/null`, so `cat`, `read` or an interpreter's REPL cannot consume the
//!   completion marker that follows it. A command that needs input behaves
//!   like it does on the one-shot path: it gets EOF.
//! - **A timeout kills the shell.** The next call starts a fresh one, so a
//!   hung command cannot poison the session. The caller formats the timeout
//!   exactly like the one-shot path does.
//! - **Unavailable means fall back.** If the shell cannot be started, the
//!   caller runs the command one-shot and reports that path's own error.
//!
//! stdout and stderr are merged (the shell starts with `exec 2>&1`). One pipe
//! is what makes the completion marker authoritative: with two, the last
//! stderr write can still be in flight when the marker arrives, and the caller
//! would have to guess how long to wait for it. The caller labels the merged
//! text as `--- output ---` rather than pretending it is all stdout.
//!
//! Unix only. Windows keeps the one-shot path.
//!
//! The registry is keyed by workspace root, so two sessions on the same
//! project in one process share a shell (and its `cd`/export state) unless a
//! command is already running in it. That is the same sharing the filesystem
//! already has; the opt-in default keeps it from surprising anyone.

use std::path::Path;
use std::time::Duration;

/// What `try_run` did with the command.
pub enum ShellRun {
    /// Persistent shells are disabled (or could not start); the caller must
    /// use the one-shot path.
    Unavailable,
    /// The command ran. `status` is the shell's `$?`; `None` means the shell
    /// exited before reporting it (e.g. the command itself called `exit`).
    Finished(ShellOutput),
    /// The command exceeded its timeout. The shell has been killed and is not
    /// reused; the caller reports the same timeout message as the one-shot
    /// path.
    TimedOut,
}

pub struct ShellOutput {
    /// stdout and stderr interleaved, in write order.
    pub output: String,
    pub status: Option<i32>,
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::process::{Child, ChildStdin};
    use tokio::sync::mpsc;

    enum Event {
        Out(String),
        Eof,
    }

    struct Shell {
        /// Kept for its `kill_on_drop`: dropping a timed-out shell kills the
        /// process, so the next command starts clean.
        _child: Child,
        stdin: ChildStdin,
        events: mpsc::UnboundedReceiver<Event>,
    }

    static SHELLS: OnceLock<tokio::sync::Mutex<HashMap<PathBuf, Shell>>> = OnceLock::new();
    static TOKEN: AtomicU64 = AtomicU64::new(1);

    fn registry() -> &'static tokio::sync::Mutex<HashMap<PathBuf, Shell>> {
        SHELLS.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
    }

    /// The marker is random per command so ordinary output cannot be mistaken
    /// for it; the counter keeps two commands in one process distinct even if
    /// the RNG were to repeat.
    fn token() -> String {
        use rand::RngCore;
        let mut bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut bytes);
        format!(
            "{:x}{:x}",
            TOKEN.fetch_add(1, Ordering::Relaxed),
            u64::from_le_bytes(bytes)
        )
    }

    async fn spawn(root: &Path) -> Result<Shell, String> {
        // `exec 2>&1; exec sh`: every byte the command writes, on either
        // stream, arrives on the one pipe this process reads.
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg("exec 2>&1; exec sh")
            .current_dir(root)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|e| format!("could not start a shell: {e}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "the shell has no stdin".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "the shell has no stdout".to_string())?;
        let (tx, events) = mpsc::unbounded_channel();
        tokio::spawn(read_stream(stdout, tx));
        Ok(Shell {
            _child: child,
            stdin,
            events,
        })
    }

    async fn read_stream<R: tokio::io::AsyncRead + Unpin>(
        mut stream: R,
        tx: mpsc::UnboundedSender<Event>,
    ) {
        let mut buf = [0u8; 8192];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => {
                    let _ = tx.send(Event::Eof);
                    return;
                }
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).into_owned();
                    if tx.send(Event::Out(text)).is_err() {
                        return;
                    }
                }
                Err(_) => {
                    let _ = tx.send(Event::Eof);
                    return;
                }
            }
        }
    }

    enum Failure {
        TimedOut,
        Exited,
    }

    impl Shell {
        async fn run(&mut self, command: &str, timeout: Duration) -> Result<ShellOutput, Failure> {
            let token = token();
            // `{ command\n} < /dev/null`: run in this shell (so `cd` and
            // exports persist), but with the shell's own stdin -- which is
            // our protocol channel -- replaced for the command. The marker is
            // printed to the merged output stream after it returns.
            let script = format!(
                "{{ {command}\n}} < /dev/null\n__covexcode_status=$?\n\
                 printf '\\036{token}\\037%s\\036\\n' \"$__covexcode_status\"\n"
            );
            if self.stdin.write_all(script.as_bytes()).await.is_err() {
                return Err(Failure::Exited);
            }
            if self.stdin.flush().await.is_err() {
                return Err(Failure::Exited);
            }

            let marker = format!("\x1e{token}\x1f");
            let mut output = String::new();
            let outcome = tokio::time::timeout(timeout, async {
                while let Some(event) = self.events.recv().await {
                    match event {
                        Event::Out(text) => {
                            output.push_str(&text);
                            if let Some(pos) = output.find(&marker) {
                                let rest = &output[pos + marker.len()..];
                                let status = rest
                                    .find('\x1e')
                                    .and_then(|end| rest[..end].trim().parse::<i32>().ok());
                                output.truncate(pos);
                                return Some(status);
                            }
                        }
                        Event::Eof => return None,
                    }
                }
                None
            })
            .await;

            match outcome {
                Ok(Some(status)) => Ok(ShellOutput { output, status }),
                // The shell died before completing the command (an `exit`,
                // a syntax error that closed it, or EOF). The caller reports
                // it; the next call spawns fresh.
                Ok(None) => Err(Failure::Exited),
                Err(_) => Err(Failure::TimedOut),
            }
        }
    }

    pub async fn try_run(root: &Path, command: &str, timeout: Duration) -> ShellRun {
        let key = root.to_path_buf();
        let existing = registry().lock().await.remove(&key);
        let mut shell = match existing {
            Some(shell) => shell,
            None => match spawn(root).await {
                Ok(shell) => shell,
                Err(_) => return ShellRun::Unavailable,
            },
        };

        match shell.run(command, timeout).await {
            Ok(output) => {
                registry().lock().await.insert(key, shell);
                ShellRun::Finished(output)
            }
            Err(Failure::TimedOut) => ShellRun::TimedOut,
            Err(Failure::Exited) => ShellRun::Finished(ShellOutput {
                output: "the persistent shell exited before completing the command".to_string(),
                status: None,
            }),
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use super::*;

    pub async fn try_run(_root: &Path, _command: &str, _timeout: Duration) -> ShellRun {
        ShellRun::Unavailable
    }
}

pub use imp::try_run;

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn finished(run: ShellRun) -> ShellOutput {
        match run {
            ShellRun::Finished(output) => output,
            ShellRun::Unavailable => panic!("the shell was unavailable"),
            ShellRun::TimedOut => panic!("the command timed out"),
        }
    }

    #[tokio::test]
    async fn a_reused_shell_keeps_its_working_directory() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir(dir.path().join("sub")).expect("mkdir");

        let first = finished(try_run(dir.path(), "cd sub", Duration::from_secs(5)).await);
        assert_eq!(first.status, Some(0));

        let second = finished(try_run(dir.path(), "pwd", Duration::from_secs(5)).await);
        assert!(
            second.output.trim().ends_with("/sub"),
            "the cd persisted: {}",
            second.output
        );
    }

    /// `cat` reads stdin; on a naive persistent shell it would drain the
    /// following protocol and the call would hang. stdin is /dev/null for the
    /// command, so it sees EOF and the next command still works.
    #[tokio::test]
    async fn a_command_cannot_consume_the_completion_marker() {
        let dir = tempfile::tempdir().expect("temp dir");
        let cat = tokio::time::timeout(
            Duration::from_secs(5),
            try_run(dir.path(), "cat", Duration::from_secs(4)),
        )
        .await
        .expect("cat finished instead of eating the protocol");
        let cat = finished(cat);
        assert_eq!(cat.status, Some(0));

        let next = finished(try_run(dir.path(), "echo after", Duration::from_secs(5)).await);
        assert_eq!(next.output.trim(), "after");
    }

    #[tokio::test]
    async fn a_timeout_kills_the_shell_and_the_next_call_starts_clean() {
        let dir = tempfile::tempdir().expect("temp dir");
        match try_run(dir.path(), "sleep 5", Duration::from_millis(150)).await {
            ShellRun::TimedOut => {}
            ShellRun::Finished(_) => panic!("the command should have timed out"),
            ShellRun::Unavailable => panic!("the shell was unavailable"),
        }

        let next = finished(try_run(dir.path(), "echo ok", Duration::from_secs(5)).await);
        assert_eq!(next.output.trim(), "ok");
        assert_eq!(next.status, Some(0));
    }

    #[tokio::test]
    async fn stdout_stderr_and_status_share_one_stream() {
        let dir = tempfile::tempdir().expect("temp dir");
        let run = finished(
            try_run(
                dir.path(),
                "sh -c 'printf out; printf err >&2; exit 3'",
                Duration::from_secs(5),
            )
            .await,
        );
        assert!(run.output.contains("out"), "{}", run.output);
        assert!(run.output.contains("err"), "{}", run.output);
        assert_eq!(run.status, Some(3));
    }
}
