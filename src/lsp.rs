//! A deliberately small LSP client for the `lsp` tool.
//!
//! One server process per (workspace, configured server), started lazily on
//! the first diagnostics call. Phase 1 supports `textDocument/publishDiagnostics`
//! only; definition/references/hover are the next phase and share this client.
//!
//! The design is synchronous on the calling blocking-pool thread: a reader
//! thread owns stdout and forwards JSON-RPC values over a channel, so every
//! wait is a `recv_timeout` and a broken or silent server can never wedge the
//! agent. Configuration is the consent to spawn; nothing is installed or
//! discovered automatically.

use crate::config::LspServerConfig;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
const DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_DIAGNOSTICS: usize = 50;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    pub line: u64,
    pub column: u64,
    pub severity: &'static str,
    pub message: String,
    pub source: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiagnosticsReport {
    pub diagnostics: Vec<Diagnostic>,
    /// The server did not publish diagnostics within the wait window. An empty
    /// list then means "not known yet", not "clean".
    pub timed_out: bool,
}

type Key = (PathBuf, String);

/// Process-wide manager shared by tool calls. Bounded to eight workspaces so
/// a long-lived daemon cannot accumulate language-server processes forever.
pub struct LspManager {
    clients: Mutex<HashMap<Key, Arc<Mutex<Client>>>>,
}

impl LspManager {
    fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn diagnostics(
        &self,
        server_name: &str,
        server: &LspServerConfig,
        workspace_root: &Path,
        path: &Path,
    ) -> Result<DiagnosticsReport, String> {
        let key = (workspace_root.to_path_buf(), server_name.to_string());
        let client = {
            let mut clients = self
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(client) = clients.get(&key) {
                client.clone()
            } else {
                if clients.len() >= 8 {
                    clients.clear();
                }
                let client = Arc::new(Mutex::new(Client::spawn(server, workspace_root)?));
                clients.insert(key.clone(), client.clone());
                client
            }
        };
        let result = {
            let mut client = client
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            client.diagnostics(path)
        };
        if result.is_err() {
            // A dead or broken server must not poison the key forever. The
            // next call retries with a fresh process.
            let mut clients = self
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if clients
                .get(&key)
                .is_some_and(|existing| Arc::ptr_eq(existing, &client))
            {
                clients.remove(&key);
            }
        }
        result
    }

    #[cfg(test)]
    fn clear(&self) {
        self.clients
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }
}

pub fn manager() -> &'static LspManager {
    static MANAGER: OnceLock<LspManager> = OnceLock::new();
    MANAGER.get_or_init(LspManager::new)
}

/// Diagnostics for one file through the configured server. The server is
/// started on first use and reused for the process's lifetime.
pub fn diagnostics(
    server_name: &str,
    server: &LspServerConfig,
    workspace_root: &Path,
    path: &Path,
) -> Result<DiagnosticsReport, String> {
    manager().diagnostics(server_name, server, workspace_root, path)
}

struct Client {
    child: Child,
    stdin: ChildStdin,
    messages: Receiver<Value>,
    /// Notifications that arrived while waiting for a response.
    stash: VecDeque<Value>,
    next_id: u64,
    opened: HashMap<PathBuf, i32>,
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Client {
    fn spawn(server: &LspServerConfig, root: &Path) -> Result<Self, String> {
        let mut child = Command::new(&server.command)
            .args(&server.args)
            .current_dir(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                format!(
                    "could not start language server '{}': {error}",
                    server.command
                )
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "language server has no stdout".to_string())?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "language server has no stdin".to_string())?;
        let (tx, rx) = mpsc::channel();
        spawn_reader(stdout, tx);
        let mut client = Self {
            child,
            stdin,
            messages: rx,
            stash: VecDeque::new(),
            next_id: 1,
            opened: HashMap::new(),
        };
        let root_uri = file_uri(root);
        let result = client.request(
            "initialize",
            json!({
                "processId": null,
                "rootUri": root_uri,
                "capabilities": {},
                "workspaceFolders": [{
                    "uri": root_uri,
                    "name": root.file_name().and_then(|name| name.to_str()).unwrap_or("workspace"),
                }],
            }),
            INITIALIZE_TIMEOUT,
        )?;
        if let Some(error) = result.get("error") {
            return Err(format!("language server rejected initialize: {error}"));
        }
        client.notify("initialized", json!({}))?;
        Ok(client)
    }

    fn diagnostics(&mut self, path: &Path) -> Result<DiagnosticsReport, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("could not read '{}': {error}", path.display()))?;
        let uri = file_uri(path);
        let version = match self.opened.get_mut(path) {
            Some(version) => {
                *version += 1;
                let version = *version;
                self.notify(
                    "textDocument/didChange",
                    json!({
                        "textDocument": { "uri": uri, "version": version },
                        "contentChanges": [{ "text": text }],
                    }),
                )?;
                version
            }
            None => {
                self.opened.insert(path.to_path_buf(), 1);
                self.notify(
                    "textDocument/didOpen",
                    json!({
                        "textDocument": {
                            "uri": uri,
                            "languageId": language_id(path),
                            "version": 1,
                            "text": text,
                        }
                    }),
                )?;
                1
            }
        };
        let _ = version;
        let deadline = Instant::now() + DIAGNOSTICS_TIMEOUT;
        loop {
            let message = match self.next_message(deadline) {
                Ok(Some(message)) => message,
                Ok(None) | Err(RecvTimeoutError::Timeout) => {
                    return Ok(DiagnosticsReport {
                        diagnostics: Vec::new(),
                        timed_out: true,
                    })
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err("language server exited".to_string())
                }
            };
            if message.get("method").and_then(Value::as_str)
                == Some("textDocument/publishDiagnostics")
            {
                let params = message.get("params").cloned().unwrap_or(Value::Null);
                if params.get("uri").and_then(Value::as_str) == Some(uri.as_str()) {
                    return Ok(DiagnosticsReport {
                        diagnostics: parse_diagnostics(&params),
                        timed_out: false,
                    });
                }
            }
            self.stash.push_back(message);
        }
    }

    fn request(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))?;
        let deadline = Instant::now() + timeout;
        loop {
            let message = match self.next_message(deadline) {
                Ok(Some(message)) => message,
                Ok(None) | Err(RecvTimeoutError::Timeout) => {
                    return Err(format!("language server timed out on {method}"))
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err("language server exited".to_string())
                }
            };
            if message.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = message.get("error") {
                    return Err(format!("language server {method} error: {error}"));
                }
                return Ok(message.get("result").cloned().unwrap_or(Value::Null));
            }
            self.stash.push_back(message);
        }
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<(), String> {
        self.send(json!({ "jsonrpc": "2.0", "method": method, "params": params }))
    }

    fn next_message(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<Value>, RecvTimeoutError> {
        if let Some(message) = self.stash.pop_front() {
            return Ok(Some(message));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(RecvTimeoutError::Timeout);
        }
        self.messages.recv_timeout(remaining).map(Some)
    }

    fn send(&mut self, message: Value) -> Result<(), String> {
        let body = serde_json::to_vec(&message).map_err(|error| error.to_string())?;
        write!(self.stdin, "Content-Length: {}\r\n\r\n", body.len())
            .and_then(|_| self.stdin.write_all(&body))
            .and_then(|_| self.stdin.flush())
            .map_err(|error| format!("could not write to language server: {error}"))
    }
}

fn spawn_reader(stdout: ChildStdout, tx: Sender<Value>) {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let Some(length) = read_header(&mut reader) else {
                return;
            };
            let mut body = vec![0u8; length];
            if reader.read_exact(&mut body).is_err() {
                return;
            }
            if let Ok(message) = serde_json::from_slice::<Value>(&body) {
                if tx.send(message).is_err() {
                    return;
                }
            }
        }
    });
}

fn read_header(reader: &mut BufReader<ChildStdout>) -> Option<usize> {
    let mut length = None;
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).ok()?;
        if read == 0 {
            return None;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            return length;
        }
        if let Some(value) = trimmed
            .strip_prefix("Content-Length:")
            .or_else(|| trimmed.strip_prefix("content-length:"))
        {
            length = value.trim().parse().ok();
        }
    }
}

fn parse_diagnostics(params: &Value) -> Vec<Diagnostic> {
    let mut diagnostics: Vec<Diagnostic> = params
        .get("diagnostics")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(MAX_DIAGNOSTICS)
        .map(|diagnostic| {
            let start = diagnostic
                .get("range")
                .and_then(|range| range.get("start"));
            let line = start
                .and_then(|start| start.get("line"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            let column = start
                .and_then(|start| start.get("character"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            let severity = match diagnostic.get("severity").and_then(Value::as_u64) {
                Some(1) => "error",
                Some(2) => "warning",
                Some(3) => "info",
                Some(4) => "hint",
                _ => "info",
            };
            Diagnostic {
                line,
                column,
                severity,
                message: diagnostic
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source: diagnostic
                    .get("source")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            }
        })
        .collect();
    diagnostics.sort_by_key(|diagnostic| (diagnostic.line, diagnostic.column));
    diagnostics
}

fn language_id(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("rs") => "rust",
        Some("ts") => "typescript",
        Some("tsx") => "typescriptreact",
        Some("js") => "javascript",
        Some("jsx") => "javascriptreact",
        Some("py") => "python",
        Some("go") => "go",
        Some("java") => "java",
        Some("kt") => "kotlin",
        Some("swift") => "swift",
        Some("c") => "c",
        Some("h") => "c",
        Some("cpp") | Some("cc") | Some("hpp") => "cpp",
        Some("cs") => "csharp",
        Some("rb") => "ruby",
        Some("php") => "php",
        _ => "plaintext",
    }
}

/// `file://` URI with percent-encoding for the characters a URI cannot carry
/// literally. Kept local rather than adding a URL dependency for one field.
fn file_uri(path: &Path) -> String {
    let raw = path.to_string_lossy();
    let raw = if cfg!(windows) {
        format!("/{}", raw.replace('\\', "/"))
    } else {
        raw.into_owned()
    };
    let mut uri = String::from("file://");
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/'
            | b':' => uri.push(byte as char),
            _ => uri.push_str(&format!("%{byte:02X}")),
        }
    }
    uri
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python() -> Option<String> {
        for candidate in ["python3", "python"] {
            if Command::new(candidate).arg("--version").output().is_ok() {
                return Some(candidate.to_string());
            }
        }
        None
    }

    fn fake_server(dir: &Path) -> PathBuf {
        let path = dir.join("fake_lsp.py");
        std::fs::write(
            &path,
            r#"
import json, sys
def read_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        key, value = line.decode().split(":", 1)
        headers[key.lower()] = value.strip()
    length = int(headers.get("content-length", 0))
    return json.loads(sys.stdin.buffer.read(length))

def send(message):
    body = json.dumps(message).encode()
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    sys.stdout.buffer.flush()

while True:
    message = read_message()
    if message is None:
        break
    method = message.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"capabilities": {}}})
    elif method == "textDocument/didOpen":
        uri = message["params"]["textDocument"]["uri"]
        send({"jsonrpc": "2.0", "method": "textDocument/publishDiagnostics", "params": {
            "uri": uri,
            "diagnostics": [{
                "range": {"start": {"line": 1, "character": 2}, "end": {"line": 1, "character": 3}},
                "severity": 1,
                "message": "fake error",
                "source": "fake-lsp"
            }]
        }})
"#,
        )
        .unwrap();
        path
    }

    #[test]
    fn diagnostics_round_trip_through_a_real_stdio_server() {
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = fake_server(root);
        let file = root.join("hello.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let manager = LspManager::new();

        let report = manager
            .diagnostics("fake", &server, root, &file)
            .expect("diagnostics");
        assert!(!report.timed_out);
        assert_eq!(report.diagnostics.len(), 1);
        let diagnostic = &report.diagnostics[0];
        assert_eq!(diagnostic.line, 2);
        assert_eq!(diagnostic.column, 3);
        assert_eq!(diagnostic.severity, "error");
        assert_eq!(diagnostic.message, "fake error");
        assert_eq!(diagnostic.source.as_deref(), Some("fake-lsp"));
        manager.clear();
    }

    #[test]
    fn a_server_that_exits_is_reported_not_panicked() {
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = root.join("exit.py");
        std::fs::write(&script, "import sys\nsys.exit(1)\n").unwrap();
        let file = root.join("hello.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let manager = LspManager::new();
        let error = manager
            .diagnostics("dying", &server, root, &file)
            .expect_err("a dying server is an error");
        assert!(!error.is_empty());
    }
}
