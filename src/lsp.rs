//! A deliberately small LSP client for the `lsp` tool and `repo_map`.
//!
//! One server process per (workspace, configured server), started lazily on
//! the first diagnostics or navigation call. Supports
//! `textDocument/publishDiagnostics`, definition, references, hover and
//! documentSymbol; the last is only ever asked of an already-running server,
//! so it can enrich `repo_map` without making a cold map wait for `initialize`.
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
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
/// A warm server answers document symbols in milliseconds. The short bound
/// keeps `repo_map` fast when the server is busy or half-dead; the map falls
/// back to its regex symbols on any failure.
const SYMBOLS_TIMEOUT: Duration = Duration::from_millis(750);
const MAX_DIAGNOSTICS: usize = 50;
const MAX_SYMBOLS: usize = 8;
/// The `lsp` tool's `symbols` action shows a whole file's outline, so it
/// takes far more than `repo_map`'s eight-name summary.
const MAX_TOOL_SYMBOLS: usize = 50;
/// A broken or hostile server must not turn a header into an unbounded
/// allocation. Real servers send a few dozen bytes; 8 KiB is generous.
const MAX_HEADER_BYTES: usize = 8 * 1024;
/// No real LSP reply approaches this size. A larger declared length is
/// refused rather than allocated.
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryKind {
    Definition,
    References,
    Hover,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
    /// Workspace-relative when the server points inside the workspace,
    /// otherwise the absolute path the server returned.
    pub path: String,
    /// 1-based, converted from the server's zero-based UTF-16 position.
    pub line: u64,
    pub column: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryReport {
    Locations(Vec<Location>),
    Hover(Option<String>),
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

    fn client_for(
        &self,
        server_name: &str,
        server: &LspServerConfig,
        workspace_root: &Path,
    ) -> Result<(Key, Arc<Mutex<Client>>), String> {
        let key = (workspace_root.to_path_buf(), server_name.to_string());
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(client) = clients.get(&key) {
            return Ok((key, client.clone()));
        }
        if clients.len() >= 8 {
            clients.clear();
        }
        let client = Arc::new(Mutex::new(Client::spawn(server, workspace_root)?));
        clients.insert(key.clone(), client.clone());
        Ok((key, client))
    }

    fn forget(&self, key: &Key, client: &Arc<Mutex<Client>>) {
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if clients
            .get(key)
            .is_some_and(|existing| Arc::ptr_eq(existing, client))
        {
            clients.remove(key);
        }
    }

    fn diagnostics(
        &self,
        server_name: &str,
        server: &LspServerConfig,
        workspace_root: &Path,
        path: &Path,
    ) -> Result<DiagnosticsReport, String> {
        let (key, client) = self.client_for(server_name, server, workspace_root)?;
        let result = {
            let mut client = client
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            client.diagnostics(path)
        };
        if result.is_err() {
            self.forget(&key, &client);
        }
        result
    }

    fn query(
        &self,
        kind: QueryKind,
        server_name: &str,
        server: &LspServerConfig,
        workspace_root: &Path,
        path: &Path,
        line: u64,
        column: u64,
    ) -> Result<QueryReport, String> {
        let (key, client) = self.client_for(server_name, server, workspace_root)?;
        let result = {
            let mut client = client
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            client.query(kind, path, line, column, workspace_root)
        };
        if result.is_err() {
            self.forget(&key, &client);
        }
        result
    }

    /// Document symbols for one file, starting the server if necessary. This
    /// backs the `lsp` tool's `symbols` action; unlike
    /// [`Self::try_document_symbols`], a cold server is expected here and gets
    /// the full query timeout.
    fn document_symbols(
        &self,
        server_name: &str,
        server: &LspServerConfig,
        workspace_root: &Path,
        path: &Path,
    ) -> Result<Vec<String>, String> {
        let (key, client) = self.client_for(server_name, server, workspace_root)?;
        let result = {
            let mut client = client
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            client.document_symbols(path, MAX_TOOL_SYMBOLS, QUERY_TIMEOUT)
        };
        if result.is_err() {
            self.forget(&key, &client);
        }
        result
    }

    /// Document symbols for one file, but only when a server for this
    /// workspace is already running. Never spawns or waits: `repo_map` calls
    /// this for its top files and keeps its regex symbols when the answer is
    /// `None`, so a cold or busy server cannot slow the map down.
    fn try_document_symbols(
        &self,
        server_name: &str,
        workspace_root: &Path,
        path: &Path,
    ) -> Option<Vec<String>> {
        let key = (workspace_root.to_path_buf(), server_name.to_string());
        let client = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key)?
            .clone();
        // Another query holds the client: skip this file rather than queueing
        // behind it. The next map build can try again.
        let mut client = client.try_lock().ok()?;
        let symbols = client
            .document_symbols(path, MAX_SYMBOLS, SYMBOLS_TIMEOUT)
            .ok()?;
        (!symbols.is_empty()).then_some(symbols)
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

/// Definition, references or hover at a 1-based line/column.
pub fn query(
    kind: QueryKind,
    server_name: &str,
    server: &LspServerConfig,
    workspace_root: &Path,
    path: &Path,
    line: u64,
    column: u64,
) -> Result<QueryReport, String> {
    manager().query(kind, server_name, server, workspace_root, path, line, column)
}

/// Document symbols from an already-running server; `None` when the server is
/// not warm yet, is busy, or does not answer in time. `repo_map` treats
/// `None` as "keep the regex symbols".
pub fn try_document_symbols(
    server_name: &str,
    workspace_root: &Path,
    path: &Path,
) -> Option<Vec<String>> {
    manager().try_document_symbols(server_name, workspace_root, path)
}

/// Document symbols for one file through the configured server, starting it
/// if needed. Unlike [`try_document_symbols`], this waits for a cold server
/// and is what the `lsp` tool's `symbols` action calls.
pub fn document_symbols(
    server_name: &str,
    server: &LspServerConfig,
    workspace_root: &Path,
    path: &Path,
) -> Result<Vec<String>, String> {
    manager().document_symbols(server_name, server, workspace_root, path)
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
        let (uri, _text) = self.ensure_open(path)?;
        let deadline = Instant::now() + DIAGNOSTICS_TIMEOUT;
        // Anything that is not this file's diagnostics is set aside for the
        // next caller. It must not go straight back into `stash`: this loop
        // would pick it up again and spin instead of waiting for the server.
        let mut deferred = Vec::new();
        let report = loop {
            let message = match self.next_message(deadline) {
                Ok(Some(message)) => message,
                Ok(None) | Err(RecvTimeoutError::Timeout) => {
                    break Ok(DiagnosticsReport {
                        diagnostics: Vec::new(),
                        timed_out: true,
                    })
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break Err("language server exited".to_string())
                }
            };
            if message.get("method").and_then(Value::as_str)
                == Some("textDocument/publishDiagnostics")
            {
                let params = message.get("params").cloned().unwrap_or(Value::Null);
                if params.get("uri").and_then(Value::as_str) == Some(uri.as_str()) {
                    break Ok(DiagnosticsReport {
                        diagnostics: parse_diagnostics(&params),
                        timed_out: false,
                    });
                }
            }
            deferred.push(message);
        };
        // Restore arrival order; `next_message` reads the queue front first.
        for message in deferred.into_iter().rev() {
            self.stash.push_front(message);
        }
        report
    }

    fn ensure_open(&mut self, path: &Path) -> Result<(String, String), String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("could not read '{}': {error}", path.display()))?;
        let uri = file_uri(path);
        match self.opened.get_mut(path) {
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
            }
        }
        Ok((uri, text))
    }

    fn query(
        &mut self,
        kind: QueryKind,
        path: &Path,
        line: u64,
        column: u64,
        workspace_root: &Path,
    ) -> Result<QueryReport, String> {
        let (uri, text) = self.ensure_open(path)?;
        let (line, character) = position(&text, line, column)?;
        let position = json!({ "line": line, "character": character });
        let (method, params) = match kind {
            QueryKind::Definition => (
                "textDocument/definition",
                json!({ "textDocument": { "uri": uri }, "position": position }),
            ),
            QueryKind::References => (
                "textDocument/references",
                json!({
                    "textDocument": { "uri": uri },
                    "position": position,
                    "context": { "includeDeclaration": true },
                }),
            ),
            QueryKind::Hover => (
                "textDocument/hover",
                json!({ "textDocument": { "uri": uri }, "position": position }),
            ),
        };
        let result = self.request(method, params, QUERY_TIMEOUT)?;
        match kind {
            QueryKind::Definition | QueryKind::References => {
                Ok(QueryReport::Locations(parse_locations(&result, workspace_root)))
            }
            QueryKind::Hover => Ok(QueryReport::Hover(parse_hover(&result))),
        }
    }

    /// `textDocument/documentSymbol`, reduced to names. Both wire shapes are
    /// handled: `SymbolInformation[]` is flat and `DocumentSymbol[]` nests
    /// children.
    fn document_symbols(
        &mut self,
        path: &Path,
        limit: usize,
        timeout: Duration,
    ) -> Result<Vec<String>, String> {
        let (uri, _text) = self.ensure_open(path)?;
        let result = self.request(
            "textDocument/documentSymbol",
            json!({ "textDocument": { "uri": uri } }),
            timeout,
        )?;
        Ok(parse_symbol_names(&result, limit))
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
        // See `diagnostics`: messages that are not our reply are deferred until
        // this request finishes rather than re-queued in front of ourselves.
        let mut deferred = Vec::new();
        let outcome = loop {
            let message = match self.next_message(deadline) {
                Ok(Some(message)) => message,
                Ok(None) | Err(RecvTimeoutError::Timeout) => {
                    break Err(format!("language server timed out on {method}"))
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break Err("language server exited".to_string())
                }
            };
            if message.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = message.get("error") {
                    break Err(format!("language server {method} error: {error}"));
                }
                break Ok(message.get("result").cloned().unwrap_or(Value::Null));
            }
            deferred.push(message);
        };
        for message in deferred.into_iter().rev() {
            self.stash.push_front(message);
        }
        outcome
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
            if length > MAX_MESSAGE_BYTES {
                // Do not allocate what the server claims. Dropping `tx` makes
                // the waiting request report a closed server, and the manager
                // starts a fresh one on the next call.
                return;
            }
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
    let mut consumed = 0usize;
    loop {
        // Bound the read itself, not just the result: a server that never
        // sends a newline must not grow `read_until`'s buffer without limit.
        let remaining = MAX_HEADER_BYTES.checked_sub(consumed)?;
        if remaining == 0 {
            return None;
        }
        let mut line = Vec::new();
        let read = reader
            .by_ref()
            .take(remaining as u64)
            .read_until(b'\n', &mut line)
            .ok()?;
        if read == 0 {
            return None;
        }
        consumed += read;
        let terminated = line.last() == Some(&b'\n');
        let trimmed = line.strip_suffix(b"\n").unwrap_or(&line);
        let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);
        if trimmed.is_empty() {
            return length;
        }
        if let Some(value) = trimmed
            .strip_prefix(b"Content-Length:")
            .or_else(|| trimmed.strip_prefix(b"content-length:"))
        {
            length = std::str::from_utf8(value).ok()?.trim().parse().ok();
        }
        if !terminated {
            // The cap was hit mid-line: a malformed header, not a longer one
            // worth buffering.
            return None;
        }
    }
}

/// Convert a 1-based character position into LSP's zero-based line and
/// UTF-16 code-unit column. `char` counts are what a human and the tool schema
/// use; the server counts UTF-16 units, which differ for emoji and some
/// combining marks.
fn position(text: &str, line: u64, column: u64) -> Result<(u64, u64), String> {
    if line == 0 || column == 0 {
        return Err("line and column are 1-based and must be at least 1".to_string());
    }
    let line_text = text
        .lines()
        .nth((line - 1) as usize)
        .ok_or_else(|| format!("line {line} is outside the file"))?;
    let offset = (column - 1) as usize;
    let prefix: String = line_text.chars().take(offset).collect();
    if prefix.chars().count() < offset {
        return Err(format!("column {column} is outside line {line}"));
    }
    Ok(((line - 1), prefix.encode_utf16().count() as u64))
}

fn parse_locations(value: &Value, workspace_root: &Path) -> Vec<Location> {
    let items: Vec<&Value> = match value {
        Value::Array(items) => items.iter().collect(),
        Value::Object(_) => vec![value],
        _ => Vec::new(),
    };
    items
        .into_iter()
        .filter_map(|item| {
            let uri = item
                .get("uri")
                .or_else(|| item.get("targetUri"))
                .and_then(Value::as_str)?;
            let range = item
                .get("range")
                .or_else(|| item.get("targetSelectionRange"))
                .or_else(|| item.get("targetRange"))?;
            let start = range.get("start")?;
            let line = start.get("line").and_then(Value::as_u64)? + 1;
            let column = start.get("character").and_then(Value::as_u64)? + 1;
            Some(Location {
                path: uri_to_path(uri, workspace_root),
                line,
                column,
            })
        })
        .take(50)
        .collect()
}

fn parse_hover(value: &Value) -> Option<String> {
    let contents = value.get("contents")?;
    let text = match contents {
        Value::String(text) => text.clone(),
        Value::Object(object) => object.get("value").and_then(Value::as_str)?.to_string(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(text) => Some(text.clone()),
                Value::Object(object) => object
                    .get("value")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

/// Names from `textDocument/documentSymbol`, bounded to `limit`. Accepts both
/// wire shapes: flat `SymbolInformation[]` and hierarchical `DocumentSymbol[]`
/// with `children`.
fn parse_symbol_names(value: &Value, limit: usize) -> Vec<String> {
    fn collect(items: &[Value], depth: usize, limit: usize, names: &mut Vec<String>) {
        // A server controls this shape; an unbounded walk would be a stack
        // hazard for no benefit. Eight levels is already deeper than any
        // outline this tool shows.
        if depth > 8 {
            return;
        }
        for item in items {
            if names.len() >= limit {
                return;
            }
            if let Some(name) = item.get("name").and_then(Value::as_str) {
                let name = name.trim();
                if !name.is_empty() && !names.iter().any(|existing| existing == name) {
                    names.push(name.to_string());
                }
            }
            if let Some(children) = item.get("children").and_then(Value::as_array) {
                collect(children, depth + 1, limit, names);
            }
        }
    }
    let items: &[Value] = match value {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut names = Vec::new();
    collect(items, 0, limit, &mut names);
    names
}

fn uri_to_path(uri: &str, workspace_root: &Path) -> String {
    let Some(rest) = uri.strip_prefix("file://") else {
        return uri.to_string();
    };
    let decoded = percent_decode(rest);
    // A `file:///C:/...` URI carries a leading slash the Windows path lacks.
    #[cfg(windows)]
    let decoded = decoded.trim_start_matches('/').to_string();
    let path = PathBuf::from(decoded);
    match path.strip_prefix(workspace_root) {
        Ok(relative) => relative
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/"),
        Err(_) => path.to_string_lossy().into_owned(),
    }
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(value) = u8::from_str_radix(&input[index + 1..index + 3], 16) {
                out.push(value);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
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
    elif method == "textDocument/definition":
        # A notification before the reply: the client must set it aside rather
        # than re-queue it ahead of the answer and spin.
        send({"jsonrpc": "2.0", "method": "window/logMessage", "params": {
            "type": 3, "message": "resolving definition"
        }})
        send({"jsonrpc": "2.0", "id": message["id"], "result": [{
            "uri": message["params"]["textDocument"]["uri"],
            "range": {"start": {"line": 4, "character": 1}, "end": {"line": 4, "character": 5}}
        }]})
    elif method == "textDocument/references":
        uri = message["params"]["textDocument"]["uri"]
        send({"jsonrpc": "2.0", "id": message["id"], "result": [
            {"uri": uri, "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}}},
            {"uri": uri, "range": {"start": {"line": 2, "character": 4}, "end": {"line": 2, "character": 5}}}
        ]})
    elif method == "textDocument/hover":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {
            "contents": {"kind": "markdown", "value": "```rust\\nfn target()\\n```"}
        }})
    elif method == "textDocument/documentSymbol":
        r = {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}}
        send({"jsonrpc": "2.0", "id": message["id"], "result": [{
            "name": "lsp_symbol", "kind": 12, "range": r, "selectionRange": r,
            "children": [{"name": "nested_symbol", "kind": 12, "range": r, "selectionRange": r}]
        }]})
"#,
        )
        .unwrap();
        path
    }

    /// A server that completes the handshake and then stays silent: the shape
    /// of one still indexing. It never publishes diagnostics.
    fn silent_server(dir: &Path) -> PathBuf {
        let path = dir.join("silent_lsp.py");
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
    if message.get("method") == "initialize":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"capabilities": {}}})
"#,
        )
        .unwrap();
        path
    }

    /// Completes the handshake, then declares a message larger than any real
    /// reply. The client must stop reading instead of allocating the claimed
    /// length.
    fn oversized_message_server(dir: &Path) -> PathBuf {
        let path = dir.join("oversized_message_lsp.py");
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
        sys.stdout.buffer.write(b"Content-Length: 1152921504606846976\r\n\r\n")
        sys.stdout.buffer.flush()
"#,
        )
        .unwrap();
        path
    }

    /// Sends a header line that never ends: the client must cap the buffered
    /// header instead of growing it for as long as the server keeps writing.
    fn endless_header_server(dir: &Path) -> PathBuf {
        let path = dir.join("endless_header_lsp.py");
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
        sys.stdout.buffer.write(b"X-Junk: " + b"a" * 100000)
        sys.stdout.buffer.flush()
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

    /// Silence is not cleanliness. A server that accepts the document but does
    /// not publish diagnostics (still indexing) must report `timed_out`, so the
    /// tool can say "unknown" instead of claiming there are no issues.
    #[test]
    fn a_silent_server_reports_timed_out_not_clean() {
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = silent_server(root);
        let file = root.join("hello.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let manager = LspManager::new();

        let started = Instant::now();
        let report = manager
            .diagnostics("silent", &server, root, &file)
            .expect("a timeout is a report, not an error");
        assert!(report.timed_out);
        assert!(report.diagnostics.is_empty());
        assert!(
            started.elapsed() >= DIAGNOSTICS_TIMEOUT,
            "waited the full window"
        );
        manager.clear();
    }

    #[test]
    fn an_oversized_declared_message_is_refused() {
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = oversized_message_server(root);
        let file = root.join("hello.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let manager = LspManager::new();

        let error = manager
            .diagnostics("oversized", &server, root, &file)
            .expect_err("the reader must stop, not allocate");
        assert!(error.contains("exited"), "{error}");
        assert!(
            manager
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty(),
            "the broken client is forgotten"
        );
        manager.clear();
    }

    #[test]
    fn an_endless_header_line_is_cut_off() {
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = endless_header_server(root);
        let file = root.join("hello.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let manager = LspManager::new();

        let error = manager
            .diagnostics("endless", &server, root, &file)
            .expect_err("the reader must cap the header");
        assert!(error.contains("exited"), "{error}");
        manager.clear();
    }

    #[test]
    fn navigation_queries_return_relative_locations_and_hover() {
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = fake_server(root);
        let file = root.join("hello.rs");
        std::fs::write(&file, "fn main() {\n    target();\n}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let manager = LspManager::new();

        let definition = manager
            .query(QueryKind::Definition, "fake", &server, root, &file, 2, 5)
            .expect("definition");
        assert_eq!(
            definition,
            QueryReport::Locations(vec![Location {
                path: "hello.rs".to_string(),
                line: 5,
                column: 2,
            }])
        );

        let references = manager
            .query(QueryKind::References, "fake", &server, root, &file, 2, 5)
            .expect("references");
        let QueryReport::Locations(references) = references else {
            panic!("expected locations");
        };
        assert_eq!(references.len(), 2);
        assert_eq!(references[0].line, 1);
        assert_eq!(references[1].line, 3);

        let hover = manager
            .query(QueryKind::Hover, "fake", &server, root, &file, 2, 5)
            .expect("hover");
        let QueryReport::Hover(Some(hover)) = hover else {
            panic!("expected hover text");
        };
        assert!(hover.contains("fn target()"), "{hover}");
        manager.clear();
    }

    #[test]
    fn document_symbol_names_accept_flat_and_nested_shapes() {
        let flat = json!([{"name": "alpha"}, {"name": "alpha"}, {"name": "beta"}]);
        assert_eq!(
            parse_symbol_names(&flat, MAX_SYMBOLS),
            vec!["alpha", "beta"]
        );

        let nested = json!([{"name": "outer", "children": [{"name": "inner"}]}]);
        assert_eq!(
            parse_symbol_names(&nested, MAX_SYMBOLS),
            vec!["outer", "inner"]
        );

        assert!(parse_symbol_names(&json!(null), MAX_SYMBOLS).is_empty());

        let many = json!([
            {"name": "one"},
            {"name": "two"},
            {"name": "three"}
        ]);
        assert_eq!(
            parse_symbol_names(&many, 2),
            vec!["one", "two"],
            "the tool's outline limit is enforced"
        );
    }

    /// `repo_map` only asks an already-warm server: a cold one must answer
    /// `None` (keeping the regex symbols) rather than spawn or block.
    #[test]
    fn document_symbols_come_only_from_a_warm_server() {
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

        assert!(manager
            .try_document_symbols("fake", root, &file)
            .is_none());

        manager
            .query(QueryKind::Hover, "fake", &server, root, &file, 1, 1)
            .expect("warm the server");
        assert_eq!(
            manager.try_document_symbols("fake", root, &file),
            Some(vec!["lsp_symbol".to_string(), "nested_symbol".to_string()])
        );
        manager.clear();
    }

    /// Tests that drive the process-global manager must not run concurrently:
    /// one test's `clear()` would drop the warm client another just
    /// established. The per-manager tests can stay parallel.
    fn global_manager_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// End to end: the top file's regex symbol is replaced by the warm
    /// server's document symbols in the rendered map.
    #[tokio::test]
    async fn repo_map_uses_warm_document_symbols() {
        let _global = global_manager_guard();
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = fake_server(root);
        let file = root.join("hello.rs");
        std::fs::write(&file, "pub fn regex_fn() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let lsp = crate::config::LspConfig {
            enabled: true,
            servers: [("fake".to_string(), server.clone())].into(),
        };
        // Warm the process-global client under this root and server name,
        // exactly as an earlier `lsp` tool call in the session would. The
        // workspace's canonical root, not the temp-dir spelling, is the key
        // both paths use.
        let workspace = crate::workspace::Workspace::new(root).expect("workspace");
        query(
            QueryKind::Hover,
            "fake",
            &server,
            workspace.root(),
            &file,
            1,
            1,
        )
        .expect("warm");

        let call = crate::llm::ToolCall {
            id: "map".to_string(),
            kind: "function".to_string(),
            function: crate::llm::FunctionCall {
                name: "repo_map".to_string(),
                arguments: "{}".to_string(),
            },
        };
        let out = crate::tools::execute_with_lsp(
            &call,
            &workspace,
            &crate::config::ToolsConfig::default(),
            Some(&lsp),
        )
        .await;
        assert!(out.content.contains("lsp_symbol"), "{}", out.content);
        assert!(!out.content.contains("regex_fn"), "{}", out.content);
        manager().clear();
    }

    /// Repeated calls must share one process: spawning `rust-analyzer` per
    /// tool call would pay `initialize` (and often an index) every time.
    #[test]
    fn repeated_calls_reuse_one_server_process() {
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

        manager
            .query(QueryKind::Hover, "fake", &server, root, &file, 1, 1)
            .expect("first query");
        let first = {
            let clients = manager
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            assert_eq!(clients.len(), 1, "one client for one workspace+server");
            clients
                .values()
                .next()
                .expect("spawned client")
                .clone()
        };
        manager
            .query(QueryKind::Hover, "fake", &server, root, &file, 1, 1)
            .expect("second query");
        let second = {
            let clients = manager
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            assert_eq!(clients.len(), 1);
            clients
                .values()
                .next()
                .expect("spawned client")
                .clone()
        };
        assert!(Arc::ptr_eq(&first, &second), "the same client was reused");
        manager.clear();
    }

    /// The documented restart policy: a request that hits a dead process
    /// fails and removes it, and the next call starts a fresh one instead of
    /// reporting the same error forever.
    #[test]
    fn a_dead_server_is_replaced_on_the_next_call() {
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

        manager
            .query(QueryKind::Hover, "fake", &server, root, &file, 1, 1)
            .expect("spawn the server");
        // Kill the child under the client, the same shape as a crash.
        {
            let client = manager
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .values()
                .next()
                .expect("spawned client")
                .clone();
            client
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .child
                .kill()
                .expect("kill the fake server");
        }

        assert!(
            manager
                .query(QueryKind::Hover, "fake", &server, root, &file, 1, 1)
                .is_err(),
            "a dead server reports an error"
        );
        assert!(
            manager
                .clients
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty(),
            "the failed client is forgotten"
        );

        let reply = manager
            .query(QueryKind::Hover, "fake", &server, root, &file, 1, 1)
            .expect("the next call starts a fresh server");
        let QueryReport::Hover(Some(reply)) = reply else {
            panic!("expected hover text");
        };
        assert!(reply.contains("fn target()"), "{reply}");
        manager.clear();
    }

    /// Two files checked in one `lsp` call: both reports are rendered, with
    /// the `path:line:column severity: message` rows the IDE card parses.
    #[tokio::test]
    async fn multi_file_diagnostics_reports_every_file() {
        let _global = global_manager_guard();
        let Some(python) = python() else {
            return;
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path();
        let script = fake_server(root);
        std::fs::write(root.join("a.rs"), "fn a() {}\n").unwrap();
        std::fs::write(root.join("b.rs"), "fn b() {}\n").unwrap();
        let server = LspServerConfig {
            command: python,
            args: vec![script.to_string_lossy().into_owned()],
            extensions: vec!["rs".to_string()],
        };
        let lsp = crate::config::LspConfig {
            enabled: true,
            servers: [("fake".to_string(), server)].into(),
        };
        let workspace = crate::workspace::Workspace::new(root).expect("workspace");
        let call = crate::llm::ToolCall {
            id: "diag".to_string(),
            kind: "function".to_string(),
            function: crate::llm::FunctionCall {
                name: crate::tools::LSP.to_string(),
                arguments: json!({ "paths": ["a.rs", "b.rs"] }).to_string(),
            },
        };
        let out = crate::tools::execute_with_lsp(
            &call,
            &workspace,
            &crate::config::ToolsConfig::default(),
            Some(&lsp),
        )
        .await;
        assert!(out.display.contains("2 files"), "{}", out.display);
        assert!(out.display.contains("2 issues"), "{}", out.display);
        for file in ["a.rs", "b.rs"] {
            assert!(
                out.content
                    .contains(&format!("{file}:2:3 error: fake error [fake-lsp]")),
                "missing {file} diagnostic: {}",
                out.content
            );
        }
        manager().clear();
    }

    /// `symbols` is an explicit outline request: it may start a cold server
    /// and returns the document's names, unlike the warm-only repo-map path.
    #[tokio::test]
    async fn symbols_action_returns_a_file_outline() {
        let _global = global_manager_guard();
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
        let lsp = crate::config::LspConfig {
            enabled: true,
            servers: [("fake".to_string(), server)].into(),
        };
        let workspace = crate::workspace::Workspace::new(root).expect("workspace");
        let call = crate::llm::ToolCall {
            id: "symbols".to_string(),
            kind: "function".to_string(),
            function: crate::llm::FunctionCall {
                name: crate::tools::LSP.to_string(),
                arguments: json!({ "action": "symbols", "path": "hello.rs" }).to_string(),
            },
        };
        let out = crate::tools::execute_with_lsp(
            &call,
            &workspace,
            &crate::config::ToolsConfig::default(),
            Some(&lsp),
        )
        .await;
        assert!(out.display.contains("symbols hello.rs — 2"), "{}", out.display);
        assert!(out.content.contains("Symbols in hello.rs:"), "{}", out.content);
        assert!(out.content.contains("lsp_symbol"), "{}", out.content);
        assert!(out.content.contains("nested_symbol"), "{}", out.content);
        manager().clear();
    }

    #[test]
    fn positions_are_converted_to_utf16_at_the_boundary() {
        // 'a' is 1 UTF-16 unit, the emoji is 2; column 3 means "after both".
        assert_eq!(position("a😀b\n", 1, 3).unwrap(), (0, 3));
        assert_eq!(position("plain\nsecond\n", 2, 1).unwrap(), (1, 0));
        assert!(position("only one line", 2, 1).is_err());
        assert!(position("abc", 1, 5).is_err());
        assert!(position("abc", 0, 1).is_err());
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
