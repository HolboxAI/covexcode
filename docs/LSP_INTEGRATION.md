# LSP integration

Status: phases 1–3 implemented (diagnostics, definition, references, hover,
and warm document symbols in `repo_map`); phase 4 proposed  
Date: 2026-11-19  
Scope: `HolboxAI/covexcode` runtime, inherited by the IDE through the bundled
sidecar

## Goal

Give the agent the few language-server answers that replace exploratory turns:

- diagnostics for the file it just changed;
- go-to-definition and find-references for a symbol;
- a one-line hover type/signature when the source is not enough.

Definition/reference queries are the real win: `grep_search` finds text, but
only the language server knows which `parse` a call resolves to. Diagnostics
close the loop after a write without running a full build.

## Non-goals

- A general editor LSP client.
- Formatting, rename, code actions, completion.
- Managing language servers the user did not configure. A missing server is a
  clear "not configured" answer, not an install prompt.

## Surface

One tool, `lsp`, with an `action` enum, rather than four schemas:

```
lsp {
  action: "diagnostics" | "definition" | "references" | "hover",
  path: string,                 // workspace-relative
  line?: integer,               // 1-based, required except diagnostics
  column?: integer,             // 1-based, required except diagnostics
}
```

It is read-only, part of the pure-read fan-out, allowed in plan mode and in
research subagents. Diagnostics return a bounded list (`severity`, `line`,
`message`, `source`); references return `path:line:column` lines; hover returns
plain text. Every response is clipped the same way `grep_search` is.

## Configuration

```toml
[lsp]
enabled = false

[lsp.servers.rust]
command = "rust-analyzer"
args = []

[lsp.servers.typescript]
command = "typescript-language-server"
args = ["--stdio"]
extensions = ["ts", "tsx", "js", "jsx"]
```

- `enabled` defaults to `false`: spawning a language server is a user decision.
- A server is selected by file extension; no extension list means the server
  name is matched against the extension (`.rs` → `rust` would need an explicit
  mapping, so examples always show one).
- No automatic installs and no PATH probing beyond spawning the configured
  command. Failure to start is reported as text.

## Architecture

`src/lsp.rs` (new), compiled into both `covexcode` and `covexcode-agent`.

- `LspManager`: process-global `Mutex<HashMap<(workspace_root, server_name),
  Arc<Mutex<LspClient>>>>`. One server per workspace and language.
- Lazy start on the first `lsp` call. The handshake is
  `initialize` → `initialized`, with `rootUri` and the workspace capabilities;
  a 10-second timeout aborts startup.
- Framing is the LSP base protocol: `Content-Length` headers over the child's
  stdin/stdout. A reader task correlates responses by JSON-RPC id and forwards
  `textDocument/publishDiagnostics` notifications into a per-URI watch channel.
- Documents are opened once per file (`didOpen`, language id from the server
  mapping) and re-opened only if the file changed; writes already invalidate
  the repo index, so a version counter can be shared with that path.
- Positions are converted at the boundary: the tool accepts 1-based line and
  column, and converts to LSP's UTF-16 code-unit positions. The bytes/UTF-16
  distinction is a correctness issue, not a detail; tests cover non-ASCII text.
- Every request has a timeout (5 seconds for query actions, 2 seconds waiting
  for diagnostics after `didOpen`). A crashed server is removed from the map
  and restarted once on the next call; a second failure is reported.
- Shutdown: `shutdown`/`exit` on session drop where practical, and the child is
  killed if it does not exit. An idle server is left running for the session.

## Integration points

- `tools.rs`: `LSP` constant, schema, `Action::Lsp`, execution dispatch,
  read-only/plan-mode/subagent allowlists.
- `config.rs`: `LspConfig` + `LspServerConfig` with defaults; config tests.
- `headless.rs`: nothing special — the tool runs through the normal executor,
  so ACP/IDE gets it automatically.
- `transport.rs`: advertise `agentCapabilities.covexcode.lsp = true`; the IDE
  can gate a future diagnostics view on it.
- `repo_map`: the top three files ask an **already-running** server for
  `textDocument/documentSymbol`; the regex index still ranks the files and is
  the fallback everywhere else. A cold or busy server is skipped rather than
  waited on, so the map never pays `initialize` or an indexing stall.

## Phases

1. **Config + manager + `lsp_diagnostics`** — implemented: handshake, didOpen,
   timeout, crash handling, fake-server tests.
2. **`definition` + `references` + `hover`** — implemented: UTF-16 conversion
   at the boundary, 5-second query timeouts, bounded rendering, notifications
   interleaved with replies handled without spinning.
3. **Repo-map symbol source** — implemented: warm servers answer
   `documentSymbol` for the top-ranked files; the regex index remains the
   ranking source and fallback, and a cold server is never spawned or waited
   on by the map.
4. **IDE surface** — a diagnostics count/list in the chat tool card or a
   dedicated view, gated on the capability flag. The IDE needs no new ACP
   transport work: the tool result already streams as a normal tool call.

## Testing

- A fake LSP server embedded in `src/lsp.rs` tests (a small Python stdio
  program) answers `initialize`, `didOpen`, `definition`, `references`,
  `hover`, `documentSymbol`, and emits diagnostics; tests spawn it through a
  real `LspServerConfig`. They skip when no Python interpreter is available.
- Timeout and crash tests: server that accepts but never answers; server that
  exits mid-request.
- UTF-16 position tests with emoji and combining characters.
- Config tests for defaults and extension mapping.
- Load test: repeated diagnostics calls reuse one server process.

## Risks / decisions to settle before phase 1

- **Language id mapping**: explicit per-server `extensions` is simple and
  predictable; auto-detection from file name is convenient but invisible.
- **Restart policy**: one restart per call is proposed; a tighter backoff may
  be safer on a broken server.
- **Workspace trust**: the CLI has no trust model, so configuration is the
  consent. The IDE should still run only in trusted folders because it spawns
  a local process.
- **Diagnostics timing**: waiting 2 seconds after `didOpen` is a heuristic;
  servers that index first may need longer. The tool should say when it timed
  out rather than implying there are no diagnostics.
- **Memory**: one server per workspace can be large (rust-analyzer especially).
  Bound the manager to a small number of workspaces, mirroring the repo index.
