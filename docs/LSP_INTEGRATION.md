# LSP integration

Status: phases 1–4 implemented: diagnostics, definition, references, hover,
warm document symbols in `repo_map`, and the IDE diagnostics card  
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
  path?: string,                // workspace-relative; required for navigation
  paths?: string[],             // diagnostics only: several files, max 10
  line?: integer,               // 1-based, required except diagnostics
  column?: integer,             // 1-based, required except diagnostics
}
```

It is read-only, part of the pure-read fan-out, allowed in plan mode and in
research subagents. Diagnostics return a bounded list (`severity`, `line`,
`message`, `source`); references return `path:line:column` lines; hover returns
plain text. Every response is clipped the same way `grep_search` is.

Diagnostics also accepts `paths` (or `path` plus `paths`): up to ten files are
checked in order, deduplicated, with a ten-second total budget. A broad edit
can therefore be checked in one call without letting a slow server hold the
turn; files past the budget are reported as skipped, not silently dropped.

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
  Frames are bounded (64 MiB per message, 8 KiB of headers): a broken or
  hostile server cannot make the client allocate an unbounded buffer from a
  declaration.
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
- `/doctor` reports whether LSP is off (and how to enable it), on without any
  servers, or on with the configured server names; it never starts a server.
- `headless.rs`: nothing special — the tool runs through the normal executor,
  so ACP/IDE gets it automatically.
- `transport.rs`: advertises `agentCapabilities.covexcode.lsp = true`. The IDE
  fails closed when it is absent, so older bundled agents never show a broken
  card.
- `covexcode-ide`: `acpClient.ts` carries the `lsp` flag, `lspDiagnostics.ts`
  parses the tool's plain-text diagnostics and location rows, and
  `extension.ts` renders the cards and registers `covexcode.openDiagnostic`.
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
4. **IDE surface** — implemented in `covexcode-ide`: a completed `lsp`
   diagnostics tool call renders a severity summary and up to ten
   `path:line:column` links; definition and references results render their
   rows the same way (`Definition`, `N references`). Each link opens the file
   at the position via `covexcode.openDiagnostic`. Gated on
   `agentCapabilities.covexcode.lsp`, deduped per turn, and the
   server-provided message is appended as text rather than parsed as
   Markdown. Hover is left in the tool row: arbitrary server markup has no
   safe compact rendering.

## Testing

- A fake LSP server embedded in `src/lsp.rs` tests (a small Python stdio
  program) answers `initialize`, `didOpen`, `definition`, `references`,
  `hover`, `documentSymbol`, and emits diagnostics; tests spawn it through a
  real `LspServerConfig`. They skip when no Python interpreter is available.
- Timeout: a server that accepts the document but never publishes diagnostics
  reports `timed_out` after the wait window, never a clean empty list.
- Framing bounds: an oversized declared message and an endless header line
  both stop the reader cleanly instead of allocating.
- Crash/restart: a server that exits is reported rather than panicking; a
  killed client is forgotten and the next call starts a fresh process.
- UTF-16 position tests with emoji and combining characters.
- Config tests for defaults and extension mapping.
- Process reuse: repeated calls leave exactly one client entry and reuse the
  same `Arc`, so a session does not pay `initialize` per tool call.

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
