# Multi-agent CLI architecture

Status: Phase 0 complete; Phase 1 complete; Phase 2 complete (JSONL store);
Phase 3 started; Phase 4 partial  
Date: 2026-11-06  
Scope: `HolboxAI/covexcode` (the Rust CLI and agent runtime)

## Implementation status

Landed on `main`:

- **Phase 0**: one pooled HTTP client for model requests; opt-in
  `COVEXCODE_TRACE` request timing.
- **Phase 1**: domain identities (`SessionId`, `AgentId`, `TurnId`, `TaskId`,
  statuses); a transport-agnostic `SessionEvent` stream covering text, tool
  lifecycle, usage, status, plans/todos, and approvals; cooperative ACP
  `session/cancel`; `HeadlessSession` is the reusable engine behind both
  `--acp` and `--listen`, with `SessionCommand` (cancel + approval answers) as
  its serializable control surface. The TUI still runs its own `App` loop by
  default; `COVEXCODE_CORE_SESSION=1` drives it through the engine instead
  (`session_client` + `core_tui`), with streaming text, tool results, usage,
  cancel, approval prompts, subagent trails, and every command that changes
  the conversation or its endpoint: `/new`, `/compact`, `/rollback`, `/diff`,
  `/subagents`, `/provider`, `/model`, and resuming (`/resume`, `/sessions`,
  startup `--resume`). Phase 1's TUI-as-client exit criterion is met; the old
  path stays as the default until the flag is retired.
- **Phase 2**: a `SessionRegistry` with one actor per session, shared by every
  connection a `--listen` process accepts. `session/list`, `session/attach`,
  `session/close` and `session/cancel` work across connections, status changes
  are mirrored into the registry, and approval and browser asks fan out to
  every attached connection. Conversations are persisted to
  `~/.covexcode/sessions` at round boundaries, compaction rotates to a fresh
  file, and `session/recordings` / `session/resume` list and reload them.
- **Phase 3 (partial)**: independent `write_file`/`edit_file` calls to
  distinct files run concurrently in both the TUI batch runner and the
  engine's auto-approved path (same-file repeats still serialize, and
  non-write calls fence the run), and a process-wide per-path write lock
  extends that ordering across sessions sharing a workspace. `[tools]
  persistent_shell` opts into one long-lived `sh` per workspace, leased one
  command at a time and falling back to the one-shot path when busy; a timeout
  kills it. The sandbox policy is still pending.
- **Phase 4 (partial)**: the read-only `explore` subagent is shared by the TUI
  and the ACP engine; sibling `agent` calls run concurrently (default 4); the
  engine forwards each child tool call as live activity on the parent call;
  `tools.subagent_model` routes children to their own model; project-defined
  read-only agent types load from `.covexcode/agents/*.toml`.

Deliberate deviation: Phase 2's event store is the TUI's JSONL format, not
SQLite. It keeps one store for `/sessions`, the CLI, and ACP; append-only
writes survive a crash mid-turn; and the files stay readable with `cat`. A
database becomes worthwhile when fork/audit/query over events is a real
requirement, not before.

Remaining, in plan order: Phase 3's sandbox policy; Phase 4's write-capable
worktree agents, budgets and agent panel; Phase 5's task DAG and teams. Phase 1 is complete; retiring the
`COVEXCODE_CORE_SESSION` flag (making the engine the TUI's only path and
deleting the old loop) is follow-up cleanup, not parity work.

The bullets in section 1 describe the tree before this work; they are kept as
the baseline the plan was written against.

## 1. Why change

The CLI already does more than its single-loop shape suggests:

- `src/app.rs:14` owns one `AppState` (`AwaitingInput`, `Sending`, `Streaming`,
  `AwaitingApproval`, `ExecutingTools`). One process is one interactive
  conversation.
- `src/main.rs:648` is one event loop. It polls the terminal every 16 ms and
  drains model, deployment, and project channels in that loop.
- `src/app.rs` is 12.5k lines and mixes domain state, prompt history, provider
  selection, approvals, usage, plans, deployment, and rendering decisions.
- `src/headless.rs:180` has a second engine, `HeadlessSession`, for ACP. It
  supports plan mode, rollback journals, and permissions, but explicitly has
  no subagents, no compaction, and no cancellation.
- `src/transport.rs:261` already keeps a `HashMap<SessionId, Sender>`, so ACP
  can host multiple sessions on one connection. `session/cancel` is received
  and ignored (`src/transport.rs:334`), and the TUI does not use this path.
- `src/agent.rs:242` runs up to 16 pure-read tools concurrently. Everything
  else is sequential; `agent` calls are awaited one at a time
  (`src/agent.rs:338`).
- `src/agent.rs:372` has one read-only `explore` subagent. It returns only its
  final report, which is the right shape, but it cannot run alongside other
  subagents and cannot write.
- `src/tools.rs:3364` runs a foreground shell command as a one-shot child with
  `kill_on_drop`; a detached background mode exists, but there is no persistent
  PTY or shell registry.
- `src/llm.rs:548` builds a fresh `reqwest::Client` for every model request.
  That throws away the connection pool and TLS session on every round.
- `src/session.rs` stores one JSONL file per conversation. There is no index,
  event log, fork, or cross-process session registry.

The result is not a Tokio limitation. It is a state-ownership limitation: one
`App`, two divergent engines, and no first-class `Session` / `Agent` / `Task`
objects. Multiple OS processes can run today, but they cannot list, attach to,
steer, or share one another's sessions.

## 2. What the leading tools do

| Tool | Architecture worth borrowing |
| --- | --- |
| **Claude Code** | Subagents are isolated-context workers that return summaries; default 20 concurrent, nesting up to 3, background by default, optional git-worktree isolation. Agent teams add a lead, shared task list, and per-agent mailboxes; they are experimental and token-expensive. Worktrees give each session its own checkout. |
| **Codex CLI** | Rust core with a Thread → Turn → Item domain, an app-server JSON-RPC control plane, and subagents configured with `agents.max_concurrent_threads_per_session`; built-ins are `default`, `worker`, and `explorer`. Subagents inherit the parent's sandbox and approval policy. Newer releases add worktrees. |
| **Gemini CLI** | `packages/cli` frontend plus `packages/core` backend. Subagents run in isolated contexts, can execute in parallel, and are declared as Markdown/YAML under `~/.gemini/agents`, `.gemini/agents`, or an extension. A shadow-git repository checkpoints file changes automatically. |
| **OpenCode** | Client/server split: local HTTP server, SSE event stream, OpenAPI, session-addressed prompts, async prompt submission, per-session abort, agents, permissions, and LSP. The TUI is a client of the server. |
| **ACP** | A single ACP connection can carry multiple concurrent sessions, each identified by `SessionId`. `session/cancel` is a required method and is scoped to one session. |

The consensus is consistent: **a session-oriented core behind a transport, with
the TUI as a client**, not a monolithic UI loop that owns the agent.

## 3. Target architecture

```
TUI ─┐
CLI ─┼─ JSON-RPC / ACP / SSE ─ SessionManager (in-process or daemon)
IDE ─┘                          ├─ Session actor A
                                │   ├─ Turn task → provider stream
                                │   ├─ Tool scheduler (parallel reads, serialized writes)
                                │   └─ Agent supervisor → child agent sessions
                                ├─ Session actor B
                                └─ SQLite event store · worktrees · sandboxes
```

### 3.1 Crates

| Crate | Responsibility |
| --- | --- |
| `cx-domain` | IDs, commands, events, session/agent/task model, policies |
| `cx-session` | session actor, turn state machine, agent loop |
| `cx-provider` | provider adapters, pooled HTTP client, streaming, caching |
| `cx-tools` | `Tool` trait and registry, fs/shell/web/MCP tools |
| `cx-sandbox` | sandbox policy, OS enforcement, PTY/shell manager |
| `cx-orchestrator` | task DAG, subagents, worktrees, merge/verify |
| `cx-store` | SQLite event store, checkpoints, usage/cost |
| `cx-proto` | ACP/JSON-RPC/SSE schemas |
| `cx-server` | session manager, stdio ACP, local HTTP/SSE |
| `cx-tui` | ratatui client |
| `cx-cli` | entry points and flags |

The current single binary can be extracted into these modules first and split
into workspace crates once the boundaries stop moving. A big-bang crate split
is not a prerequisite for the runtime work.

### 3.2 Domain model

```rust
SessionId, AgentId, TurnId, TaskId

enum Command {
    Prompt { session, text, images, mode },
    Cancel { session, turn },
    Approve { session, request_id, decision },
    Steer { session, text },
    SpawnAgent { parent, role, task, isolation },
    Close { session },
}

enum Event {
    TurnStarted,
    TokenDelta,
    ToolCallProposed,
    ApprovalRequested,
    ToolStarted,
    ToolFinished,
    AgentSpawned,
    TaskChanged,
    TurnFinished,
}
```

Commands enter a per-session bounded `mpsc`; the session actor owns mutable
state and never shares it behind a lock. Events leave on a `broadcast` channel
and are appended to the event store. That single decision enables resume, fork,
multi-client attach, audit, crash recovery, and agent-panel UIs.

### 3.3 Runtime rules

- One session actor per session. Turns are serialized **per session** by
  default; different sessions run concurrently.
- A turn task owns a `CancellationToken`. Cancellation drops the provider
  stream and kills tool process groups, not just the future.
- Global and per-provider semaphores bound requests. Per-session limits bound
  child agents and concurrent tools.
- Reads use `JoinSet` plus a semaphore (keep the current 16). Writes serialize
  per file or per worktree. Child agents use a bounded `JoinSet`.
- Sessions can be attached or detached. `covexcode serve` makes them outlive the
  TUI; start in-process and add the daemon without changing the engine.

## 4. Multi-agent model

Build in this order:

1. **Subagents.** A parent spawns isolated-context child sessions with their
   own model, tool allowlist, token/step budget, and sandbox. Read-only
   explorers share the workspace and run in parallel. Write agents receive a
   git worktree (or overlay), return a patch plus summary, and are merged by
   the parent or an integrator. Default 4–6 concurrent, depth 2–3, both
   configurable.
2. **Task DAG.** A planner emits tasks with dependencies and owned paths. A
   scheduler claims tasks atomically (`UPDATE ... WHERE status='pending'` in
   SQLite), dispatches them, and unblocks dependents. A verifier/test agent
   runs after implementation.
3. **Lead and teammates.** A shared task list plus a mailbox, with one lead
   coordinating peers. Make this opt-in and experimental; it has much higher
   token and coordination cost. Do not start here.

### 4.1 Isolation and approvals

- `SandboxPolicy`: read-only, workspace-write, danger-full-access. A child
  inherits its parent's policy and may only tighten it.
- Approval requests carry `session_id` and `agent_id`. Prompts route to the
  parent/lead, and an agent can never approve for the user or for another agent.
- Per-agent tool scopes: `explore` gets read/grep/glob; `implement` gets writes
  only inside its worktree; `review` gets read-only diff access.
- Worktree merge queue: implement agents branch from a known commit, tests run
  in their worktree, then the integrator merges in dependency order and
  resolves conflicts.

### 4.2 Shells

Replace the one-shot-only path with a `ShellManager`:

- one PTY-backed shell per agent/session, with a stable cwd and environment;
- command IDs, streamed output, timeouts, maximum output, process-group kill;
- background jobs attached to the session, not the process;
- read-only commands may share a shell; write agents get their own worktree and
  shell;
- per-command approval uses the same policy engine as file writes.

## 5. Speed plan

| Change | Why it matters | Current location |
| --- | --- | --- |
| Reuse one HTTP client | Avoids a TLS handshake and pool rebuild every model round | `src/llm.rs:548` |
| Parallel `agent` calls | Today each subagent is awaited sequentially | `src/agent.rs:338` |
| Per-file write locks + scheduler | Makes independent edits safe to parallelize | tool runtime |
| Persistent PTY shells | Removes process-spawn cost per command | `src/tools.rs:3364` |
| Prompt caching / stable prefix | Keeps system prompt and tool schemas byte-identical; add provider cache controls where supported | provider layer |
| Model routing | Fast model for explore/plan, stronger model for implementation | orchestrator |
| Repo index + LSP | Less context and fewer exploratory turns | new `cx-index` |
| Event-driven UI and async log writer | Removes 16 ms polling and per-tick file I/O | `src/main.rs`, `src/session.rs` |

Measure before and after:

- p50/p95 time-to-first-token;
- turn wall time and tool latency;
- tokens per completed task and cache-hit rate;
- subagent speedup (independent tasks should finish in about `max(t)`, not
  `sum(t)`);
- merge-conflict rate;
- concurrent-session throughput.

Add a `cx-bench` harness and CI performance budgets. Do not claim a speedup
without a before/after trace.

## 6. Migration plan

| Phase | PRs | Exit criteria |
| --- | --- | --- |
| **0. Instrument + quick wins** | 2 | Request/tool traces and baseline methodology; shared HTTP client; no behavior change |
| **1. Core extraction** | 4 | Domain types; `HeadlessSession` becomes `SessionEngine`; TUI renders engine events with feature parity; old `App` path behind a flag |
| **2. Multi-session runtime** | 4 | `SessionManager` + actor per session; ACP `session/cancel`; TUI `/sessions` and attach; SQLite event store/resume |
| **3. Tools and shells** | 3 | Dependency scheduler; per-file locks; PTY shell manager; read-only/workspace sandbox policy |
| **4. Subagents v2** | 4 | Agent definitions; parallel read-only agents; worktree implement/test/review agents; agent panel; budgets |
| **5. Orchestrator/teams** | 3 | Task DAG + atomic claims; lead/mailbox; verifier/merge agent; teams behind an experimental flag |
| **6. Performance/hardening** | continuous | Caching, repo index, model routing, load/fault tests, performance gates |

Every phase is flag-gated (`COVEXCODE_CORE_SESSION`,
`COVEXCODE_MULTI_SESSION`, `COVEXCODE_SUBAGENTS_V2`) and keeps the current path
as a rollback until parity tests pass.

### Phase 0 PRs

1. **Shared HTTP client.** One pooled `reqwest::Client` for model requests.
2. **Instrumentation and baseline.** Request/tool timing spans and a documented
   benchmark harness.

### Phase 1 PRs

1. **Domain identities.** Move `SessionId` out of the ACP protocol into a core
   domain module; add `AgentId`, `TurnId`, `TaskId`, and session status.
2. **Command/event contract.** Define the session command and event envelopes;
   no behavior change.
3. **Engine extraction.** Make `HeadlessSession` the reusable engine and add an
   event sink.
4. **TUI as client.** Render one session through the engine while the old
   `App` path remains behind a flag.

## 7. From-scratch build order

If this were started over, the order would be:

1. `cx-domain` (IDs, commands, events).
2. `cx-session` actor + turn state machine.
3. `cx-provider` pooled client + streaming + caching.
4. `cx-tools` trait/registry + approvals.
5. `cx-server` ACP/JSON-RPC + local HTTP/SSE.
6. `cx-tui` client.
7. Subagents + worktrees.
8. Task DAG/teams.

The existing approval seam, rollback journal, prompt-cache-aware prompt order,
and ACP multi-session router are valuable. A strangler-fig extraction is lower
risk and faster than a rewrite.

## 8. Decisions to settle

- Default subagent concurrency (start at 4–6) and nesting depth (start at 2).
- Whether write-capable agents always require worktrees (recommended: yes).
- Whether the daemon is opt-in (`covexcode serve`) or the default host.
- Whether team mode is a separate experimental surface after subagents.
- Which provider cache controls to support first (OpenAI-compatible automatic
  prefix caching, DeepSeek cache, Anthropic `cache_control`).
- Minimum model set for routing (fast explorer, strong implementer, verifier).

## References

- [Claude Code — Run agents in parallel](https://code.claude.com/docs/en/agents)
- [Claude Code — Subagents](https://code.claude.com/docs/en/sub-agents.md)
- [Claude Code — Agent teams](https://code.claude.com/docs/en/agent-teams.md)
- [Codex — Subagents](https://learn.chatgpt.com/docs/agent-configuration/subagents)
- [Gemini CLI — Subagents announcement](https://developers.googleblog.com/subagents-have-arrived-in-gemini-cli/)
- [Gemini CLI — Core architecture](https://geminicli.com/docs/core/)
- [Gemini CLI — Architecture overview](https://github.com/google-gemini/gemini-cli/blob/main/docs/architecture.md)
- [OpenCode — Server API](https://opencode.ai/docs/server/)
- [OpenCode internals (community documentation)](https://github.com/bgauryy/open-docs/tree/main/docs/opencode)
- [Agent Client Protocol — Cancellation](https://agentclientprotocol.com/protocol/v2/cancellation)
- [Agent Client Protocol — v1 schema](https://github.com/agentclientprotocol/agent-client-protocol/blob/main/docs/protocol/v1/draft/schema.mdx)
