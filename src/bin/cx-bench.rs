/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! `cx-bench` -- the repeatable harness behind Phase 6's performance work.
//!
//! It drives the real `HeadlessSession` against a local mock LLM, so every
//! number includes the engine's actual serialization, event fan-out, tool
//! dispatch and persistence paths -- not a microbenchmark of one function.
//! The mock makes runs deterministic: no network, no provider variance, and
//! a fixed amount of simulated model latency.
//!
//! Scenarios:
//! - `stream`: time to first token and whole-turn time for a streamed reply.
//! - `tool`: a `read_file` round before the answer; tool latency and turn time.
//! - `subagents`: two sibling `agent` calls, run with `subagent_concurrency`
//!   1 and 2. The speedup is the number that matters: independent children
//!   should take `max(t)`, not `sum(t)`.
//! - `sessions`: one versus eight sessions answered concurrently, reported as
//!   throughput. This is the actor-per-session path, not the TUI.
//!
//! `--check` turns the scaling ratios and generous hang budgets into an exit
//! code. It deliberately does not compare absolute wall-clock numbers across
//! machines: a shared CI runner is not a performance laboratory. The budgets
//! catch serialization regressions and hangs, while the JSON output is what a
//! before/after comparison should be made from.
//!
//! Run it with:
//!
//! ```text
//! cargo run --features bench --bin cx-bench -- --check
//! cargo run --features bench --bin cx-bench -- --json
//! ```
//!
//! The bench is feature-gated so ordinary builds and the release matrix never
//! compile it. It redirects `HOME`, `USERPROFILE` and `COVEXCODE_CONFIG_DIR`
//! to a temporary directory before touching the engine, so it can never write
//! to the real session store, usage log or config.

#[path = "../approval.rs"]
mod approval;
#[path = "../artifacts.rs"]
mod artifacts;
#[path = "../auth.rs"]
mod auth;
#[path = "../backend.rs"]
mod backend;
#[path = "../compact.rs"]
mod compact;
#[path = "../config.rs"]
mod config;
#[path = "../contrast.rs"]
mod contrast;
#[path = "../danger.rs"]
mod danger;
#[path = "../dateutil.rs"]
mod dateutil;
#[path = "../db.rs"]
mod db;
#[path = "../deploy/mod.rs"]
mod deploy;
#[path = "../diff.rs"]
mod diff;
#[path = "../domain.rs"]
mod domain;
#[path = "../errors.rs"]
mod errors;
#[path = "../file_locks.rs"]
mod file_locks;
#[path = "../headless.rs"]
mod headless;
#[path = "../llm.rs"]
mod llm;
#[path = "../locked_store.rs"]
mod locked_store;
#[path = "../mailbox.rs"]
mod mailbox;
#[path = "../mcp.rs"]
mod mcp;
#[path = "../message.rs"]
mod message;
#[path = "../notice.rs"]
mod notice;
#[path = "../plan.rs"]
mod plan;
#[path = "../projects.rs"]
mod projects;
#[path = "../protocol.rs"]
mod protocol;
#[path = "../providers.rs"]
mod providers;
#[path = "../quota.rs"]
mod quota;
#[path = "../read_policy.rs"]
mod read_policy;
#[path = "../requests.rs"]
mod requests;
#[path = "../rollback.rs"]
mod rollback;
#[path = "../session.rs"]
mod session;
#[path = "../shell.rs"]
mod shell;
#[path = "../subagent.rs"]
mod subagent;
#[path = "../tasks.rs"]
mod tasks;
#[path = "../teammates.rs"]
mod teammates;
#[cfg(feature = "cli")]
#[path = "../theme.rs"]
mod theme;
#[path = "../tools.rs"]
mod tools;
#[path = "../usage.rs"]
mod usage;
#[path = "../workspace.rs"]
mod workspace;
#[path = "../worktree.rs"]
mod worktree;

use config::Config;
use domain::{SessionEvent, ToolStatus};
use headless::HeadlessSession;
use protocol::SessionId;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use workspace::Workspace;

const VERSION: &str = env!("CARGO_PKG_VERSION");

const STREAM_TOKENS: usize = 48;
const CHILD_DELAY: Duration = Duration::from_millis(120);
const SESSION_DELAY: Duration = Duration::from_millis(60);
const HANG_BUDGET: Duration = Duration::from_secs(5);

static REQUEST_SEQ: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------- mock LLM

/// One streamed SSE event, held for `delay` before it is written.
struct Chunk {
    payload: String,
    delay: Duration,
}

/// One complete mock response. `delay` is the simulated think time before the
/// response headers go out; per-chunk delays simulate streaming cadence.
struct Reply {
    delay: Duration,
    chunks: Vec<Chunk>,
    /// `(prompt, completion, cached)` as the mock endpoint reports it.
    usage: Option<(u64, u64, u64)>,
}

impl Reply {
    fn text(text: &str) -> Self {
        Self {
            delay: Duration::ZERO,
            chunks: vec![Chunk {
                payload: content_event(text),
                delay: Duration::ZERO,
            }],
            usage: None,
        }
    }

    fn streamed(tokens: usize, inter_chunk: Duration) -> Self {
        let chunks = (0..tokens)
            .map(|i| Chunk {
                payload: content_event(&format!("tok{i} ")),
                delay: inter_chunk,
            })
            .collect();
        Self {
            delay: Duration::ZERO,
            chunks,
            usage: None,
        }
    }

    fn tool_calls(calls: &[(&str, serde_json::Value)]) -> Self {
        Self {
            delay: Duration::ZERO,
            chunks: vec![Chunk {
                payload: tool_calls_event(calls),
                delay: Duration::ZERO,
            }],
            usage: None,
        }
    }

    /// Report token usage the way an OpenAI-compatible endpoint does: one
    /// final chunk with no choices. `cached` is the cached share of the
    /// prompt, so the report can show a cache-hit rate.
    fn with_usage(mut self, prompt: u64, completion: u64, cached: u64) -> Self {
        self.usage = Some((prompt, completion, cached));
        self
    }
}

fn content_event(text: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"choices": [{"delta": {"content": text}}]})
    )
}

fn usage_event(prompt: u64, completion: u64, cached: u64) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "choices": [],
            "usage": {
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "total_tokens": prompt + completion,
                "prompt_tokens_details": { "cached_tokens": cached },
            }
        })
    )
}

fn tool_calls_event(calls: &[(&str, serde_json::Value)]) -> String {
    let calls: Vec<serde_json::Value> = calls
        .iter()
        .enumerate()
        .map(|(index, (name, arguments))| {
            json!({
                "index": index,
                "id": format!("call_{}", index + 1),
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": arguments.to_string(),
                }
            })
        })
        .collect();
    format!(
        "data: {}\n\n",
        json!({"choices": [{"delta": {"tool_calls": calls}}]})
    )
}

type Handler = Arc<dyn Fn(&str) -> Reply + Send + Sync>;

/// Start a concurrent mock OpenAI-compatible endpoint. Each connection is
/// handled on its own task, which is what lets the concurrency scenarios ask
/// whether *the engine* overlaps work rather than whether the mock does.
async fn start_mock(handler: Handler) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let addr = listener.local_addr().expect("mock address");
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let request = read_request(&mut socket).await;
                let reply = handler(&request);
                if !reply.delay.is_zero() {
                    tokio::time::sleep(reply.delay).await;
                }
                let headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
                               Connection: close\r\n\r\n";
                if socket.write_all(headers.as_bytes()).await.is_err() {
                    return;
                }
                for chunk in reply.chunks {
                    if socket.write_all(chunk.payload.as_bytes()).await.is_err() {
                        return;
                    }
                    let _ = socket.flush().await;
                    if !chunk.delay.is_zero() {
                        tokio::time::sleep(chunk.delay).await;
                    }
                }
                if let Some((prompt, completion, cached)) = reply.usage {
                    if socket
                        .write_all(usage_event(prompt, completion, cached).as_bytes())
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = socket.write_all(b"data: [DONE]\n\n").await;
                let _ = socket.shutdown().await;
            });
        }
    });
    format!("http://{addr}")
}

async fn read_request(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = socket.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(header_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&buf[..header_end]).to_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(0);
            if buf.len() >= header_end + 4 + length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

// ---------------------------------------------------------------- one turn

struct Turn {
    ttft: Duration,
    total: Duration,
    tool_latency: Option<Duration>,
    prompt_tokens: u64,
    completion_tokens: u64,
    cached_tokens: u64,
}

fn bench_config(endpoint: &str) -> Config {
    let mut config = Config::default();
    config.llm.endpoint = endpoint.to_string();
    config.llm.model = "cx-bench-model".to_string();
    config.llm.api_key = "sk-bench".to_string();
    config.llm.max_tokens = 4096;
    config.quota.enabled = false;
    config
}

fn next_session_id() -> SessionId {
    SessionId(format!(
        "bench-{}-{}",
        std::process::id(),
        REQUEST_SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Run one prompt through a fresh engine session and time the parts that
/// matter. The event channel is read while the prompt future runs, so TTFT is
/// measured from the caller's perspective, not reconstructed after the fact.
async fn run_turn(workspace: Workspace, config: Config, prompt: &str) -> Turn {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut session = HeadlessSession::new(next_session_id(), workspace, config);
    session.set_event_sink(events_tx);

    // Dropping the receivers is deliberate: this harness never approves a
    // prompt-gated action, and a closed channel makes the engine fail such a
    // send immediately instead of filling a buffer and stalling the turn.
    let (updates_tx, updates_rx) = mpsc::channel(8);
    drop(updates_rx);
    let (permissions_tx, permissions_rx) = mpsc::channel(1);
    drop(permissions_rx);
    let (browser_tx, browser_rx) = mpsc::channel(1);
    drop(browser_rx);
    let (interact_tx, interact_rx) = mpsc::channel(1);
    drop(interact_rx);

    let start = Instant::now();
    let prompt = prompt.to_string();
    let handle = tokio::spawn(async move {
        session
            .prompt(
                prompt,
                Vec::new(),
                None,
                &updates_tx,
                &permissions_tx,
                &browser_tx,
                &interact_tx,
            )
            .await
    });

    let mut ttft = None;
    let mut tool_starts: HashMap<String, Instant> = HashMap::new();
    let mut tool_latency = None;
    let mut prompt_tokens = 0;
    let mut completion_tokens = 0;
    let mut cached_tokens = 0;
    while let Some(event) = events_rx.recv().await {
        match event {
            SessionEvent::TextDelta { .. } => {
                ttft.get_or_insert_with(|| start.elapsed());
            }
            SessionEvent::ToolCallStarted { tool_call_id, .. } => {
                tool_starts.insert(tool_call_id, Instant::now());
            }
            SessionEvent::ToolCallUpdated {
                tool_call_id,
                status: Some(ToolStatus::Completed | ToolStatus::Failed),
                ..
            } => {
                if let Some(began) = tool_starts.remove(&tool_call_id) {
                    tool_latency =
                        Some(tool_latency.unwrap_or(Duration::ZERO).max(began.elapsed()));
                }
            }
            SessionEvent::UsageUpdated {
                used,
                prompt_tokens: prompt_side,
                cached_tokens: cached_side,
                ..
            } => {
                let prompt = prompt_side.unwrap_or(0);
                prompt_tokens += prompt;
                completion_tokens += used.saturating_sub(prompt);
                cached_tokens += cached_side.unwrap_or(0);
            }
            SessionEvent::TurnFinished { .. } => break,
            _ => {}
        }
    }
    let stop = handle.await.expect("benchmark turn task");
    assert_eq!(
        stop,
        protocol::StopReason::EndTurn,
        "benchmark turn ended without an answer: {stop:?}"
    );
    let total = start.elapsed();
    Turn {
        ttft: ttft.unwrap_or(total),
        total,
        tool_latency,
        prompt_tokens,
        completion_tokens,
        cached_tokens,
    }
}

// ---------------------------------------------------------------- stats

#[derive(Clone, Copy)]
struct Summary {
    p50: Duration,
    p95: Duration,
    max: Duration,
}

fn summarize(mut values: Vec<Duration>) -> Summary {
    values.sort();
    let pick = |percent: f64| {
        if values.is_empty() {
            return Duration::ZERO;
        }
        let rank = ((values.len() as f64) * percent).ceil() as usize;
        values[rank.saturating_sub(1).min(values.len() - 1)]
    };
    Summary {
        p50: pick(0.50),
        p95: pick(0.95),
        max: values.last().copied().unwrap_or_default(),
    }
}

fn millis(value: Duration) -> f64 {
    value.as_secs_f64() * 1000.0
}

fn micros(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000_000.0
}

/// Best of N for the scaling comparisons. The question these checks answer is
/// "can the engine overlap this work at all", and one delayed scheduling slice
/// makes an average lie about capacity. A regression to serialization raises
/// every sample, so the minimum still catches it.
fn best(values: Vec<Duration>) -> Duration {
    values.into_iter().min().unwrap_or_default()
}

// ---------------------------------------------------------------- scenarios

#[derive(Clone, Copy, Default)]
struct TokenSummary {
    prompt: f64,
    completion: f64,
    cached: f64,
}

async fn stream_scenario(
    workspace: &Workspace,
    config: &Config,
    runs: usize,
) -> (Summary, Summary, TokenSummary) {
    let mut ttft = Vec::with_capacity(runs);
    let mut total = Vec::with_capacity(runs);
    let mut tokens = TokenSummary::default();
    for _ in 0..runs {
        let turn = run_turn(workspace.clone(), config.clone(), "stream a short answer").await;
        ttft.push(turn.ttft);
        total.push(turn.total);
        tokens.prompt += turn.prompt_tokens as f64;
        tokens.completion += turn.completion_tokens as f64;
        tokens.cached += turn.cached_tokens as f64;
    }
    let runs = runs.max(1) as f64;
    (
        summarize(ttft),
        summarize(total),
        TokenSummary {
            prompt: tokens.prompt / runs,
            completion: tokens.completion / runs,
            cached: tokens.cached / runs,
        },
    )
}

async fn tool_scenario(workspace: &Workspace, config: &Config, runs: usize) -> (Summary, Summary) {
    let mut latency = Vec::with_capacity(runs);
    let mut total = Vec::with_capacity(runs);
    for _ in 0..runs {
        let turn = run_turn(workspace.clone(), config.clone(), "read hello.txt").await;
        latency.push(turn.tool_latency.unwrap_or_default());
        total.push(turn.total);
    }
    (summarize(latency), summarize(total))
}

async fn subagent_once(workspace: &Workspace, base: &Config, concurrency: usize) -> Duration {
    let mut config = base.clone();
    config.tools.subagent_concurrency = concurrency;
    run_turn(workspace.clone(), config, "research two things")
        .await
        .total
}

async fn concurrent_once(workspace: &Workspace, base: &Config, sessions: usize) -> Duration {
    let mut tasks = Vec::with_capacity(sessions);
    let start = Instant::now();
    for _ in 0..sessions {
        let workspace = workspace.clone();
        let config = base.clone();
        tasks.push(tokio::spawn(async move {
            run_turn(workspace, config, "hello").await.total
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    start.elapsed()
}

// ---------------------------------------------------------------- conflicts

#[derive(Clone, Copy, Default)]
struct ConflictStats {
    overlap_conflicts: usize,
    overlap_total: usize,
    disjoint_conflicts: usize,
    disjoint_total: usize,
}

fn git_in(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("git {args:?} failed to start: {e}"));
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn commit_base(dir: &Path, content: &str) -> String {
    git_in(dir, &["init", "-q"]);
    git_in(dir, &["config", "user.email", "bench@example.com"]);
    git_in(dir, &["config", "user.name", "cx-bench"]);
    std::fs::write(dir.join("a.txt"), content).expect("write base file");
    git_in(dir, &["add", "-A"]);
    git_in(dir, &["commit", "-qm", "base"]);
    git_in(dir, &["rev-parse", "HEAD"])
}

/// Two synthetic patches whose outcomes are known: one pair edits the same
/// line (must conflict), the other edits opposite ends of one file (must
/// apply cleanly through the three-way fallback). This exercises the same
/// worktree/snapshot/apply path a writing subagent uses, without a model.
///
/// It is a regression harness for the *apply* path, not a claim about how
/// often real agents collide; that needs recorded runs.
fn conflict_scenario(runs: usize) -> ConflictStats {
    const BASE: &str = "one\ntwo\nthree\nfour\nfive\nsix\n";
    let dir = temporary_dir("conflicts");
    let base = commit_base(&dir, BASE);
    let worktrees = dir.join("worktrees");
    std::fs::create_dir_all(&worktrees).expect("worktrees dir");
    let mut stats = ConflictStats::default();

    for index in 0..runs {
        for overlap in [true, false] {
            let first = worktree::Worktree::create_at(
                &worktrees,
                &dir,
                &format!("first-{index}-{overlap}"),
                &base,
            )
            .expect("first worktree");
            let second = worktree::Worktree::create_at(
                &worktrees,
                &dir,
                &format!("second-{index}-{overlap}"),
                &base,
            )
            .expect("second worktree");
            let target = worktree::Worktree::create_at(
                &worktrees,
                &dir,
                &format!("target-{index}-{overlap}"),
                &base,
            )
            .expect("target worktree");

            let (first_text, second_text) = if overlap {
                (
                    "one\nFIRST\nthree\nfour\nfive\nsix\n",
                    "one\nSECOND\nthree\nfour\nfive\nsix\n",
                )
            } else {
                (
                    "FIRST\ntwo\nthree\nfour\nfive\nsix\n",
                    "one\ntwo\nthree\nfour\nfive\nSECOND\n",
                )
            };
            std::fs::write(first.path().join("a.txt"), first_text).expect("first edit");
            std::fs::write(second.path().join("a.txt"), second_text).expect("second edit");
            let first_patch = first.diff_against(&base).expect("first diff");
            let second_patch = second.diff_against(&base).expect("second diff");

            target
                .apply_with_conflicts(&first_patch)
                .expect("first applies");
            let conflicted = target
                .apply_with_conflicts(&second_patch)
                .expect("second applies or conflicts");
            if overlap {
                stats.overlap_total += 1;
                stats.overlap_conflicts += usize::from(conflicted);
            } else {
                stats.disjoint_total += 1;
                stats.disjoint_conflicts += usize::from(conflicted);
            }

            first.remove();
            second.remove();
            target.remove();
        }
    }
    std::fs::remove_dir_all(&dir).ok();
    stats
}

// ---------------------------------------------------------------- main

struct Args {
    check: bool,
    json: bool,
    quick: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        check: false,
        json: false,
        quick: false,
    };
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--check" => args.check = true,
            "--json" => args.json = true,
            "--quick" => args.quick = true,
            "-h" | "--help" => {
                println!(
                    "cx-bench {VERSION}\n\n\
                     Usage: cx-bench [--check] [--json] [--quick]\n\n\
                     --check  Fail the process when scaling or hang budgets regress\n\
                     --json   Print the raw measurements as JSON\n\
                     --quick  Fewer samples (the CI smoke run)"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(args)
}

fn temporary_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "covexcode-bench-{}-{}-{label}",
        std::process::id(),
        REQUEST_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("create temporary directory");
    dir
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    // Isolate every file the engine may touch before constructing a session.
    let home = temporary_dir("home");
    let config_dir = home.join(".covexcode");
    std::fs::create_dir_all(&config_dir)?;
    std::env::set_var("HOME", &home);
    std::env::set_var("USERPROFILE", &home);
    std::env::set_var("COVEXCODE_CONFIG_DIR", &config_dir);

    let workspace_dir = temporary_dir("workspace");
    std::fs::write(workspace_dir.join("hello.txt"), "hello\n")?;
    let workspace = Workspace::new(&workspace_dir)?;

    let stream_runs = if args.quick { 10 } else { 20 };
    let scaling_runs = if args.quick { 2 } else { 3 };
    let session_runs = if args.quick { 2 } else { 3 };

    // Streaming: a fixed cadence is what makes TTFT exercise the live path
    // instead of a single buffered write.
    let stream_endpoint = start_mock(Arc::new(|_| {
        Reply::streamed(STREAM_TOKENS, Duration::from_millis(1)).with_usage(
            1200,
            STREAM_TOKENS as u64,
            600,
        )
    }))
    .await;
    let stream_config = bench_config(&stream_endpoint);
    let (stream_ttft, stream_total, stream_tokens) =
        stream_scenario(&workspace, &stream_config, stream_runs).await;

    // A read-only tool round: first request asks for read_file, the one after
    // the tool result answers in text. No approval prompt is involved, so the
    // engine's auto-approved fan-out path is what is measured.
    let tool_endpoint = start_mock(Arc::new(|request| {
        if request.contains("\"role\":\"tool\"") {
            Reply::text("hello.txt says hello")
        } else {
            Reply::tool_calls(&[(tools::READ_FILE, json!({"path": "hello.txt"}))])
        }
    }))
    .await;
    let tool_config = bench_config(&tool_endpoint);
    let (tool_latency, tool_total) = tool_scenario(&workspace, &tool_config, stream_runs).await;

    // Two sibling subagents. The child is recognised by its system prompt;
    // the parent's final answer is the request that carries a tool result.
    let subagent_endpoint = start_mock(Arc::new(|request| {
        if request.contains("read-only research subagent") {
            Reply {
                delay: CHILD_DELAY,
                chunks: vec![Chunk {
                    payload: content_event("child report"),
                    delay: Duration::ZERO,
                }],
                usage: None,
            }
        } else if request.contains("\"role\":\"tool\"") {
            Reply::text("both reports are in")
        } else {
            Reply::tool_calls(&[
                (tools::AGENT, json!({"task": "find the first thing"})),
                (tools::AGENT, json!({"task": "find the second thing"})),
            ])
        }
    }))
    .await;
    let subagent_config = bench_config(&subagent_endpoint);
    let mut serial = Vec::new();
    let mut parallel = Vec::new();
    for _ in 0..scaling_runs {
        serial.push(subagent_once(&workspace, &subagent_config, 1).await);
        parallel.push(subagent_once(&workspace, &subagent_config, 2).await);
    }
    let serial = best(serial);
    let parallel = best(parallel);
    let speedup = serial.as_secs_f64() / parallel.as_secs_f64().max(f64::MIN_POSITIVE);

    // One session versus eight. The mock holds each response for a fixed
    // delay; the wall-clock difference is the engine's overlap.
    let session_endpoint = start_mock(Arc::new(|_| Reply {
        delay: SESSION_DELAY,
        chunks: vec![Chunk {
            payload: content_event("ok"),
            delay: Duration::ZERO,
        }],
        usage: None,
    }))
    .await;
    let session_config = bench_config(&session_endpoint);
    let mut one = Vec::new();
    let mut eight = Vec::new();
    for _ in 0..session_runs {
        one.push(concurrent_once(&workspace, &session_config, 1).await);
        eight.push(concurrent_once(&workspace, &session_config, 8).await);
    }
    let one = best(one);
    let eight = best(eight);
    let throughput_speedup = (8.0 / eight.as_secs_f64().max(f64::MIN_POSITIVE))
        / (1.0 / one.as_secs_f64().max(f64::MIN_POSITIVE));

    let conflict_runs = if args.quick { 3 } else { 5 };
    let conflicts = conflict_scenario(conflict_runs);

    let subagent_ok = speedup >= if args.quick { 1.25 } else { 1.4 };
    let sessions_ok = throughput_speedup >= 2.0;
    let stream_ok = stream_total.p95 < HANG_BUDGET;
    let tool_ok = tool_total.p95 < HANG_BUDGET;
    let all_ok = subagent_ok && sessions_ok && stream_ok && tool_ok;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "version": VERSION,
                "quick": args.quick,
                "stream": {
                    "ttft_ms": {"p50": micros(stream_ttft.p50) / 1000.0, "p95": micros(stream_ttft.p95) / 1000.0, "max": micros(stream_ttft.max) / 1000.0},
                    "turn_ms": {"p50": millis(stream_total.p50), "p95": millis(stream_total.p95), "max": millis(stream_total.max)},
                    "tokens": {
                        "prompt": stream_tokens.prompt,
                        "completion": stream_tokens.completion,
                        "cached": stream_tokens.cached,
                        "cache_hit_rate": if stream_tokens.prompt > 0.0 { stream_tokens.cached / stream_tokens.prompt } else { 0.0 },
                    },
                },
                "tool": {
                    "latency_ms": {"p50": millis(tool_latency.p50), "p95": millis(tool_latency.p95), "max": millis(tool_latency.max)},
                    "turn_ms": {"p50": millis(tool_total.p50), "p95": millis(tool_total.p95), "max": millis(tool_total.max)},
                },
                "subagents": {
                    "serial_ms": millis(serial),
                    "parallel_ms": millis(parallel),
                    "speedup": speedup,
                },
                "sessions": {
                    "one_ms": millis(one),
                    "eight_ms": millis(eight),
                    "throughput_speedup": throughput_speedup,
                },
                "conflicts": {
                    "overlap_conflicts": conflicts.overlap_conflicts,
                    "overlap_total": conflicts.overlap_total,
                    "disjoint_conflicts": conflicts.disjoint_conflicts,
                    "disjoint_total": conflicts.disjoint_total,
                },
                "checks": {
                    "subagent_speedup": subagent_ok,
                    "session_throughput": sessions_ok,
                    "stream_hang": stream_ok,
                    "tool_hang": tool_ok,
                    "ok": all_ok,
                },
            }))?
        );
    } else {
        println!(
            "cx-bench {VERSION}{}",
            if args.quick { " (quick)" } else { "" }
        );
        println!(
            "stream     ttft p50 {:>7.2}ms  p95 {:>7.2}ms   turn p50 {:>7.2}ms  p95 {:>7.2}ms",
            millis(stream_ttft.p50),
            millis(stream_ttft.p95),
            millis(stream_total.p50),
            millis(stream_total.p95),
        );
        let cache_hit = if stream_tokens.prompt > 0.0 {
            stream_tokens.cached / stream_tokens.prompt * 100.0
        } else {
            0.0
        };
        println!(
            "tokens     prompt {:>6.0}  completion {:>5.0}  cached {:>5.0}  (cache hit {cache_hit:.1}%)",
            stream_tokens.prompt, stream_tokens.completion, stream_tokens.cached,
        );
        println!(
            "tool       latency p50 {:>6.2}ms  p95 {:>6.2}ms   turn p50 {:>7.2}ms  p95 {:>7.2}ms",
            millis(tool_latency.p50),
            millis(tool_latency.p95),
            millis(tool_total.p50),
            millis(tool_total.p95),
        );
        println!(
            "subagents  serial {:>7.1}ms  parallel {:>7.1}ms   speedup {speedup:.2}x",
            millis(serial),
            millis(parallel),
        );
        println!(
            "sessions   1: {:>7.1}ms  8: {:>7.1}ms   throughput {throughput_speedup:.2}x",
            millis(one),
            millis(eight),
        );
        println!(
            "conflicts  overlapping {}/{}   disjoint {}/{}",
            conflicts.overlap_conflicts,
            conflicts.overlap_total,
            conflicts.disjoint_conflicts,
            conflicts.disjoint_total,
        );
        if args.check {
            println!(
                "checks     subagents {}  sessions {}  stream {}  tool {}  => {}",
                pass(subagent_ok),
                pass(sessions_ok),
                pass(stream_ok),
                pass(tool_ok),
                pass(all_ok),
            );
        }
    }

    let _ = std::fs::remove_dir_all(&home);
    let _ = std::fs::remove_dir_all(&workspace_dir);

    if args.check && !all_ok {
        std::process::exit(1);
    }
    Ok(())
}

fn pass(ok: bool) -> &'static str {
    if ok {
        "pass"
    } else {
        "FAIL"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_order_independent() {
        let values = vec![
            Duration::from_millis(5),
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_millis(2),
            Duration::from_millis(4),
        ];
        let summary = summarize(values);
        assert_eq!(summary.p50, Duration::from_millis(3));
        assert_eq!(summary.p95, Duration::from_millis(5));
        assert_eq!(summary.max, Duration::from_millis(5));
    }

    #[test]
    fn the_tool_event_shape_is_valid_sse() {
        let event = tool_calls_event(&[(tools::READ_FILE, json!({"path": "a.txt"}))]);
        assert!(event.starts_with("data: {"));
        assert!(event.ends_with("\n\n"));
        let payload = event.trim().strip_prefix("data: ").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(
            parsed["choices"][0]["delta"]["tool_calls"][0]["function"]["name"],
            tools::READ_FILE
        );
    }
}
