//! The agent loop's mechanics -- fire a request, ingest what streams back,
//! execute what the user allowed -- extracted from `main.rs`'s event loop.
//! Step two of Phase 3 in `upgrade-plan.md`.
//!
//! Nothing here decides anything. `App` still owns the state machine and every
//! approval still flows through `approval.rs`'s seam; these functions are the
//! *plumbing* between `App`, the LLM transport, and the tool runner, gathered
//! in one module so they stop being woven through the UI loop. They are called
//! from exactly the same three points in `run_app` they always ran at, and
//! behavior is deliberately identical.
//!
//! Phase 4 builds on that seam: [`run_subagent`] is the same
//! fire-ingest-execute cycle folded into one function, run as a *child* loop
//! whose "user" is the parent model and whose tools are the read-only slice
//! that needs no approval. It lives here because this module is where the
//! loop's mechanics live; when step three of Phase 3 makes `AgentLoop` a
//! spawned task, parent and child become the same code with different
//! callers.
//!
//! Why this shape: step three of the phase turns this trio into the body of a
//! spawned `AgentLoop` task -- fire, ingest, execute *is* the loop. Once they
//! live here, that move is a change of caller, not of code. The deployment
//! flow is deliberately absent: it takes over the terminal and converses with
//! the user directly, which makes it part of the UI's world, not the agent's.

use crate::app::{App, AppState};
use crate::config::Config;
use crate::llm::{self, ChatMessage, StreamEvent, ToolCall};
use crate::read_policy::is_pure_read_call;
use crate::subagent::run_subagent;
use crate::tools::{self, ToolOutcome};
use crate::workspace::Workspace;
use tokio::sync::mpsc;

/// Fire the request `App` is waiting to send, spawning the streaming task.
/// A no-op unless `app.state` is `Sending` -- the caller does not need to
/// check first.
pub fn fire_request(
    app: &mut App,
    workspace: Option<&Workspace>,
    tx: &mpsc::Sender<(u64, StreamEvent)>,
) {
    if app.state != AppState::Sending {
        return;
    }
    app.request_id += 1;
    // The one place a request actually goes out, so the one place the
    // per-request clock can be honest about when it started.
    app.request_started = Some(std::time::Instant::now());
    let id = app.request_id;
    let endpoint = app.config.llm.endpoint.clone();
    let model = app.config.llm.model.clone();
    let api_key = app.config.llm.api_key.clone();
    // A `/compact` request reserves the summary budget, not the full reply
    // budget: reserving 128K of context for a short summary would make the
    // escape hatch fail on exactly the conversations that need it.
    let max_tokens = if app.compacting {
        app.config
            .llm
            .max_tokens
            .min(crate::compact::SUMMARY_MAX_TOKENS)
    } else {
        app.config.llm.max_tokens
    };
    let context_window = app.config.llm.effective_context_window();
    let temperature = app.config.llm.effective_temperature();

    // Withholding the schemas once the budget is spent is what actually
    // stops a runaway loop: the model has nothing left to call, so it
    // answers. Saying "stop" in the prompt alone would only be a request.
    let budget_left =
        app.config.tools.max_steps == 0 || app.tool_steps < app.config.tools.max_steps;
    // Exact counts make quota and `/usage` real; without them they fall back
    // to the same character estimate `usage.rs` uses. Deliberately not gated
    // on `quota.enabled`: the same stream_options flag also feeds the always-
    // visible cost/usage meter, which must stay truthful with quota off.
    let include_usage = app.config.quota.include_usage;
    // A `/compact` request reads the conversation and writes a summary
    // of it; it has nothing to run. Withholding the schemas is what
    // makes that true rather than merely asked for -- and a tool call
    // here would be worse than useless, since the history that replaces
    // this one has nowhere to put the result it would be owed.
    let (schemas, system) = match workspace {
        _ if app.compacting => (Vec::new(), None),
        Some(ws) => (
            if budget_left {
                // Whether anything in this workspace has ever been
                // published, not just this session -- reads the same
                // on-disk registry `/pull`'s picker does, so a resumed or
                // relaunched session picks the four gated tools back up
                // immediately rather than waiting for a fresh
                // publish_artifact call. `any_published_under`, not
                // `remembered_id`: a project published as a single file
                // registers under that file's own path, never equal to
                // (only nested under) the directory `Workspace::new`
                // resolves it to.
                let published = crate::artifacts::any_published_under(ws.root());
                // File count, not step count: the schema list has to stay
                // byte-identical across rounds of one turn or the prefix
                // cache is busted. A webpage tree withholds `agent`; a
                // codebase at the threshold keeps it.
                tools::schemas_for(
                    app.mode,
                    app.active_plan.is_some(),
                    app.config.deploy.enabled,
                    published,
                    false, // no browser tab in the TUI to fulfill check_in_browser with
                    tools::SchemaDiet::for_workspace(ws.root()),
                )
            } else {
                Vec::new()
            },
            Some(tools::system_prompt(
                ws,
                &app.config.tools,
                app.tool_steps,
                app.mode,
            )),
        ),
        None => (Vec::new(), None),
    };
    // The briefing that changes between rounds of one turn -- plan progress,
    // what is left of the budget -- goes *after* the conversation rather than
    // into the system prompt. Both providers bill a matching prefix at a
    // fraction of the rate, and the prompt is the first thing in that prefix:
    // a step counter up there invalidated the schemas and the whole transcript
    // behind them on every round. See `tools::turn_status`.
    let status = match workspace {
        Some(_) if !app.compacting => {
            tools::turn_status(&app.config.tools, app.tool_steps, app.active_plan.as_ref())
        }
        _ => None,
    };
    let mut history = if app.compacting {
        app.compaction_history()
    } else {
        app.history(system.as_deref())
    };
    if let Some(status) = status {
        history.push(ChatMessage::text("user", status));
    }
    let tx = tx.clone();

    let handle = tokio::spawn(async move {
        llm::stream_chat(
            llm::Target {
                endpoint: &endpoint,
                model: &model,
                api_key: &api_key,
                max_tokens,
                context_window,
                include_usage,
                temperature,
            },
            history,
            schemas,
            id,
            tx,
        )
        .await;
    });

    app.abort = Some(handle.abort_handle());
    app.state = AppState::Streaming;
}

/// Feed one event from the stream (or the tool runner, which reports on the
/// same channel) into `App`. Events for a request that is no longer current
/// -- cancelled, superseded -- are dropped here, so `App` only ever hears
/// about the turn it is actually in.
pub fn handle_event(app: &mut App, id: u64, event: StreamEvent) {
    if id != app.request_id {
        return; // stale: belongs to a cancelled request
    }
    match event {
        StreamEvent::Token(token) => app.append_token(&token),
        StreamEvent::Reasoning(text) => app.append_reasoning(&text),
        StreamEvent::ToolCalls(calls, truncated) => app.request_tools_truncated(calls, truncated),
        StreamEvent::ToolsFinished(outcomes) => app.finish_tools(outcomes),
        StreamEvent::AgentActivity {
            call_id,
            label,
            rounds,
        } => app.record_subagent_activity(&call_id, label, rounds),
        StreamEvent::Usage(u) => app.record_exact_usage(u),
        StreamEvent::Done => app.finish_stream(),
        StreamEvent::Notice(note) => app.note(note),
        StreamEvent::Error(err) => app.fail_stream(err),
    }
}

/// Run the calls the user allowed, spawned off the event loop. A no-op
/// unless `App` has finished its approvals and has something to run.
///
/// Spawned rather than run inline: a command may take a minute, and doing
/// it on the event loop would freeze the whole UI -- no redraw, no Esc, no
/// way to tell a slow build from a hang. Results come back on the same
/// channel as tokens, so the stale-request-id guard in `handle_event`
/// covers them too.
pub fn execute_approved(
    app: &mut App,
    workspace: Option<&Workspace>,
    tx: &mpsc::Sender<(u64, StreamEvent)>,
) {
    if app.state != AppState::ExecutingTools || app.approved_tools.is_empty() {
        return;
    }
    let calls = std::mem::take(&mut app.approved_tools);
    // Whether the response these calls came from was cut off by the token
    // budget (`finish_reason == "length"`) rather than finishing on its own --
    // set by `request_tools_truncated` when the calls first arrived, and read
    // here rather than threaded through `approved_tools` because every call in
    // one batch shares one response and so shares one answer to this question.
    let truncated = app.truncated_response;
    // The whole config, not just `[tools]`: an `agent` call spawns a child
    // loop, and a loop needs the endpoint. Everything else still sees only
    // the `tools` half it always saw.
    let config = app.config.clone();
    match workspace {
        Some(ws) => {
            let ws = ws.clone();
            let id = app.request_id;
            let tx = tx.clone();
            let handle = tokio::spawn(async move {
                let outcomes = run_calls(&calls, &ws, &config, truncated, Some((id, &tx))).await;
                let _ = tx.send((id, StreamEvent::ToolsFinished(outcomes))).await;
            });
            app.abort = Some(handle.abort_handle());
        }
        // Only reachable if a model invents tool calls for a schema it
        // was never sent. Answer them anyway, or the history is left
        // invalid and the next prompt fails instead of this one.
        None => app.fail_stream(
            "The model asked to run a command, but the command tool is not enabled.".to_string(),
        ),
    }
}

/// How many pure-read calls run concurrently within one fan-out run. A turn
/// can ask for dozens of reads at once (a model fanning `grep_search` out
/// over a big tree); without a cap that opens dozens of file handles or
/// spawns dozens of `run_command` processes in one instant. Same cap
/// `artifacts.rs` uses for its own bounded upload fan-out.
const MAX_CONCURRENT_READS: usize = 16;

/// The immediate tool result for a background `agent` call: the child is
/// already running as a teammate, so this reports its job id instead of a
/// report the parent would have to wait for.
fn start_teammate(call: &ToolCall, ws: &Workspace, config: &Config) -> ToolOutcome {
    match crate::teammates::spawn(call.clone(), ws.clone(), config.clone()) {
        Ok(started) => ToolOutcome {
            call_id: call.id.clone(),
            display: format!("agent — started {}", started.id),
            content: format!(
                "Started background teammate '{}' ({}). Continue with other work; collect \
                 its report with teammates {{ \"action\": \"report\", \"id\": \"{}\" }}.",
                started.name, started.id, started.id
            ),
            diff: None,
            rollback: None,
        },
        Err(e) => ToolOutcome {
            call_id: call.id.clone(),
            display: "agent — not started".to_string(),
            content: format!("Error: {e}"),
            diff: None,
            rollback: None,
        },
    }
}

/// Run one turn's approved calls and hand back their outcomes in the same
/// order `calls` came in, regardless of which read finished first.
///
/// The batch is walked once, splitting it into maximal runs: a contiguous
/// stretch of [`is_pure_read_call`] calls is dispatched concurrently (capped
/// at [`MAX_CONCURRENT_READS`] in flight), a contiguous stretch of `agent`
/// calls is dispatched concurrently up to
/// `config.tools.subagent_concurrency`, and a contiguous stretch of
/// writes/edits to *distinct* files runs concurrently (capped at
/// [`tools::MAX_CONCURRENT_WRITES`]). A repeat of an already-targeted file,
/// and anything else -- shell commands, deploy, MCP -- fences the run and
/// still runs one at a time. The runs are stitched back together in the order
/// they occurred, so a `read, read, write, read` turn becomes "two reads
/// concurrently, then the write, then the last read" rather than reordering
/// anything the model or the transcript would notice.
///
/// Order within a concurrent run is preserved by `join_all`: each chunk is
/// polled together but its results are yielded in submission order, i.e. the
/// order the model asked for them -- so no explicit re-sorting is needed.
async fn run_calls(
    calls: &[ToolCall],
    ws: &Workspace,
    config: &Config,
    // Whether the response these calls came from was cut off by the token
    // budget (`finish_reason == "length"`) -- see `App::truncated_response`.
    // Threaded through to every dispatch below (not just the sequential
    // branch) so a `write_file`/`edit_file` call is refused the same way
    // regardless of whether it happened to land in a concurrent read run or
    // not; only those two tools actually look at the flag, so it is a no-op
    // for everything else.
    truncated: bool,
    progress: Option<(u64, &mpsc::Sender<(u64, StreamEvent)>)>,
) -> Vec<ToolOutcome> {
    let mut outcomes = Vec::with_capacity(calls.len());
    let mut i = 0;
    while i < calls.len() {
        if is_pure_read_call(&calls[i]) {
            let start = i;
            while i < calls.len() && is_pure_read_call(&calls[i]) {
                i += 1;
            }
            let run = &calls[start..i];
            // Chunked rather than fanned out as one `join_all` over the whole
            // run: a chunk's futures are polled together, so this is the cap
            // on how many reads (file handles, `run_command` child processes)
            // are ever in flight at once. `join_all` -- not
            // `stream::buffered` -- because each future here owns its
            // arguments (cloned per call); a version borrowing `ws`/`config`
            // through the closure ran into rustc's known HRTB false-positive
            // ("implementation of `FnOnce` is not general enough") when a
            // `Stream::map` closure's return type is itself an `async fn`
            // call rather than a fully owned `async move` block.
            for chunk in run.chunks(MAX_CONCURRENT_READS) {
                let futures =
                    chunk.iter().map(|call| {
                        let call = call.clone();
                        let ws = ws.clone();
                        let tools_config = config.tools.clone();
                        async move {
                            tools::execute_truncated(&call, &ws, &tools_config, truncated).await
                        }
                    });
                outcomes.extend(futures::future::join_all(futures).await);
            }
        } else if tools::write_target(ws, &calls[i]).is_some() {
            // Independent file writes: different files share no state, so a
            // contiguous run of them can run together. The run ends at a
            // non-write call or at a repeat of a file already in it, which is
            // what keeps two writes to the same path ordered.
            let range = tools::parallel_write_run(ws, calls, i);
            let run = &calls[range.clone()];
            i = range.end;
            for chunk in run.chunks(tools::MAX_CONCURRENT_WRITES) {
                let futures = chunk.iter().map(|call| {
                    let call = call.clone();
                    let ws = ws.clone();
                    let tools_config = config.tools.clone();
                    async move { tools::execute_truncated(&call, &ws, &tools_config, truncated).await }
                });
                outcomes.extend(futures::future::join_all(futures).await);
            }
        } else if calls[i].function.name == tools::AGENT {
            // Independent research tasks: run a contiguous run of `agent`
            // calls concurrently instead of paying their latencies one after
            // another. They are read-only and isolated from each other, so
            // the only coupling is the shared token bill -- which is exactly
            // what `subagent_concurrency` bounds.
            let start = i;
            while i < calls.len() && calls[i].function.name == tools::AGENT {
                i += 1;
            }
            let run = &calls[start..i];
            let cap = if config.tools.subagent_concurrency == 0 {
                run.len()
            } else {
                config.tools.subagent_concurrency
            };
            // Background calls start a teammate and finish immediately; the
            // results are placed back by index so a mixed run keeps the
            // transcript's order.
            let mut results: Vec<Option<ToolOutcome>> = (0..run.len()).map(|_| None).collect();
            let mut foreground: Vec<(usize, ToolCall)> = Vec::new();
            for (index, call) in run.iter().enumerate() {
                if matches!(
                    tools::describe_action(call),
                    Some(tools::Action::Agent {
                        background: true,
                        ..
                    })
                ) {
                    results[index] = Some(start_teammate(call, ws, config));
                } else {
                    foreground.push((index, call.clone()));
                }
            }
            // Chunked exactly like the read fan-out above: a chunk's futures
            // are polled together, so this is the hard cap on how many
            // children are ever in flight. `join_all` still yields results in
            // submission order, so the transcript order is unchanged.
            for chunk in foreground.chunks(cap.max(1)) {
                let futures = chunk.iter().map(|(_, call)| {
                    let call = call.clone();
                    let ws = ws.clone();
                    let config = config.clone();
                    async move { run_subagent(&call, &ws, &config, progress).await }
                });
                let finished = futures::future::join_all(futures).await;
                for ((index, _), outcome) in chunk.iter().zip(finished) {
                    results[*index] = Some(outcome);
                }
            }
            outcomes.extend(
                results
                    .into_iter()
                    .map(|outcome| outcome.expect("every agent call produced an outcome")),
            );
        } else {
            let call = &calls[i];
            outcomes.push(tools::execute_truncated(call, ws, &config.tools, truncated).await);
            i += 1;
        }
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn agent_call(args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call_agent".to_string(),
            kind: "function".to_string(),
            function: llm::FunctionCall {
                name: tools::AGENT.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    fn workspace_with_hello() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("hello.txt"), "one\ntwo\nthree\n").unwrap();
        let ws = Workspace::new(dir.path()).expect("workspace");
        (dir, ws)
    }

    fn sse(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{body}"
        )
    }

    /// One SSE round that asks for a single tool call.
    fn tool_call_round(name: &str, args: &str) -> String {
        let escaped = args.replace('\\', "\\\\").replace('"', "\\\"");
        sse(&format!(
            "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"c1\",\
             \"type\":\"function\",\"function\":{{\"name\":\"{name}\",\"arguments\":\
             \"{escaped}\"}}}}]}}}}]}}\n\ndata: [DONE]\n\n"
        ))
    }

    /// One SSE round that answers in plain text.
    fn text_round(text: &str) -> String {
        sse(&format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\ndata: [DONE]\n\n"
        ))
    }

    /// Read one full HTTP request (headers + Content-Length body) off a socket.
    async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
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
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                if buf.len() >= header_end + 4 + length {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    /// Serve the given responses to successive connections and hand back the
    /// requests that were made, once all responses are spent.
    async fn serve_rounds(
        responses: Vec<String>,
    ) -> (String, tokio::sync::oneshot::Receiver<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut socket, _) = listener.accept().await.expect("accept");
                requests.push(read_request(&mut socket).await);
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
            let _ = done_tx.send(requests);
        });
        (format!("http://{addr}"), done_rx)
    }

    fn config_for(endpoint: &str) -> Config {
        let mut config = Config::default();
        config.llm.endpoint = endpoint.to_string();
        config.llm.model = "test-model".to_string();
        config.llm.api_key = "sk-test".to_string();
        config
    }

    /// The whole Phase 4.1 exit criterion in one test: the child reads a file
    /// and only its final report reaches the parent -- as the outcome of one
    /// tool call, with the file's contents nowhere in it.
    #[tokio::test]
    async fn a_subagent_researches_then_reports_only_its_final_message() {
        // A turn records usage, so this must not run against the real config
        // directory -- see `IsolatedHome`.
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace_with_hello();
        let (endpoint, requests) = serve_rounds(vec![
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            text_round("hello.txt has three lines."),
        ])
        .await;
        let config = config_for(&endpoint);

        let outcome = run_subagent(
            &agent_call(json!({ "task": "count lines in hello.txt" })),
            &ws,
            &config,
            None,
        )
        .await;

        assert_eq!(outcome.content, "hello.txt has three lines.");
        assert_eq!(outcome.call_id, "call_agent");
        assert!(
            outcome.display.contains("1 tool round"),
            "{}",
            outcome.display
        );
        let requests = requests.await.expect("both rounds served");
        assert!(
            requests[1].contains("three"),
            "the file's contents went back to the child, not the parent"
        );
        assert!(
            requests[0].contains("read-only research subagent"),
            "the child gets the child's prompt, not the parent's"
        );
    }

    /// Research can be pointed at a faster model without changing the
    /// parent's. The request carries the child model, and the usage ledger
    /// attributes the spend to it.
    #[tokio::test]
    async fn a_subagent_can_use_its_own_model() {
        let (_dir, ws) = workspace_with_hello();
        let (endpoint, requests) = serve_rounds(vec![text_round("done")]).await;
        let mut config = config_for(&endpoint);
        config.tools.subagent_model = Some("fast-model".to_string());

        let outcome = run_subagent(
            &agent_call(json!({ "task": "look around" })),
            &ws,
            &config,
            None,
        )
        .await;

        assert_eq!(outcome.content, "done");
        let requests = requests.await.expect("served");
        assert!(
            requests[0].contains("fast-model"),
            "the child request must name the routed model: {}",
            requests[0]
        );
        assert!(
            !requests[0].contains("test-model"),
            "the parent model must not leak into the child request"
        );
    }

    /// A project-defined agent type is accepted, and its prompt is appended
    /// to the built-in child prompt rather than replacing it.
    #[tokio::test]
    async fn a_custom_agent_type_gets_its_own_prompt() {
        let (dir, ws) = workspace_with_hello();
        let agents = dir.path().join(".covexcode").join("agents");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(
            agents.join("reviewer.toml"),
            "name = \"reviewer\"\ndescription = \"reviews code\"\nprompt = \"You are a reviewer.\"\n",
        )
        .unwrap();
        let (endpoint, requests) = serve_rounds(vec![text_round("reviewed")]).await;
        let config = config_for(&endpoint);

        let outcome = run_subagent(
            &agent_call(json!({ "task": "review hello.txt", "agent_type": "reviewer" })),
            &ws,
            &config,
            None,
        )
        .await;

        assert_eq!(outcome.content, "reviewed");
        let requests = requests.await.expect("served");
        assert!(
            requests[0].contains("You are a reviewer."),
            "the definition's prompt must reach the child: {}",
            requests[0]
        );
        assert!(
            requests[0].contains("read-only"),
            "the built-in read-only rules must stay in force"
        );
    }

    /// A child that asks to write gets the refusal as that call's result and
    /// can still finish -- the read-only guarantee holds without anything
    /// reaching disk or a prompt.
    #[tokio::test]
    async fn a_subagent_write_is_refused_and_the_child_carries_on() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (dir, ws) = workspace_with_hello();
        let (endpoint, requests) = serve_rounds(vec![
            tool_call_round(tools::WRITE_FILE, r#"{"path":"notes.md","content":"hi"}"#),
            text_round("Report without writing."),
        ])
        .await;
        let config = config_for(&endpoint);

        let outcome = run_subagent(
            &agent_call(json!({ "task": "write up findings" })),
            &ws,
            &config,
            None,
        )
        .await;

        assert_eq!(outcome.content, "Report without writing.");
        assert!(
            !dir.path().join("notes.md").exists(),
            "nothing may reach disk"
        );
        let requests = requests.await.expect("both rounds served");
        assert!(
            requests[1].contains("read-only research subagent"),
            "the refusal is answered to the model so the transcript stays valid"
        );
    }

    /// Unknown types are refused before any request is made -- with the fix
    /// spelled out, because a bare "no" gets retried.
    #[tokio::test]
    async fn an_unknown_agent_type_is_refused_with_directions() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace_with_hello();
        let config = config_for("http://127.0.0.1:9"); // never contacted
        let outcome = run_subagent(
            &agent_call(json!({ "task": "fix the bug", "agent_type": "nonsense" })),
            &ws,
            &config,
            None,
        )
        .await;
        assert!(outcome.content.contains("'explore'"), "{}", outcome.content);
        assert!(outcome.content.contains("worker"), "{}", outcome.content);
        assert!(outcome.content.contains("Error"), "{}", outcome.content);
    }

    #[tokio::test]
    async fn unparseable_arguments_are_reported_not_run() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace_with_hello();
        let config = config_for("http://127.0.0.1:9"); // never contacted
        let mut call = agent_call(json!({}));
        call.function.arguments = "not json".to_string();
        let outcome = run_subagent(&call, &ws, &config, None).await;
        assert!(outcome.content.contains("task"), "{}", outcome.content);
    }

    /// While it works, the child announces each tool call on the parent's
    /// event channel -- executed ones plainly, refused ones saying so -- and
    /// every event carries the parent call's id and request id, so the
    /// stale-id guard and the trail bookkeeping both work unchanged.
    #[tokio::test]
    async fn a_subagent_reports_each_step_while_it_works() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace_with_hello();
        let (endpoint, _requests) = serve_rounds(vec![
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            tool_call_round(tools::WRITE_FILE, r#"{"path":"n.md","content":"x"}"#),
            text_round("Report."),
        ])
        .await;
        let config = config_for(&endpoint);
        let (tx, mut rx) = mpsc::channel(64);

        let outcome = run_subagent(
            &agent_call(json!({ "task": "look around" })),
            &ws,
            &config,
            Some((7, &tx)),
        )
        .await;
        drop(tx); // so the drain below ends instead of waiting forever

        assert_eq!(outcome.content, "Report.");
        let mut activities = Vec::new();
        while let Some((id, event)) = rx.recv().await {
            let StreamEvent::AgentActivity {
                call_id,
                label,
                rounds,
            } = event
            else {
                panic!("the child must only send activity, got {event:?}");
            };
            assert_eq!(id, 7, "tagged with the parent's request id");
            assert_eq!(call_id, "call_agent", "tagged with the parent call");
            activities.push((label, rounds));
        }
        assert_eq!(
            activities.len(),
            2,
            "one event per tool call the child made"
        );
        assert!(activities[0].0.contains("read"), "{}", activities[0].0);
        assert_eq!(activities[0].1, 1);
        assert!(
            activities[1].0.ends_with("— refused"),
            "{}",
            activities[1].0
        );
        assert_eq!(activities[1].1, 2);
    }

    /// With the step budget already spent, the request that follows carries
    /// the answer-now prompt and no schemas: the report is forced, not asked
    /// for.
    #[tokio::test]
    async fn a_spent_budget_forces_the_report() {
        let _isolated = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace_with_hello();
        let (endpoint, requests) = serve_rounds(vec![
            tool_call_round(tools::READ_FILE, r#"{"path":"hello.txt"}"#),
            text_round("Forced report."),
        ])
        .await;
        let mut config = config_for(&endpoint);
        // One allowed round, then the schemas are withheld and the child is
        // made to answer. (`0` now means "no limit", so a spent budget has to
        // be a positive one.)
        config.tools.subagent_max_steps = 1;

        let outcome = run_subagent(
            &agent_call(json!({ "task": "anything" })),
            &ws,
            &config,
            None,
        )
        .await;

        assert_eq!(outcome.content, "Forced report.");
        let requests = requests.await.expect("both rounds served");
        assert!(
            requests[1].contains("Write your report now"),
            "the prompt demands an answer"
        );
        assert!(
            !requests[1].contains("\"tools\""),
            "no schemas: stopping is enforced, not requested"
        );
    }

    // -- Phase 1 parallel read-only fan-out (`run_calls`) --------------------

    fn call_with(id: &str, name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            kind: "function".to_string(),
            function: llm::FunctionCall {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    fn read_call(id: &str, path: &str) -> ToolCall {
        call_with(id, tools::READ_FILE, json!({ "path": path }))
    }

    fn write_call(id: &str, path: &str, content: &str) -> ToolCall {
        call_with(
            id,
            tools::WRITE_FILE,
            json!({ "path": path, "content": content }),
        )
    }

    /// Not on `is_read_only`'s allowlist even as a literal prefix (`rm` is
    /// nowhere near it), so this is deliberately never classified as a pure
    /// read -- used to prove a non-read `run_command` still runs like before.
    fn run_command_call(id: &str, command: &str) -> ToolCall {
        call_with(id, tools::RUN_COMMAND, json!({ "command": command }))
    }

    fn workspace_with_files(files: &[(&str, &str)]) -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().expect("temp dir");
        for (name, content) in files {
            std::fs::write(dir.path().join(name), content).unwrap();
        }
        let ws = Workspace::new(dir.path()).expect("workspace");
        (dir, ws)
    }

    /// `is_pure_read_call` is the fence the whole fan-out depends on: it has
    /// to agree with `tools::is_read_only` for commands, admit the four
    /// dedicated read tools, and refuse everything that writes, edits, or
    /// runs a subagent -- including a command `is_read_only` would not vouch
    /// for.
    #[test]
    fn is_pure_read_call_matches_the_documented_allowlist() {
        assert!(is_pure_read_call(&read_call("1", "a.txt")));
        assert!(is_pure_read_call(&call_with(
            "2",
            tools::LIST_DIR,
            json!({ "path": "." })
        )));
        assert!(is_pure_read_call(&call_with(
            "3",
            tools::GLOB,
            json!({ "pattern": "**/*.rs" })
        )));
        assert!(is_pure_read_call(&call_with(
            "4",
            tools::GREP_SEARCH,
            json!({ "pattern": "fn main" })
        )));
        assert!(is_pure_read_call(&run_command_call("5", "ls -la")));

        assert!(!is_pure_read_call(&write_call("6", "a.txt", "x")));
        assert!(!is_pure_read_call(&call_with(
            "7",
            tools::EDIT_FILE,
            json!({ "path": "a.txt", "old_string": "x", "new_string": "y" })
        )));
        assert!(!is_pure_read_call(&run_command_call("8", "rm -rf a.txt")));
        assert!(!is_pure_read_call(&agent_call(
            json!({ "task": "look around" })
        )));
    }

    /// A whole batch of reads -- the four dedicated tools plus a read-only
    /// `run_command` -- runs through the concurrent fan-out, but the outcomes
    /// still come back call-for-call in the order the model asked for them.
    /// Correctness of the ordering, not the timing of the concurrency, is
    /// what `ToolsFinished` consumers actually depend on.
    #[tokio::test]
    async fn a_batch_of_reads_comes_back_in_original_order() {
        let (_dir, ws) =
            workspace_with_files(&[("a.txt", "aaa"), ("b.txt", "bbb"), ("c.txt", "ccc")]);
        let config = Config::default();
        let calls = vec![
            read_call("1", "a.txt"),
            call_with("2", tools::LIST_DIR, json!({ "path": "." })),
            read_call("3", "b.txt"),
            call_with("4", tools::GLOB, json!({ "pattern": "*.txt" })),
            read_call("5", "c.txt"),
            run_command_call("6", list_command()),
        ];

        let outcomes = run_calls(&calls, &ws, &config, false, None).await;

        assert_eq!(outcomes.len(), calls.len());
        let ids: Vec<&str> = outcomes.iter().map(|o| o.call_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["1", "2", "3", "4", "5", "6"],
            "results must match call order"
        );
        assert!(outcomes[0].content.contains("aaa"));
        assert!(outcomes[2].content.contains("bbb"));
        assert!(outcomes[4].content.contains("ccc"));
    }

    /// `dir` on Windows, `ls` elsewhere -- matches `tools.rs`'s own helper of
    /// the same name, needed here because the read-only `run_command` in the
    /// tests above has to be a real command on whatever platform runs them.
    fn list_command() -> &'static str {
        if cfg!(windows) {
            "dir"
        } else {
            "ls"
        }
    }

    /// A write sitting between two reads still lands in the outcome vector at
    /// its original position, and the file it wrote is really there --
    /// proving the read-only runs on either side of it did not smear it out
    /// of order or skip it.
    #[tokio::test]
    async fn a_write_between_reads_keeps_its_place_in_the_order() {
        let (dir, ws) = workspace_with_files(&[("a.txt", "aaa"), ("b.txt", "bbb")]);
        let config = Config::default();
        let calls = vec![
            read_call("1", "a.txt"),
            write_call("2", "out.txt", "written"),
            read_call("3", "b.txt"),
        ];

        let outcomes = run_calls(&calls, &ws, &config, false, None).await;

        let ids: Vec<&str> = outcomes.iter().map(|o| o.call_id.as_str()).collect();
        assert_eq!(ids, vec!["1", "2", "3"]);
        assert!(outcomes[0].content.contains("aaa"));
        assert!(outcomes[2].content.contains("bbb"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("out.txt")).unwrap(),
            "written",
            "the write in the middle of the batch still ran"
        );
    }

    /// A batch with nothing read-only in it -- two writes to the same path,
    /// which only stay correct if they still run strictly one after another
    /// -- behaves exactly like the pre-Phase-1 sequential loop: same order,
    /// no interleaving, last write wins deterministically.
    #[tokio::test]
    async fn an_all_write_batch_still_runs_sequentially() {
        let (dir, ws) = workspace_with_files(&[("shared.txt", "start")]);
        let config = Config::default();
        let calls = vec![
            write_call("1", "shared.txt", "first"),
            write_call("2", "shared.txt", "second"),
        ];

        let outcomes = run_calls(&calls, &ws, &config, false, None).await;

        let ids: Vec<&str> = outcomes.iter().map(|o| o.call_id.as_str()).collect();
        assert_eq!(ids, vec!["1", "2"]);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("shared.txt")).unwrap(),
            "second",
            "sequential writes still apply in call order, exactly as before Phase 1"
        );
    }

    /// Writes to different files form one concurrent run: each outcome keeps
    /// its call's place, and every file is written.
    #[tokio::test]
    async fn writes_to_distinct_files_run_as_one_batch() {
        let (dir, ws) = workspace_with_files(&[]);
        let config = Config::default();
        let calls = vec![
            write_call("1", "a.txt", "one"),
            write_call("2", "b.txt", "two"),
            write_call("3", "c.txt", "three"),
        ];

        let outcomes = run_calls(&calls, &ws, &config, false, None).await;

        let ids: Vec<&str> = outcomes.iter().map(|o| o.call_id.as_str()).collect();
        assert_eq!(ids, vec!["1", "2", "3"]);
        for (name, content) in [("a.txt", "one"), ("b.txt", "two"), ("c.txt", "three")] {
            assert_eq!(
                std::fs::read_to_string(dir.path().join(name)).unwrap(),
                content,
                "{name} was written"
            );
        }
    }

    /// Two independent `agent` calls in one response must overlap, not add
    /// their latencies. The mock endpoint holds each child request until the
    /// other has arrived, so a sequential implementation deadlocks here and
    /// trips the timeout.
    #[tokio::test]
    async fn sibling_subagents_run_concurrently() {
        use std::sync::Arc;
        use std::time::Duration;

        let (_dir, ws) = workspace_with_hello();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        let answer = text_round("report");
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        tokio::spawn(async move {
            let mut handlers = Vec::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let barrier = barrier.clone();
                let answer = answer.clone();
                handlers.push(tokio::spawn(async move {
                    let _ = read_request(&mut socket).await;
                    // Both children must be in flight for either to finish.
                    barrier.wait().await;
                    let _ = socket.write_all(answer.as_bytes()).await;
                    let _ = socket.shutdown().await;
                }));
            }
            for handler in handlers {
                let _ = handler.await;
            }
        });

        let config = config_for(&format!("http://{addr}"));
        let calls = vec![
            call_with("a1", tools::AGENT, json!({ "task": "research one" })),
            call_with("a2", tools::AGENT, json!({ "task": "research two" })),
        ];

        let outcomes = tokio::time::timeout(
            Duration::from_secs(5),
            run_calls(&calls, &ws, &config, false, None),
        )
        .await
        .expect("the two subagents must be in flight at the same time");

        let ids: Vec<&str> = outcomes.iter().map(|o| o.call_id.as_str()).collect();
        assert_eq!(ids, vec!["a1", "a2"], "call order is preserved");
        assert_eq!(outcomes[0].content, "report");
        assert_eq!(outcomes[1].content, "report");
    }

    /// The built-in types resolve without any `.covexcode/agents` file: the
    /// reviewer is read-only and reports like explore.
    #[tokio::test]
    async fn builtin_agent_types_resolve_without_project_files() {
        let _home = crate::config::test_support::IsolatedHome::new();
        let (_dir, ws) = workspace_with_hello();
        let (endpoint, _bodies) = serve_rounds(vec![text_round("Looked fine.")]).await;
        let config = config_for(&endpoint);
        let calls = vec![agent_call(
            json!({ "task": "review hello.txt", "agent_type": "reviewer" }),
        )];

        let outcomes = run_calls(&calls, &ws, &config, false, None).await;

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].content, "Looked fine.");
    }

    /// A background agent call returns immediately; its report is collected
    /// from the teammate registry instead of the tool result.
    #[tokio::test]
    async fn a_background_agent_call_returns_a_job_and_its_report_is_collectable() {
        let (_dir, ws) = workspace_with_hello();
        let (endpoint, _bodies) = serve_rounds(vec![text_round("Background report.")]).await;
        let config = config_for(&endpoint);
        let call = agent_call(json!({
            "task": "look around in the background",
            "background": true
        }));

        let outcomes = run_calls(&[call], &ws, &config, false, None).await;
        assert_eq!(outcomes.len(), 1);
        assert!(
            outcomes[0].content.contains("Started background teammate"),
            "{}",
            outcomes[0].content
        );

        let id = crate::teammates::list(ws.root())
            .into_iter()
            .find(|job| job.task == "look around in the background")
            .map(|job| job.id)
            .expect("the job is registered");
        let mut collected = None;
        for _ in 0..200 {
            match crate::teammates::report(ws.root(), &id) {
                Ok(text) => {
                    collected = Some(text);
                    break;
                }
                Err(e) if e.contains("still running") => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(e) => panic!("unexpected report error: {e}"),
            }
        }
        assert_eq!(collected.expect("a report"), "Background report.");
    }

    fn committed_workspace() -> (tempfile::TempDir, Workspace) {
        use std::process::Command;

        let (dir, ws) = workspace_with_hello();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .output()
                .expect("git runs");
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init", "-q"]);
        git(&["config", "user.email", "test@example.com"]);
        git(&["config", "user.name", "Test"]);
        git(&["add", "hello.txt"]);
        git(&["commit", "-qm", "initial"]);
        (dir, ws)
    }

    fn worker_definition(dir: &std::path::Path, verify: Option<&str>) {
        let agents = dir.join(".covexcode").join("agents");
        std::fs::create_dir_all(&agents).unwrap();
        let verify = verify
            .map(|command| format!("\nverify = \"{command}\""))
            .unwrap_or_default();
        std::fs::write(
            agents.join("worker.toml"),
            format!(
                "name = \"worker\"\nwrites = true\nprompt = \"Create the file.\"{verify}\n"
            ),
        )
        .unwrap();
    }

    async fn run_worker(ws: &Workspace, config: &Config) -> ToolOutcome {
        let calls = vec![agent_call(
            json!({ "task": "create created.txt", "agent_type": "worker" }),
        )];
        let outcomes = run_calls(&calls, ws, config, false, None).await;
        assert_eq!(outcomes.len(), 1);
        outcomes.into_iter().next().expect("one outcome")
    }

    /// A definition with `writes = true` runs in its own git worktree, its
    /// verify command passes, and the clean patch is applied to the
    /// requester's checkout with the diff in the outcome.
    #[tokio::test]
    async fn a_writing_subagent_applies_a_clean_patch_to_the_workspace() {
        // Worktrees live under the config dir; isolate HOME like every other
        // test that reaches it.
        let _home = crate::config::test_support::IsolatedHome::new();
        let (dir, ws) = committed_workspace();
        worker_definition(dir.path(), Some("test -f created.txt"));

        let (endpoint, _bodies) = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"created.txt","content":"hello\n"}"#,
            ),
            text_round("Created created.txt."),
        ])
        .await;
        let config = config_for(&endpoint);

        let outcome = run_worker(&ws, &config).await;

        let content = &outcome.content;
        assert!(content.contains("--- patch"), "{content}");
        assert!(content.contains("verify passed"), "{content}");
        assert!(content.contains("applied cleanly"), "{content}");
        assert!(content.contains("created.txt"), "{content}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("created.txt")).unwrap(),
            "hello\n",
            "the clean patch reached the requester's checkout"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("hello.txt")).unwrap(),
            "one\ntwo\nthree\n"
        );
    }

    /// The child may run its own verify command while it works -- the one
    /// command beyond the read-only list -- so a failure can be fixed in the
    /// same turn instead of being discovered only after the report.
    #[tokio::test]
    async fn a_writing_subagent_can_run_its_verify_command_while_working() {
        let _home = crate::config::test_support::IsolatedHome::new();
        let (dir, ws) = committed_workspace();
        worker_definition(dir.path(), Some("test -f created.txt"));

        let (endpoint, bodies) = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"created.txt","content":"hello\n"}"#,
            ),
            tool_call_round(tools::RUN_COMMAND, r#"{"command":"test -f created.txt"}"#),
            text_round("Created and verified."),
        ])
        .await;
        let config = config_for(&endpoint);

        let outcome = run_worker(&ws, &config).await;

        assert!(
            outcome.content.contains("applied cleanly"),
            "{}",
            outcome.content
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("created.txt")).unwrap(),
            "hello\n"
        );
        let bodies = bodies.await.expect("three rounds served");
        assert!(
            bodies[0].contains("Verification") && bodies[0].contains("test -f created.txt"),
            "the child must be told which command it may run: {}",
            bodies[0]
        );
        assert!(
            bodies[2].contains("exit code: 0"),
            "the verify command must have actually run, not been refused: {}",
            bodies[2]
        );
        assert!(
            !bodies[2].contains("may only run commands"),
            "a refusal would carry the allowlist message: {}",
            bodies[2]
        );
    }

    /// A failing verify command stops the patch: the requester's checkout is
    /// untouched and the failure output is in the result.
    #[tokio::test]
    async fn a_failed_verify_keeps_the_patch_unapplied() {
        let _home = crate::config::test_support::IsolatedHome::new();
        let (dir, ws) = committed_workspace();
        worker_definition(dir.path(), Some("echo verify says no >&2; exit 1"));

        let (endpoint, _bodies) = serve_rounds(vec![
            tool_call_round(
                tools::WRITE_FILE,
                r#"{"path":"created.txt","content":"hello\n"}"#,
            ),
            text_round("Created created.txt."),
        ])
        .await;
        let config = config_for(&endpoint);

        let outcome = run_worker(&ws, &config).await;

        let content = &outcome.content;
        assert!(content.contains("verify command failed"), "{content}");
        assert!(content.contains("verify says no"), "{content}");
        assert!(
            !dir.path().join("created.txt").exists(),
            "a failed verify must not apply the patch"
        );
    }
}
