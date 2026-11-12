/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! The dedicated ACP-only runtime shipped inside Covexcode IDE.
//!
//! This binary deliberately shares the protocol, session, tool, and LLM
//! modules with the terminal client, but omits the TUI, login/logout, and
//! self-update command surface. It speaks ACP over stdio by default, or over
//! loopback TCP with `--listen` for local multi-connection clients.

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
#[cfg(feature = "cli")]
#[path = "../theme.rs"]
mod theme;
#[path = "../tools.rs"]
mod tools;
#[path = "../transport.rs"]
mod transport;
#[path = "../usage.rs"]
mod usage;
#[path = "../workspace.rs"]
mod workspace;
#[path = "../worktree.rs"]
mod worktree;

use std::error::Error;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn print_help() {
    println!("covexcode-agent {VERSION}");
    println!();
    println!("ACP (Agent Client Protocol) runtime bundled with Covexcode IDE.");
    println!("By default it reads and writes ndjson on stdio; --listen serves the");
    println!("same protocol over TCP for local clients, one session set per connection.");
    println!();
    println!("Usage: covexcode-agent [--acp] [--listen [ADDR]] [--providers-json] [--version]");
    println!();
    println!("  --listen [ADDR]  Serve ACP over TCP (default 127.0.0.1:7422, loopback only)");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut listen: Option<String> = None;
    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-V" | "--version" => {
                // Keep the same shape as the CLI: the IDE's doctor parses it.
                println!("covexcode {VERSION}");
                return Ok(());
            }
            "--providers-json" => {
                println!(
                    "{}",
                    serde_json::to_string(providers::PROVIDERS)
                        .expect("PROVIDERS must serialize")
                );
                return Ok(());
            }
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            "--acp" => {}
            "--listen" => {
                // Peek rather than consume: `--listen --providers-json` must
                // keep its second flag, not swallow it as an address.
                let addr = match args.peek() {
                    Some(next) if !next.starts_with('-') => args.next().unwrap(),
                    _ => "127.0.0.1:7422".to_string(),
                };
                listen = Some(addr);
            }
            other => {
                eprintln!("Unknown argument: {other}\n");
                print_help();
                std::process::exit(2);
            }
        }
    }

    // ACP is the default mode. `--acp` is accepted so the IDE can keep
    // launching both this binary and the terminal CLI with the same argument
    // list. `--listen` swaps the stdio transport for a loopback TCP server.
    let config = config::Config::load()?;
    match listen {
        Some(addr) => transport::serve(config, &addr).await,
        None => transport::run(config).await,
    }
}
