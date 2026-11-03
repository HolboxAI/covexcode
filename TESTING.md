# Testing Guide

## What Gets Tested

### 1. Unit Tests (`cargo test`)

Roughly 500 tests, almost all of them living beside the code they cover in a
`#[cfg(test)] mod tests`. The broad areas:

- Config loading, TOML parsing, env-var precedence, and upgrade safety (an
  older `config.toml` missing a whole table must still load)
- SSE streaming and tool-call reassembly, against a real socket fed a few bytes
  at a time so lines and multi-byte characters split across chunks
- The `danger.rs` command classifier — blocked, destructive, and ordinary
- Tool execution: `read_file`, `write_file`, `edit_file`, `list_dir`, `glob`,
  `web_search`, including the workspace-escape refusals
- Quota arithmetic, usage history, and the readouts
- Terminal rendering, swept across sizes from 1×1 to 200×60 so no screen can
  panic on a terminal too small to hold it, plus WCAG contrast checks on every
  palette against both backgrounds
- Deployment (`src/deploy/`) — see below
- "Deny with feedback" — see below

**Run:**
```bash
cargo test --all
```

### 1a. Deny-with-feedback tests

The `/login`-style promise here is that refusing something costs nothing extra:
`n` offers a reason, and Enter-on-empty or Esc must behave exactly like the old
one-key refusal. That is a claim about keys, so it is tested at the key level in
three layers, with no network and no model.

```bash
cargo test --bin covexcode approval::     # the shared enum + input widget
cargo test --bin covexcode feedback       # wiring, at the App level
cargo test --bin covexcode ui::tests       # what is actually drawn
```

What is covered:

- `approval::tests` — the `Feedback` widget on its own: typed reason becomes
  `DenyWithFeedback` verbatim (inner punctuation, apostrophes and all); empty
  Enter and Esc are plain `Deny`; Esc after typing throws the text away rather
  than sending it; whitespace-only is not a reason; `y` and `n` are ordinary
  characters inside the prompt; Ctrl-chords are swallowed rather than typed.
- `app::tests` — `n` opens the prompt and decides nothing (the call is still
  queued); the reason reaches the tool result as `Reason: <text>`; an empty
  Enter and an Esc both produce the byte-identical refusal string that shipped
  before this feature; a reason does not leak onto the next queued call; a plan
  rejected *with* a reason goes straight back to the model while one rejected
  without stays waiting for the user; a cancelled rollback records its reason.
- `ui::tests` — the prompt is drawn at width 60 and 120, and drawn **twice into
  the same `TestBackend`**: the second frame must show one prompt, the input
  caret, and not one surviving row of the y/n choice it replaced. A reused
  buffer is what makes a stale row visible; a fresh one would hide exactly the
  bug this is guarding. A second test asserts the feedback footer occupies the
  same number of rows as the y/n choice, since the viewport is sized from that
  count — equal height is why opening the prompt is a repaint and not a resize.

What is **not** covered automatically, and needs a human:

- The real terminal. `scripts/` has no harness for it, and the row-cleanliness
  guarantee in a live terminal rests on the height-equality test above plus the
  reused-backend test. A manual pass: `approval = "always"` under `[tools]`, ask
  for any file edit, press `n`, type, Enter, and check the transcript line and
  that no half-drawn prompt is left above the composer.

### 1a. Deployment tests

The deployment flow is deliberately testable with **no CLI installed, nobody
signed in and no network**: providers describe commands rather than running
them, and `service.rs` is a pure state machine fed canned `CommandOutput`s.

```bash
cargo test --bin covexcode deploy      # everything under src/deploy/
cargo test --bin covexcode detect      # framework detection only
```

What is covered:

| Area | Where |
| --- | --- |
| The v1.0.0 state migration: nothing lost, nothing mixed | `paths::tests` |
| `COVEXCODE_*` over legacy `BOXCODE_*` / deprecated `TUISAMPLE_*` precedence | `paths::tests` |
| Project/framework detection, ordering, build+output defaults | `deploy::detect` |
| CLI detection: present, missing, present-but-broken | `deploy::cli` |
| Install command judged by the `danger` guardrails | `deploy::cli` |
| Provider selection and the whole flow end to end | `deploy::service` |
| Authentication state: signed in, out, and unrecognised | `deploy::vercel`, `deploy::netlify` |
| Deployment configuration, editing, env vars | `deploy::service` |
| Successful deployment, URL extraction, history record | `deploy::service` |
| Every failure path, retry, and cancellation | `deploy::service` |
| Command running, streaming, timeout, kill | `deploy::runner` |
| Secrets never reaching argv, the UI, or the history | `deploy` (several) |
| Every deployment screen rendering at every size | `ui::tests` |
| Deployment is not reachable as a slash command | `app::tests` |
| The panel fits a short viewport with its status line pinned | `ui::tests` |
| The `deploy_project` tool: args, risk, refusals | `tools::tests` |
| A deployment always stops for approval, even unattended | `tools::tests` |
| An approved tool call hands off to the interactive flow | `app::tests` |
| The model gets the URL, the build log, or the cancellation back | `app::tests` |
| A deployment batched with other calls is declined, not sequenced | `app::tests` |

The runner's tests spawn real processes, but only `sh`/`cmd` — never `vercel`
or `netlify`. Nothing in `cargo test` reaches the network or deploys anything.

### 2. System Requirements Tests
- ✅ Cargo/Rust must be installed
- ✅ Config directory can be created (~/.covexcode)
- ✅ Env var format validation

**These will SKIP if requirements aren't met** (not failures)

### 3. Manual Testing (Before Release)

#### Prerequisites
Before testing, you need:
1. **Rust/Cargo installed** - https://rustup.rs/
2. **An LLM endpoint** - OpenAI, Bedrock, or self-hosted
3. **Valid API credentials**

#### Test Procedure
```bash
# Build from source
cargo build --release

# Set environment variables
export COVEXCODE_ENDPOINT=https://api.openai.com
export COVEXCODE_MODEL=gpt-4
export COVEXCODE_API_KEY=sk-...

# Run the application
./target/release/covexcode
```

#### Expected Behavior
1. TUI launches (header, messages area, input box, status)
2. Can type prompt in input area
3. Press Enter to send
4. Response streams from LLM
5. Can cancel with Esc
6. Exit with Ctrl-C

#### Deny-with-feedback, by hand

Needs an approval prompt, which the shipping `destructive` posture will not
give you for a file edit — turn the question on first:

```toml
# ~/.covexcode/config.toml
[tools]
approval = "always"
```

Then, in a session, ask for any file edit and check each of these:

| Step | Expected |
| --- | --- |
| Wait for `Apply this edit?` | one y/n choice, above `↑↓ choose · enter confirm · esc skip` |
| Press `n` | the choice is replaced by `Why? (optional — enter to send, esc to skip)` and an empty `❯` field |
| Type a reason | it appears as you type; `y` and `n` are ordinary letters here |
| Press Enter | the box closes; transcript shows `✗ edit <path> — rejected: "<reason>"` |
| Ask for another edit, press `n`, press Enter | a plain refusal, and the box above it must be clean |
| Ask for another edit, press `n`, press Esc | a plain refusal, and nothing typed survives |
| Ask for another edit, press Esc | a plain refusal, no reason prompt at all |
| Ask for an edit, press `y` | it applies — approving never asks for a reason |
| Press Ctrl-C twice at any point | exits normally; the reason prompt does not swallow Ctrl-C |

The one thing to look at with your own eyes: after the prompt closes there must
be no orphaned rows — no half-drawn input box, no duplicated key hints — above
the composer.

#### Scrollback smoke test

After running any slash command (/login, /logout, /provider, /model, /init), scroll up in the terminal. You must see only the command echoed and its System: output. No popup box, no stray /, no clipped border should appear anywhere in scrollback. Test at terminal widths 60, 100 and 160 and after resizing mid-command.

### 4. Install Script Testing
```bash
# This requires Cargo to be installed
bash <(curl -fsSL https://covexcode.com/install.sh)
```

**Error: "Rust/Cargo not found"?**
→ Install from https://rustup.rs/

### 3a. Manual Deployment Testing

The one thing unit tests genuinely cannot cover is a real deployment. To
exercise it end to end:

```bash
# A throwaway project to ship
mkdir -p /tmp/deploy-demo && cd /tmp/deploy-demo
npm create vite@latest . -- --template react
npm install

# Point the app at it and launch
COVEXCODE_WORKSPACE=/tmp/deploy-demo covexcode
```

Then ask it: `deploy this to vercel`. Approve the prompt, and walk the panel:
confirm the detected Vite setup, skip the environment variables, choose Preview
(not Production, for a test), and let it run.

Worth checking by hand, since no test can:

1. **A browser login.** With no `VERCEL_TOKEN` and no prior `vercel login`,
   choosing "Log in with a browser" should tear the TUI down, hand over cleanly,
   and rebuild the screen intact afterwards.
2. **Cancellation.** Press Esc mid-build; the local CLI process should die
   (`ps aux | grep vercel` shows nothing).
3. **A failing build.** Break the build script (`"build": "exit 1"`) and confirm
   the failure screen names the cause and that Retry works after fixing it.
4. **Secrets.** Add an environment variable with a recognisable value, deploy,
   then check it appears nowhere:
   ```bash
   grep -r "your-test-value" ~/.covexcode/    # must find nothing
   ```

### 3b. Manual `/login` Testing

The browser handover needs a human, but everything around it is checkable by
hand, and three of those checks have been wrong before:

1. **The slash menu after a command.** Type `/`, press Ctrl+P, pick a command,
   Enter. The popup must disappear completely — it has no open flag of its own,
   it is derived from the input buffer, so a buffer left behind re-renders the
   menu into the scrollback every frame. Covered by
   `running_a_command_from_the_palette_clears_the_slash_menu`, but worth seeing
   once in a real terminal, since only the terminal shows scrollback.
2. **One composer, no gap.** After `/login` finishes, exactly one input box
   should sit below the transcript. The fixed 12-row viewport leaves empty rows
   above it by design; the box itself must not be duplicated or clipped.
3. **The System lines.** "Handing the terminal to the browser…" and "Signed in
   via covexcode.com" must each be on their own lines with no box characters
   anywhere in them.
4. **Widths and resize.** Repeat at 60, 100 and 160 columns. Growing the window
   mid-login is safe; *shrinking* to roughly 80×24 draws the footer over the
   composer's bottom border — that predates `/login` and reproduces with no
   login at all, so do not read it as a handover bug.
5. **Failure.** Point `COVEXCODE_AUTH_URL` at a closed port and run `/login`: a
   single `Login failed: …` line, no popup, no clipped box.

A throwaway device-flow stub makes all of this repeatable without a network:
serve `/api/auth/device/start`, `/poll` and `/heartbeat` on `127.0.0.1`, then
run with `COVEXCODE_AUTH_URL=http://127.0.0.1:<port>` and a scratch `HOME`.
Use a threaded HTTP server — a single-threaded one keeps the browser's
keep-alive connection open and every later request hangs.

### 3c. Driving the approval prompt without a model

Any OpenAI-compatible SSE endpoint can stand in for the model, which makes the
approval prompt reproducible on demand: serve one `edit_file` tool call, then
prose once a `"role":"tool"` message arrives, and run against it with a scratch
`HOME` and `COVEXCODE_ENDPOINT` pointed at `127.0.0.1`. Two settings are needed
or the prompt never appears / gets its keystrokes eaten:

```toml
[tools]
approval = "always"      # "destructive" auto-approves edits, so no prompt
[update]
check_on_start = false   # else the upgrade prompt's read_line eats the keys
```

Two caveats worth knowing before you build this:

- **`tmux` is not required, and `screen -X hardcopy` is not a substitute.** On
  macOS with `screen`, `hardcopy` has been observed to write a 0-byte file even
  for a plain `echo`, so it proves nothing. Drive a pty directly instead, and
  answer `CSI 6n` with the cursor position — an unanswered query makes the app
  fall back to a fullscreen viewport and abandon scrollback, which is not the
  mode you meant to test.
- **Clearing inherited `COVEXCODE_*` is not optional.** A harness that copies
  `os.environ` and sets only what it needs will reach whatever real endpoint and
  key the developer already has exported, and quietly make live requests from a
  test. Strip every `COVEXCODE_*` first, then set only the mock's.

## CI/CD Tests
GitHub Actions runs:
- `cargo build --release` - Compilation
- `cargo test --all` - Unit tests
- `cargo clippy` - Linter suggestions

**Status:** View at https://github.com/HolboxAI/covexcode/actions

## What's NOT Tested Automatically
- A real deployment to Vercel or Netlify (needs an account and a live network)
- The browser-login handover, which by definition needs a human at a browser
- Anything on Windows — the `cmd /C` path in `deploy::runner` and the
  PowerShell installer are exercised by CI's build matrix but not by a person
- Streaming responses against a real LLM endpoint

The TUI itself *is* tested: `ui.rs` renders every screen into a `TestBackend`
across terminal sizes from 1×1 upward.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| "Rust/Cargo not found" | Rust not installed | Install from https://rustup.rs/ |
| "Could not connect to endpoint" | LLM endpoint invalid | Check COVEXCODE_ENDPOINT env var |
| "Authentication failed" | Invalid API key | Check COVEXCODE_API_KEY env var |
| "Cannot find model" | Model name typo | Check COVEXCODE_MODEL env var |
| "No project directory" when deploying | `[tools] enabled = false` | Enable tools, or set `COVEXCODE_WORKSPACE` |
| "nothing to build or serve" | No `package.json` and no `index.html` | Launch covexcode from the project directory |
| CLI "installed but is not working" | Broken global npm install | `npm install -g vercel` again, or check `vercel --version` by hand |

## Adding New Tests
When adding features:
1. Add unit test in `tests/`
2. Run `cargo test --all` locally
3. Verify CI passes
4. Create PR with tests

**Do NOT commit without tests!**
