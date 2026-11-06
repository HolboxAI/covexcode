#!/usr/bin/env python3
"""Drive the real covexcode TUI in a pty and capture what it draws.

tmux is not installed on this machine, and `screen -X hardcopy` writes 0 bytes
here even for a trivial session -- checked against a bare `echo`, which is how
this was found -- so neither multiplexer can produce the scrollback capture the
task asks for. A pty is the thing both of them are a wrapper around, so this
uses it directly: fork the binary onto a real terminal of a known size, write
keystrokes to the master, read the bytes back, and replay them through a small
screen emulator so what is printed is the *rendered frame*, not a soup of
escape sequences.

    python3 scripts/pty-capture.py

Writes one file per step under .cov-verify/.
"""

import os
import pty
import re
import select
import shutil
import signal
import struct
import subprocess
import sys
import termios
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, ".cov-verify")
PORT = 8099
ROWS, COLS = 34, 100


class Screen:
    """Enough of a terminal to replay what ratatui draws.

    Only what this app actually emits is implemented: absolute cursor
    positioning (`CSI r;cH`), relative moves, erase-in-line/display, insert and
    delete lines, CR/LF, and scrolling when output passes the last row. Colour
    and mode sequences are recognised and ignored -- they change appearance, not
    which characters land in which cell, which is what is being checked here.
    """

    CSI = re.compile(rb"\x1b\[([0-9;?]*)([ -/]*)([@-~])")

    def __init__(self, rows=ROWS, cols=COLS):
        self.rows, self.cols = rows, cols
        self.grid = [[" "] * cols for _ in range(rows)]
        # Lines that scrolled off the top. This is the real scrollback: with the
        # inline viewport active, `insert_before` pushes finished messages up and
        # out, so what leaves the screen is exactly the history a user would
        # scroll back through.
        self.scrollback = []
        self.row = self.col = 0
        # DECSTBM scroll region, 0-based and inclusive. ratatui's viewport
        # insertion scrolls a band of the screen, so a replay that ignores the
        # region puts the scrolled rows in the wrong place entirely.
        self.region = (0, rows - 1)
        self.pending = b""

    def _put(self, ch: str) -> None:
        if self.col >= self.cols:
            self.col = 0
            self._newline()
        self.grid[self.row][self.col] = ch
        self.col += 1

    def _scroll_up(self, amount: int = 1) -> None:
        top, bottom = self.region
        for _ in range(max(1, amount)):
            gone = self.grid.pop(top)
            if top == 0:
                self.scrollback.append("".join(gone).rstrip())
            self.grid.insert(bottom, [" "] * self.cols)

    def _scroll_down(self, amount: int = 1) -> None:
        top, bottom = self.region
        for _ in range(max(1, amount)):
            self.grid.pop(bottom)
            self.grid.insert(top, [" "] * self.cols)

    def _newline(self) -> None:
        if self.row >= self.region[1]:
            self._scroll_up(1)
            self.row = self.region[1]
        else:
            self.row += 1

    def _move(self, row, col) -> None:
        self.row = max(0, min(self.rows - 1, row))
        self.col = max(0, min(self.cols - 1, col))

    def feed(self, data: bytes) -> None:
        data = self.pending + data
        self.pending = b""
        i = 0
        while i < len(data):
            byte = data[i]
            if byte == 0x1B:
                match = self.CSI.match(data, i)
                if match is None:
                    # An unterminated sequence split across reads: keep it for
                    # the next chunk rather than dropping a cursor move.
                    self.pending = data[i:]
                    return
                self._csi(match.group(1).decode("ascii", "replace"), match.group(3))
                i = match.end()
                continue
            if byte == 0x0D:
                self.col = 0
            elif byte == 0x0A:
                self._newline()
            elif byte == 0x08:
                self.col = max(0, self.col - 1)
            elif byte >= 0x20:
                # Multi-byte UTF-8 (the box-drawing characters this UI is full
                # of) has to be decoded, not fed a byte at a time.
                for length in range(1, 5):
                    chunk = data[i : i + length]
                    try:
                        text = chunk.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                    for ch in text:
                        self._put(ch)
                    i += length - 1
                    break
            i += 1

    def _csi(self, params: str, final: str) -> None:
        args = [
            int(p) if p.isdigit() else 0 for p in params.lstrip("?").split(";") if p != ""
        ]
        first = args[0] if args else 0

        if final == "H" or final == "f":
            self._move((args[0] - 1) if args else 0, (args[1] - 1) if len(args) > 1 else 0)
        elif final == "A":
            self._move(self.row - max(1, first), self.col)
        elif final == "B":
            self._move(self.row + max(1, first), self.col)
        elif final == "C":
            self._move(self.row, self.col + max(1, first))
        elif final == "D":
            self._move(self.row, self.col - max(1, first))
        elif final == "G":
            self._move(self.row, max(1, first) - 1)
        elif final == "d":
            self._move(max(1, first) - 1, self.col)
        elif final == "E":
            self._move(self.row + max(1, first), 0)
        elif final == "F":
            self._move(self.row - max(1, first), 0)
        elif final == "r":
            top = (args[0] - 1) if args else 0
            bottom = (args[1] - 1) if len(args) > 1 else self.rows - 1
            if top < bottom:
                self.region = (max(0, top), min(self.rows - 1, bottom))
                self._move(0, 0)
        elif final == "S":
            self._scroll_up(first)
        elif final == "T":
            self._scroll_down(first)
        elif final == "J":
            if first == 2:
                self.grid = [[" "] * self.cols for _ in range(self.rows)]
            elif first == 0:
                for c in range(self.col, self.cols):
                    self.grid[self.row][c] = " "
                for r in range(self.row + 1, self.rows):
                    self.grid[r] = [" "] * self.cols
            else:
                for r in range(0, self.row):
                    self.grid[r] = [" "] * self.cols
                for c in range(0, min(self.col + 1, self.cols)):
                    self.grid[self.row][c] = " "
        elif final == "K":
            if first == 0:
                for c in range(self.col, self.cols):
                    self.grid[self.row][c] = " "
            elif first == 1:
                for c in range(0, min(self.col + 1, self.cols)):
                    self.grid[self.row][c] = " "
            else:
                self.grid[self.row] = [" "] * self.cols
        elif final == "L":
            for _ in range(max(1, first)):
                self.grid.insert(self.row, [" "] * self.cols)
                _ = self.grid.pop(self.region[1])
        elif final == "M":
            for _ in range(max(1, first)):
                self.grid.pop(self.row)
                self.grid.insert(self.region[1], [" "] * self.cols)
        # Everything else (SGR, mode set/reset, cursor visibility) does not
        # change which character sits in which cell.

    def text(self) -> str:
        lines = ["".join(row).rstrip() for row in self.grid]
        while lines and not lines[-1].strip():
            lines.pop()
        return "\n".join(lines)

    def full(self) -> str:
        """Everything ever printed, scrolled-off history included."""
        history = [line for line in self.scrollback if line.strip()]
        live = self.text()
        return "\n".join(history + ([live] if live.strip() else []))


class Session:
    def __init__(self, argv, env):
        self.master, slave = pty.openpty()
        import fcntl

        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))
        self.proc = subprocess.Popen(
            argv, stdin=slave, stdout=slave, stderr=slave, env=env, cwd=ROOT,
            preexec_fn=os.setsid, close_fds=True,
        )
        os.close(slave)
        self.screen = Screen()

    def drain(self, seconds: float) -> None:
        end = time.time() + seconds
        while time.time() < end:
            ready, _, _ = select.select([self.master], [], [], 0.1)
            if not ready:
                continue
            try:
                chunk = os.read(self.master, 65536)
            except OSError:
                break
            if not chunk:
                break
            # A real terminal answers `CSI 6n` with where its cursor is. Not
            # answering it makes the app fall back to a fullscreen viewport and
            # give up on scrollback, which is not the mode being checked.
            for _ in range(chunk.count(b"\x1b[6n")):
                self.screen.feed(chunk.split(b"\x1b[6n")[0])
                chunk = chunk.split(b"\x1b[6n", 1)[1]
                self.reply_cursor()
            self.screen.feed(chunk)

    def reply_cursor(self) -> None:
        os.write(
            self.master,
            f"\x1b[{self.screen.row + 1};{self.screen.col + 1}R".encode(),
        )

    def send(self, text: str) -> None:
        os.write(self.master, text.encode())

    def close(self):
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(os.getpgid(self.proc.pid), sig)
            except (ProcessLookupError, PermissionError):
                return
            try:
                self.proc.wait(timeout=3)
                return
            except subprocess.TimeoutExpired:
                continue


def capture(name: str, screen: Screen, show_full: bool = False) -> tuple[str, str]:
    """Write the live frame and the accumulated scrollback; return both."""
    live = screen.text()
    full = screen.full()
    with open(os.path.join(OUT, name), "w") as handle:
        handle.write(live + "\n")
    with open(os.path.join(OUT, name.replace(".txt", ".full.txt")), "w") as handle:
        handle.write(full + "\n")
    print(f"\n================ {name} (live frame) ================")
    print(live)
    if show_full:
        print(f"\n================ {name} (scrollback) ================")
        print(full)
    return live, full


def main() -> int:
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(os.path.join(OUT, "home"))
    work = os.path.join(OUT, "workspace", "src")
    os.makedirs(work)
    with open(os.path.join(work, "main.rs"), "w") as handle:
        handle.write('fn main() {\n    println!("hi");\n}\n')

    # Two settings are load-bearing, both written rather than assumed:
    #   * `approval = "always"` -- the shipping default is `destructive`, under
    #     which an `edit_file` is auto-approved and no prompt ever appears. The
    #     point here is the prompt, so the stricter posture is selected.
    #   * `check_on_start = false` -- otherwise a newer release is offered at
    #     startup and its `read_line` swallows the keystrokes meant for the
    #     approval prompt.
    config_dir = os.path.join(OUT, "home", ".covexcode")
    os.makedirs(config_dir, exist_ok=True)
    with open(os.path.join(config_dir, "config.toml"), "w") as handle:
        # `[llm]` has no serde default, so it must be present or the whole file
        # is rejected.
        handle.write(
            "[llm]\n"
            f'endpoint = "http://127.0.0.1:{PORT}/v1"\n'
            'model = "mock-model"\n'
            'api_key = "test-key"\n'
            "\n[tools]\napproval = \"always\"\n"
            "\n[update]\ncheck_on_start = false\n"
        )

    mock = subprocess.Popen(
        [sys.executable, "scripts/mock-approval-server.py", str(PORT)],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    time.sleep(1)

    env = dict(os.environ)
    # Every COVEXCODE_* var is cleared first. The harness must not be able to
    # reach a real endpoint through something inherited from the shell -- the
    # mock is the only thing this run is allowed to talk to.
    for key in [k for k in env if k.startswith("COVEXCODE_")]:
        del env[key]
    env.update({
        "HOME": os.path.join(OUT, "home"),
        "COVEXCODE_CONFIG_DIR": config_dir,
        "COVEXCODE_WORKSPACE": os.path.join(OUT, "workspace"),
        "COVEXCODE_ENDPOINT": f"http://127.0.0.1:{PORT}/v1",
        "COVEXCODE_API_KEY": "test-key",
        "COVEXCODE_MODEL": "mock-model",
        "TERM": "xterm-256color",
    })

    session = Session([os.path.join(ROOT, "target", "debug", "covexcode")], env)
    try:
        session.drain(4)
        capture("00-startup.txt", session.screen)

        session.send("switch main.rs over to the helper")
        session.drain(0.8)
        session.send("\r")
        session.drain(6)
        approved, approved_full = capture("01-edit-approval.txt", session.screen)

        session.send("n")
        session.drain(2)
        asking, asking_full = capture("02-feedback-open.txt", session.screen)

        session.send("use the existing helper instead")
        session.drain(2)
        typed, typed_full = capture("03-reason-typed.txt", session.screen)

        session.send("\r")
        session.drain(9)
        after, after_full = capture("04-after-send.txt", session.screen, show_full=True)

        session.send("\x03")
        session.drain(1.5)
        armed, _ = capture("05a-ctrl-c-armed.txt", session.screen)
        session.send("\x03")
        session.drain(4)
        exited, _ = capture("05-ctrl-c-exit.txt", session.screen)
        # Whatever the farewell text did or did not make it through the replay,
        # the process actually being gone is the thing that matters.
        exited_process = session.proc.poll() is not None
    finally:
        session.close()
        mock.terminate()
        try:
            mock.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mock.kill()

    # ---- the assertions the task asks for, on the captured frames ----------
    #
    # Scope, stated plainly: this replay is not a complete terminal emulator, so
    # it can prove what the app *drew* but not perfectly where a redrawn cell
    # ended up. The one claim it therefore does NOT make here is "no leftover
    # rows" -- `ui::tests::the_feedback_prompt_replaces_the_choice_with_no_
    # leftover_rows` owns that, because it draws two frames into a single
    # TestBackend buffer, where a stale row is visible by construction.
    print("\n================ RESULT ================")
    checks = []

    def check(label, ok, detail=""):
        checks.append((label, ok, detail))
        print(
            f"{'PASS' if ok else 'FAIL'}  {label}"
            + (f"  [{detail}]" if detail and not ok else "")
        )

    def flatten(text):
        """The replay interleaves the caret glyph between characters, and
        welds rows together; strip both so the assertion is about the words."""
        return " ".join(text.replace("▏", "").split())

    check(
        "a file-edit approval prompt appeared, with its diff",
        "Apply this edit?" in approved
        and "src/main.rs" in approved
        and 'greet();' in approved,
    )
    check(
        "the y/n choice was drawn exactly once",
        approved.count("y edit") == 1 and approved.count("n skip") == 1,
        f"y edit x{approved.count('y edit')}, n skip x{approved.count('n skip')}",
    )
    check(
        "pressing n showed the one-line reason prompt",
        "Why? (optional — enter to send, esc to skip)" in asking,
    )
    check("the typed reason was echoed as it was typed", "existing helper instead" in flatten(typed))
    check(
        "the transcript shows the rejection with the exact reason",
        '✗ edit src/main.rs — rejected: "use the existing helper instead"'
        in flatten(after_full),
    )
    check(
        "the model received the rejection and changed course",
        "Understood - taking a different approach." in after_full,
    )
    check("Ctrl-C once only arms the quit", "press again to quit" in flatten(armed))
    check(
        "Ctrl-C twice exits normally",
        exited_process or "Goodbye" in flatten(exited),
        f"process alive, frame tail: {flatten(exited)[-120:]}",
    )

    failed = [c for c in checks if not c[1]]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
