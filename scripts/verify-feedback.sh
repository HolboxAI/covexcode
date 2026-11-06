#!/bin/sh
# End-to-end check of "deny with feedback" against the real binary.
#
# tmux is not installed on this machine, so this drives the TUI in `screen`
# instead -- `stuff` to type, `hardcopy -h` to capture the scrollback. That is
# the same mechanism, only a different multiplexer.
#
# Everything it creates stays under .cov-verify/ and is torn down at the end.
set -u

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT="$ROOT/.cov-verify"
rm -rf "$OUT"
mkdir -p "$OUT/home" "$OUT/workspace"
cd "$ROOT" || exit 1

PORT=8099
python3 scripts/mock-approval-server.py "$PORT" > "$OUT/mock.log" 2>&1 &
MOCK=$!
sleep 1

screen -dmS covfb sh scripts/run-tui-mock.sh
sleep 4

# Ask for something; the mock always answers with the same tool call.
screen -S covfb -X stuff "clean up the build directory please"
screen -S covfb -X stuff "\r"
sleep 4

# Capture the approval prompt before answering it.
screen -S covfb -X hardcopy -h "$OUT/01-prompt.txt"

# Deny, and say why.
screen -S covfb -X stuff "n"
sleep 1
screen -S covfb -X hardcopy -h "$OUT/02-feedback-open.txt"
screen -S covfb -X stuff "use the existing helper instead"
sleep 1
screen -S covfb -X hardcopy -h "$OUT/03-reason-typed.txt"
screen -S covfb -X stuff "\r"
sleep 4
screen -S covfb -X hardcopy -h "$OUT/04-after-send.txt"

screen -S covfb -X quit >/dev/null 2>&1
kill "$MOCK" 2>/dev/null
wait "$MOCK" 2>/dev/null
echo "captured:"
ls -1 "$OUT"
