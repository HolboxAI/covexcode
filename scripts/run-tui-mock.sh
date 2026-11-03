#!/bin/sh
# Start the TUI against the local mock endpoint, for scripts/verify-feedback.sh.
#
# A separate file rather than a long inline command because `screen` re-parses
# whatever it is handed, and nested quoting there was silently killing the
# session before any output could be captured.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT="$ROOT/.cov-verify"

HOME="$OUT/home" \
COVEXCODE_CONFIG_DIR="$OUT/home/.covexcode" \
COVEXCODE_ENDPOINT="http://127.0.0.1:8099/v1" \
COVEXCODE_API_KEY=test-key \
COVEXCODE_MODEL=mock-model \
COVEXCODE_WORKSPACE="$OUT/workspace" \
exec "$ROOT/target/debug/covexcode" 2> "$OUT/tui-err.txt"
