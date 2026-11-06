#!/usr/bin/env python3
"""A throwaway OpenAI-compatible endpoint that always asks for one file edit.

Exists for one purpose: driving the real TUI through an edit_file approval
without a model, a key or a network. It streams a single `edit_file` call
against `src/main.rs`, which is the shape that produces a file-edit approval
prompt with a diff preview.

Driven by scripts/pty-capture.py; not part of the build and not referenced by
any test -- src/app.rs and src/ui.rs cover this logic with TestBackend instead.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8099


def tool_call_frame() -> dict:
    return {
        "choices": [
            {
                "delta": {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call_mock_edit_1",
                            "type": "function",
                            "function": {
                                "name": "edit_file",
                                "arguments": json.dumps(
                                    {
                                        "path": "src/main.rs",
                                        "old_string": "fn main() {\n    println!(\"hi\");\n}\n",
                                        "new_string": "fn main() {\n    greet();\n}\n",
                                    }
                                ),
                            },
                        }
                    ]
                }
            }
        ]
    }


def prose_frame() -> dict:
    return {
        "choices": [
            {"delta": {"content": "Understood - taking a different approach."}}
        ]
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        payload = body.decode("utf-8", "replace")

        # A second request carries the tool result back, so answer with prose
        # to end the turn instead of asking for another edit forever.
        frame = prose_frame() if '"role":"tool"' in payload else tool_call_frame()

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        out = (
            "data: " + json.dumps(frame) + "\n\n"
            "data: "
            + json.dumps({"choices": [{"delta": {}, "finish_reason": "stop"}]})
            + "\n\n"
            "data: [DONE]\n\n"
        )
        self.wfile.write(out.encode())
        self.wfile.flush()

    def log_message(self, *args):
        sys.stderr.write("mock: " + (args[0] % args[1:]) + "\n")


if __name__ == "__main__":
    print(f"mock edit-approval server on http://127.0.0.1:{PORT}/v1", flush=True)
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
