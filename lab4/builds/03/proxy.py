#!/usr/bin/env python3
"""proxy.py: the agent-traffic proxy for Lab 4, with a live dashboard.

A reverse proxy that prints every request and response and forwards them
unchanged. Standard library only; nothing to install.

    python3 proxy.py                       # listens on http://localhost:8041
    ANTHROPIC_BASE_URL=http://localhost:8041 claude
    open http://localhost:8041/__dashboard

Everything Claude Code sends to the API now passes through this program and is
printed to the terminal. The proxy changes nothing: headers go through untouched
(a subscription login needs its bearer token and its `anthropic-beta` header),
and the reply is streamed back byte for byte as it arrives.

On top of that, every request and its reply are tagged with one exchange id and
recorded in memory (the last 500), and published as Server-Sent Events on
`/__events`. `/__dashboard` serves `dashboard.html`, which turns those events
into a readable live view. Paths starting with `/__` are never forwarded.

    python3 proxy.py --demo                # also replays a built-in sample; no API needed
    python3 proxy.py --port 9000
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
"""

from __future__ import annotations

import argparse
import base64
import codecs
import copy
import http.client
import json
import os
import queue
import random
import string
import sys
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

# Headers that describe one hop of the connection, not the message. They are
# never forwarded. `accept-encoding` is dropped too, so the API answers in plain
# text and what we print is readable.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}

UPSTREAM = "https://api.anthropic.com"
HERE = os.path.dirname(os.path.abspath(__file__))


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Exchange store and event publishing (section 4 of SPEC.md)
# ---------------------------------------------------------------------------

MAX_EXCHANGES = 500   # exchanges kept in memory, oldest dropped first
QUEUE_LIMIT = 1000    # a page whose queue reaches this many items is dropped
KEEPALIVE_S = 15

LOCK = threading.Lock()          # guards STORE, CLIENTS and NEXT_ID
ID_LOCK = threading.Lock()       # keeps request events in id order
STORE: "OrderedDict[int, dict]" = OrderedDict()
CLIENTS: "set[Client]" = set()
NEXT_ID = 0


class Client:
    """One connected dashboard page."""

    def __init__(self) -> None:
        self.q: "queue.Queue[str]" = queue.Queue()
        self.dropped = False


def now_ms() -> float:
    return time.time() * 1000.0


def shorten(name: str, value: str) -> str:
    if name.lower() in ("authorization", "x-api-key"):
        return value[:16] + "..."
    return value


def header_pairs(items) -> list:
    return [[name, shorten(name, value)] for name, value in items]


def sse_line(ev: dict) -> str:
    return "data: " + json.dumps(ev, ensure_ascii=False) + "\n\n"


def publish(ev: dict) -> None:
    """Record an event in the store and hand it to every connected page.

    Never blocks: a page that falls 1000 events behind is dropped (it
    reconnects by itself and gets a fresh replay).
    """
    line = sse_line(ev)  # serialize outside the lock
    with LOCK:
        kind, xid = ev["type"], ev["id"]
        if kind == "request":
            STORE[xid] = {"request": ev, "start": None, "text": [], "chunk_t": None, "end": None}
            while len(STORE) > MAX_EXCHANGES:
                STORE.popitem(last=False)
        else:
            rec = STORE.get(xid)
            if rec is not None:
                if kind == "response_start":
                    rec["start"] = ev
                elif kind == "response_chunk":
                    rec["text"].append(ev["text"])
                    rec["chunk_t"] = ev["t"]
                elif kind == "response_end":
                    rec["end"] = ev
        for client in list(CLIENTS):
            if client.q.qsize() >= QUEUE_LIMIT:
                CLIENTS.discard(client)
                client.dropped = True
            else:
                client.q.put_nowait(line)


class Exchange:
    """Records one request and its reply as events. Used by real traffic and by --demo."""

    def __init__(self, method: str, path: str, headers, body: bytes) -> None:
        global NEXT_ID
        self.decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.ended = False
        body_json, body_text = None, None
        if body:
            try:
                body_json = json.loads(body)
            except ValueError:
                body_text = body.decode("utf-8", "replace")
        with ID_LOCK:
            with LOCK:
                NEXT_ID += 1
                self.id = NEXT_ID
            publish({
                "type": "request", "id": self.id, "t": now_ms(), "method": method, "path": path,
                "headers": header_pairs(headers), "body": body_text, "body_json": body_json,
            })

    def start(self, status: int, reason: str, headers) -> None:
        publish({"type": "response_start", "id": self.id, "t": now_ms(), "status": status,
                 "reason": reason, "headers": header_pairs(headers)})

    def chunk(self, data: bytes) -> None:
        text = self.decoder.decode(data)
        if text:
            publish({"type": "response_chunk", "id": self.id, "t": now_ms(), "text": text})

    def end(self, ok: bool, error: str | None) -> None:
        if self.ended:
            return
        self.ended = True
        tail = self.decoder.decode(b"", final=True)
        if tail:
            publish({"type": "response_chunk", "id": self.id, "t": now_ms(), "text": tail})
        publish({"type": "response_end", "id": self.id, "t": now_ms(), "ok": ok, "error": error})


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------


class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # we print our own lines

    def do_POST(self) -> None:
        self.route()

    def do_GET(self) -> None:
        self.route()

    def do_HEAD(self) -> None:
        self.route()

    def route(self) -> None:
        if self.path.startswith("/__"):
            self.local()
        else:
            self.relay()

    # -- local routes: never forwarded, never printed -----------------------

    def local(self) -> None:
        path = self.path.split("?", 1)[0]
        self.close_connection = True
        if path == "/__dashboard":
            self.serve_dashboard()
        elif path == "/__events":
            self.serve_events()
        else:
            self.send_error(404, "unknown local route")

    def serve_dashboard(self) -> None:
        try:
            with open(os.path.join(HERE, "dashboard.html"), "rb") as f:  # read on every request
                data = f.read()
        except OSError as e:
            self.send_error(500, f"cannot read dashboard.html: {e}")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def serve_events(self) -> None:
        client = Client()
        with LOCK:  # snapshot and subscribe atomically, so nothing is lost or doubled
            snapshot = [(r["request"], r["start"], list(r["text"]), r["chunk_t"], r["end"])
                        for r in STORE.values()]
            CLIENTS.add(client)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command == "HEAD":
                return
            self.wfile.write(sse_line({"type": "reset"}).encode())
            for req, start, texts, chunk_t, end in snapshot:
                parts = [sse_line(req)]
                if start is not None:
                    parts.append(sse_line(start))
                if texts:
                    parts.append(sse_line({"type": "response_chunk", "id": req["id"],
                                           "t": chunk_t, "text": "".join(texts)}))
                if end is not None:
                    parts.append(sse_line(end))
                self.wfile.write("".join(parts).encode())
            self.wfile.flush()
            while not client.dropped:
                try:
                    batch = [client.q.get(timeout=KEEPALIVE_S)]
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                while len(batch) < 200:
                    try:
                        batch.append(client.q.get_nowait())
                    except queue.Empty:
                        break
                self.wfile.write("".join(batch).encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with LOCK:
                CLIENTS.discard(client)

    # -- the proxy itself ----------------------------------------------------

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        ex = Exchange(self.command, self.path, self.headers.items(), body)
        error: str | None = None
        try:
            # 1. Print what the agent sent.
            print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
            for name, value in self.headers.items():
                if name.lower() in ("authorization", "x-api-key"):
                    value = value[:16] + "..."
                print(f"    {name}: {value}")
            if body:
                try:
                    print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
                except ValueError:
                    print(body.decode("utf-8", "replace"))

            # 2. Forward it, headers intact.
            target = urlsplit(UPSTREAM)
            conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
            # Connect with a short timeout (a lost SYN must not stall the agent for
            # minutes); the long timeout is for reading a slow stream.
            upstream = conn_cls(target.netloc, timeout=10)
            try:
                upstream.connect()
            except OSError as e:
                print(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}", flush=True)
                error = f"cannot reach {UPSTREAM}: {e}"
                ex.start(502, "Bad Gateway", [])
                self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
                self.close_connection = True
                return
            upstream.sock.settimeout(600)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()

            # 3. Stream the reply back as it arrives, printing it on the way.
            print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
            ex.start(reply.status, reply.reason, reply.getheaders())
            self.send_response(reply.status, reply.reason)
            for name, value in reply.getheaders():
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command == "HEAD":
                self.close_connection = True
                return
            while chunk := reply.read1(4096):  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except OSError as e:
                    error = f"agent disconnected: {e}"
                    raise
                ex.chunk(chunk)
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
            self.close_connection = True
            print(f"\n[{stamp()}] --- done", flush=True)
        except Exception as e:
            if error is None:
                error = f"{type(e).__name__}: {e}"
            raise
        finally:
            ex.end(error is None, error)  # always sent, so no row stays "pending"


# ---------------------------------------------------------------------------
# Demo mode (section 9 of SPEC.md): a built-in sample published through the
# same Exchange class as real traffic.
# ---------------------------------------------------------------------------

DEMO_MODEL = "claude-opus-5-5"
DEMO_MODEL_B = "claude-sonnet-5-5"
DEMO_SMALL = "claude-haiku-5-5"
DEMO_CWD = "/Users/demo/lab4"
DEMO_BETA = ("claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,"
             "context-management-2025-06-27,prompt-caching-scope-2026-01-05,effort-2025-11-24,"
             "fine-grained-tool-streaming-2025-05-14")


class DemoIds:
    B62 = string.ascii_letters + string.digits

    def __init__(self, rnd: random.Random) -> None:
        self.rnd = rnd

    def _b62(self, n: int) -> str:
        return "".join(self.rnd.choice(self.B62) for _ in range(n))

    def toolu(self) -> str:
        return "toolu_01" + self._b62(22)

    def msg(self) -> str:
        return "msg_01" + self._b62(22)

    def req(self) -> str:
        return "req_011C" + self._b62(20)

    def uuid(self) -> str:
        return str(uuid.UUID(int=self.rnd.getrandbits(128), version=4))

    def hex(self, n: int = 64) -> str:
        return "".join(self.rnd.choice("0123456789abcdef") for _ in range(n))

    def sig(self) -> str:
        return base64.b64encode(bytes(self.rnd.getrandbits(8) for _ in range(330))).decode()


def _s(d: str) -> dict:
    return {"type": "string", "description": d}


def _tool(name: str, description: str, props: dict, required=(), deferred: bool = False) -> dict:
    t = {"name": name, "description": description, "input_schema": {
        "$schema": "http://json-schema.org/draft-07/schema#", "type": "object",
        "properties": props, "required": list(required), "additionalProperties": False}}
    if deferred:
        t["defer_loading"] = True
    return t


BASH_DESC = """Executes a given bash command and returns its output.

The working directory persists between commands, but shell state does not.

IMPORTANT: Avoid using this tool to run `cat`, `head`, `tail`, `sed`, `awk`, or `echo` commands, unless explicitly instructed. Instead, use the appropriate dedicated tool.

# Committing changes with git
Only create commits when requested by the user. When the user asks you to create a new git commit:
1. Run git status, git diff and git log in parallel.
2. Draft a concise commit message that focuses on the "why" rather than the "what".
3. Pass the commit message via a HEREDOC to ensure correct formatting.
- NEVER update the git config
- NEVER run destructive git commands (push --force, hard reset) unless the user explicitly requests them"""


def demo_tools() -> list:
    num = {"type": "number"}
    return [
        _tool("Agent", "Launch a new agent to handle complex, multi-step tasks autonomously.\n\nEach agent type has specific capabilities and tools available to it.",
              {"description": _s("A short (3-5 word) description of the task"), "prompt": _s("The task for the agent to perform"),
               "subagent_type": _s("The type of specialized agent to use for this task")}, ["description", "prompt"]),
        _tool("Bash", BASH_DESC, {"command": _s("The command to execute"), "timeout": dict(num, description="Optional timeout in milliseconds (max 600000)"),
                                  "description": _s("Clear, concise description of what this command does in 5-10 words"),
                                  "run_in_background": {"type": "boolean", "description": "Run this command in the background"}}, ["command"]),
        _tool("Glob", "Fast file pattern matching tool that works with any codebase size.\nSupports glob patterns like \"**/*.js\".",
              {"pattern": _s("The glob pattern to match files against"), "path": _s("The directory to search in")}, ["pattern"]),
        _tool("Grep", "A powerful search tool built on ripgrep.\nSupports full regex syntax.",
              {"pattern": _s("The regular expression pattern to search for"), "path": _s("File or directory to search in"),
               "glob": _s("Glob pattern to filter files"), "output_mode": {"type": "string", "enum": ["content", "files_with_matches", "count"]}}, ["pattern"]),
        _tool("Read", "Reads a file from the local filesystem.\nThe file_path parameter must be an absolute path.",
              {"file_path": _s("The absolute path to the file to read"), "offset": num, "limit": num}, ["file_path"]),
        _tool("Edit", "Performs exact string replacements in files.\nYou must use your Read tool at least once before editing.",
              {"file_path": _s("The absolute path to the file to modify"), "old_string": _s("The text to replace"),
               "new_string": _s("The text to replace it with"), "replace_all": {"type": "boolean", "default": False}}, ["file_path", "old_string", "new_string"]),
        _tool("Write", "Writes a file to the local filesystem.\nThis tool will overwrite the existing file if there is one.",
              {"file_path": _s("The absolute path to the file to write"), "content": _s("The content to write to the file")}, ["file_path", "content"]),
        _tool("NotebookEdit", "Completely replaces the contents of a specific cell in a Jupyter notebook.",
              {"notebook_path": _s("Absolute path to the notebook"), "cell_id": _s("The ID of the cell to edit"), "new_source": _s("The new source for the cell")}, ["notebook_path", "new_source"]),
        _tool("WebFetch", "Fetches content from a specified URL and processes it using an AI model.",
              {"url": {"type": "string", "format": "uri"}, "prompt": _s("The prompt to run on the fetched content")}, ["url", "prompt"]),
        _tool("WebSearch", "Allows Claude to search the web and use the results to inform responses.",
              {"query": _s("The search query to use")}, ["query"]),
        _tool("TodoWrite", "Use this tool to create and manage a structured task list for your current coding session.",
              {"todos": {"type": "array", "items": {"type": "object", "properties": {"content": {"type": "string"}, "status": {"type": "string", "enum": ["pending", "in_progress", "completed"]}}}}}, ["todos"]),
        _tool("BashOutput", "Retrieves output from a running or completed background bash shell.", {"bash_id": _s("The ID of the background shell")}, ["bash_id"]),
        _tool("KillShell", "Kills a running background bash shell by its ID.", {"shell_id": _s("The ID of the background shell to kill")}, ["shell_id"]),
        _tool("ExitPlanMode", "Use this tool when you are in plan mode and have finished presenting your plan.", {"plan": _s("The plan you came up with")}, ["plan"]),
        _tool("Skill", "Execute a skill within the main conversation.", {"skill": _s("The skill name")}, ["skill"]),
        _tool("SlashCommand", "Execute a slash command within the main conversation.", {"command": _s("The slash command to execute")}, ["command"]),
        _tool("AskUserQuestion", "Use this tool when you need to ask the user questions during execution.",
              {"questions": {"type": "array", "items": {"type": "object"}}}, ["questions"]),
        _tool("ToolSearch", "Fetches full schema definitions for deferred tools so they can be called.\nQuery forms: \"select:Name1,Name2\" or keywords.",
              {"query": _s("Query to find deferred tools"), "max_results": dict(num, default=5)}, ["query"]),
        _tool("ListMcpResourcesTool", "Lists available resources from configured MCP servers.", {"server": _s("Optional server name")}, deferred=True),
        _tool("DeferredToolPlaceholder", "Placeholder for tools whose definitions are loaded on demand with ToolSearch.", {}),
    ]


TASKSTOP_TOOL = _tool("TaskStop", "Stops a running background task by its ID.", {"task_id": _s("The ID of the task to stop")}, ["task_id"])

_PROMPT_SECTIONS = [
    ("Tone and style", [
        "Your responses should be short and concise. You can use Github-flavored markdown for formatting.",
        "Only use emojis if the user explicitly requests it. Avoid using emojis in all communication unless asked.",
        "Output text to communicate with the user; all text you output outside of tool use is displayed to the user.",
        "Never create files unless they're absolutely necessary for achieving your goal. Prefer editing an existing file.",
    ]),
    ("Professional objectivity", [
        "Prioritize technical accuracy and truthfulness over validating the user's beliefs.",
        "Focus on facts and problem-solving, providing direct, objective technical info without unnecessary superlatives.",
        "When there is uncertainty, investigate to find the truth first rather than instinctively confirming the user's beliefs.",
    ]),
    ("Task management", [
        "You have access to the TodoWrite tool to help you manage and plan tasks. Use it very frequently.",
        "Mark todos as completed as soon as you are done with a task. Do not batch up multiple tasks before marking them.",
        "Break larger, complex tasks into smaller steps so the user can follow your progress.",
    ]),
    ("Doing tasks", [
        "The user will primarily request you perform software engineering tasks: solving bugs, adding functionality, refactoring, explaining code.",
        "Use the available search tools to understand the codebase and the user's query before changing anything.",
        "Implement the solution using all tools available to you, then verify it with tests where possible.",
        "Be careful not to introduce security vulnerabilities such as command injection, XSS or SQL injection.",
    ]),
    ("Tool usage policy", [
        "When doing file search, prefer to use the Agent tool in order to reduce context usage.",
        "You can call multiple tools in a single response. If there are no dependencies between them, make all independent calls in parallel.",
        "Use specialized tools instead of bash commands when possible: Read instead of cat, Edit instead of sed, Write instead of echo redirection.",
        "Never use bash echo or other command-line tools to communicate thoughts or explanations to the user.",
    ]),
    ("Code references", [
        "When referencing specific functions or pieces of code include the pattern `file_path:line_number`.",
        "This allows the user to easily navigate to the source code location.",
    ]),
]
DEMO_MAIN_PROMPT = (
    "You are an interactive CLI tool that helps users with software engineering tasks. Use the instructions below "
    "and the tools available to you to assist the user.\n\nIMPORTANT: Assist with authorized security testing, "
    "defensive security, CTF challenges, and educational contexts.\n\n"
    + "\n\n".join(f"# {title}\n" + "\n".join(f"- {line}" for line in lines) for title, lines in _PROMPT_SECTIONS)
    + "\n\n# Environment\n<env>\nWorking directory: " + DEMO_CWD + "\nIs directory a git repo: Yes\nPlatform: darwin\n"
    "OS Version: Darwin 25.6.0\nToday's date: 2026-11-04\n</env>\n\n"
    "gitStatus: This is the git status at the start of the conversation.\nCurrent branch: main\n\nStatus:\n M proxy.py\n?? dashboard.html\n\n"
    "Recent commits:\n" + "\n".join(f"{i:07x} Lab 4 spec: revision {i}" for i in range(0x1a2b3c, 0x1a2b3c + 40, 3))
)

DEMO_REMINDER_1 = (
    "<system-reminder>\nAs you answer the user's questions, you can use the following context:\n# claudeMd\n"
    "Codebase and user instructions are shown below. Be sure to adhere to these instructions.\n\n"
    f"Contents of {DEMO_CWD}/CLAUDE.md (project instructions, checked into the codebase):\n\n"
    "- Python standard library only; nothing to install.\n- Keep the proxy's terminal output unchanged.\n"
    "- The dashboard must never use innerHTML with data.\n\n# currentDate\nToday's date is 2026-11-04.\n\n"
    "      IMPORTANT: this context may or may not be relevant to your tasks.\n</system-reminder>\n"
)
DEMO_REMINDER_2 = (
    "<system-reminder>\nThe following skills are available for use with the Skill tool:\n\n"
    "- simplify: Review changed code for reuse, quality, and efficiency, then fix any issues found.\n"
    "- run: Launch and drive this project's app to see a change working.\n"
    "- code-review: Review the current diff for correctness bugs.\n</system-reminder>"
)


def demo_agent_context() -> str:
    lines = ["<system-reminder>", "Agent context for this session. Deferred tools, skills and project files follow.", ""]
    areas = ["proxy", "dashboard", "spec", "tests", "notes", "lab2", "lab3", "week4"]
    for i in range(1, 196):
        area = areas[i % len(areas)]
        lines.append(f"- {area}/item-{i:03d}: context entry {i} describing a file, skill or tool the agent may use "
                     f"while working in {DEMO_CWD}/{area}.")
    lines.append("</system-reminder>")
    return "\n".join(lines)


def demo_read_result() -> str:
    try:
        with open(os.path.abspath(__file__), encoding="utf-8") as f:
            src = f.read().splitlines()[:140]
    except OSError:
        src = [f"# line {i}" for i in range(1, 141)]
    return "\n".join(f"{n:>6}\t{line}" for n, line in enumerate(src, 1))


DEMO_COMMIT_CMD = """git add proxy.py dashboard.html && git commit -F - <<'EOF'
Lab 4: live traffic dashboard

proxy.py now tags every request and reply with an exchange id,
keeps the last 500 exchanges in memory and serves them as
Server-Sent Events on /__events. dashboard.html renders them:
- one row per exchange, newest first
- prompts, replies, tool calls and results in their own colors
- tool calls paired with their results by tool_use_id

Refs: lab4/SPEC.md
EOF"""

DEMO_STATUS_RESULT = (
    " M proxy.py\n?? dashboard.html\n?? notes/<b>hi</b><script>alert(1)</script>.txt\n"
    "e26de5c Lab 4 spec: thread chips to follow one session at a time\n"
    "9df2dde Lab 4 spec: keep the tool call summary to one line\n"
    "1cdbc05 Lab 4 spec: classify prompt-suggestion calls as side calls"
)
DEMO_COMMIT_ERROR = (
    f"Exit code 128\nfatal: Unable to create '{DEMO_CWD}/.git/index.lock': File exists.\n\n"
    "Another git process seems to be running in this repository, e.g.\nan editor opened by 'git commit'. "
    "Please make sure all processes\nare terminated then try again. If it still fails, a git process\n"
    "may have crashed in this repository earlier:\nremove the file manually to continue."
)

DEMO_A2_TEXT = (
    "`proxy.py` is a small **reverse proxy** for watching an agent's API traffic — standard library only.\n\n"
    "What it does, step by step:\n\n"
    "1. Listens on `127.0.0.1:8041` with a `ThreadingHTTPServer`, so several calls can run at once.\n"
    "2. For every `GET`/`POST`/`HEAD` it reads the body and prints the method, path, headers "
    "(with `Authorization` cut to 16 characters) and the pretty-printed JSON body.\n"
    "3. It forwards the request to `https://api.anthropic.com` with the same headers, minus hop-by-hop "
    "ones in `SKIP` (and `accept-encoding`, so replies stay readable).\n"
    "4. It streams the reply back with `read1(4096)`, writing and printing each chunk as it lands.\n"
    "5. If the upstream can't be reached it answers `502`.\n\n"
    "Nothing is modified on the way through — it is a pure tap."
)
DEMO_SUGGESTION = (
    "[SUGGESTION MODE: Suggest what the user might naturally type next into Claude Code.]\n\n"
    "Reply with only the suggested text, in the user's voice, at most 12 words. If nothing fits, reply with an empty string."
)
DEMO_A3_PROMPT = "Commit the work and show me the status"
DEMO_A4_TEXT = ("The status check worked, but the commit failed: another git process holds `.git/index.lock`. "
                "A background task may still be running — let me load the tool that can stop it.")
DEMO_A5_TEXT = (
    "Loaded `TaskStop`. Before stopping anything I checked: no background task is registered in this session, "
    "so the lock is most likely left over from a git process that crashed.\n\nYou can remove it safely with:\n\n"
    "    rm .git/index.lock\n\nthen re-run the commit. The commit message is ready — it describes the exchange "
    "store, the `/__events` stream and the dashboard."
)
DEMO_A6_PROMPT = "ok, remove the lock and commit again"
DEMO_A6_TEXT = "Removing the stale lock and retrying the commit with the same message. After that I'll show the log."
DEMO_B_PROMPT = "Write a haiku about proxies"
DEMO_HAIKU = "Requests pass through me\nI copy every whisper\nand forward the rest\n"
DEMO_B2_TEXT = "Saved to `haiku/proxies.txt`:\n\n> Requests pass through me\n> I copy every whisper\n> and forward the rest"
DEMO_B3_PROMPT = "Now one about reverse proxies specifically"
DEMO_B3_TEXT = "> One face to the world —\n> many servers hide behind,\n> I answer for all."
DEMO_TITLE_SYSTEM = (
    "Generate a concise, sentence-case title (3-7 words) that captures the main topic or goal of this coding "
    "session. The title should be clear enough that the user recognizes the session in a list. "
    "Return JSON with a single \"title\" field."
)


def demo_usage(inp: int, cw: int, cr: int) -> dict:
    return {"input_tokens": inp, "cache_creation_input_tokens": cw, "cache_read_input_tokens": cr,
            "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": cw},
            "output_tokens": 0, "service_tier": "standard"}


def demo_split(s: str, rnd: random.Random, lo: int = 6, hi: int = 28) -> list:
    out, i = [], 0
    while i < len(s):
        n = rnd.randint(lo, hi)
        out.append(s[i:i + n])
        i += n
    return out


def demo_cut(data: bytes, rnd: random.Random) -> list:
    """Cut raw bytes into network-like chunks; many end mid-line (or mid-character)."""
    out, i = [], 0
    while i < len(data):
        n = rnd.randint(40, 320)
        out.append(data[i:i + n])
        i += n
    return out


def demo_sse(rnd: random.Random, ids: DemoIds, model: str, usage: dict, blocks: list, stop_reason: str,
             out_tokens: int, thinking_tokens: int = 0, complete: bool = True, parallel: bool = False) -> str:
    """Build an SSE reply body from block specs: ("thinking", sig) / ("text", s) / ("tool_use", id, name, input)."""
    events: list = []

    def ev(name: str, obj: dict, pad: str = "") -> None:
        events.append(f"event: {name}\ndata: {json.dumps(obj, ensure_ascii=False)}{pad}\n\n")

    ev("message_start", {"type": "message_start", "message": {
        "id": ids.msg(), "type": "message", "role": "assistant", "model": model, "content": [],
        "stop_reason": None, "stop_sequence": None, "usage": dict(copy.deepcopy(usage), output_tokens=1)}})
    seqs = []
    for i, b in enumerate(blocks):
        if b[0] == "thinking":
            start = {"type": "thinking", "thinking": "", "signature": ""}
            deltas = [{"type": "signature_delta", "signature": b[1]}]
        elif b[0] == "text":
            start = {"type": "text", "text": ""}
            deltas = [{"type": "text_delta", "text": p} for p in demo_split(b[1], rnd)]
        else:
            _, tid, name, inp = b
            start = {"type": "tool_use", "id": tid, "name": name, "input": {}, "caller": {"type": "direct"}}
            deltas = [{"type": "input_json_delta", "partial_json": ""}] + [
                {"type": "input_json_delta", "partial_json": p} for p in demo_split(json.dumps(inp, ensure_ascii=False), rnd, 10, 40)]
        seqs.append((i, start, deltas))
    groups: list = []
    for s in seqs:  # parallel tool calls stream with their deltas interleaved
        if parallel and groups and groups[-1][-1][1]["type"] == "tool_use" and s[1]["type"] == "tool_use":
            groups[-1].append(s)
        else:
            groups.append([s])
    pinged = False
    for group in groups:
        for i, start, _ in group:
            ev("content_block_start", {"type": "content_block_start", "index": i, "content_block": start})
            if not pinged:
                ev("ping", {"type": "ping"}, "    ")  # trailing spaces, as seen in the capture
                pinged = True
        pending = [list(d) for _, _, d in group]
        while any(pending):
            for gi, (i, _, _) in enumerate(group):
                if pending[gi]:
                    ev("content_block_delta", {"type": "content_block_delta", "index": i, "delta": pending[gi].pop(0)},
                       rnd.choice(["", "", "", " ", "   "]))
        for i, _, _ in group:
            ev("content_block_stop", {"type": "content_block_stop", "index": i})
    if not complete:
        return "".join(events[:max(4, int(len(events) * 0.6))])
    final = dict(copy.deepcopy(usage), output_tokens=out_tokens, output_tokens_details={"thinking_tokens": thinking_tokens})
    ev("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop_reason, "stop_sequence": None}, "usage": final})
    ev("message_stop", {"type": "message_stop"})
    return "".join(events)


def as_message(blocks: list) -> dict:
    content = []
    for b in blocks:
        if b[0] == "thinking":
            content.append({"type": "thinking", "thinking": "", "signature": b[1]})
        elif b[0] == "text":
            content.append({"type": "text", "text": b[1]})
        else:
            content.append({"type": "tool_use", "id": b[1], "name": b[2], "input": b[3], "caller": {"type": "direct"}})
    return {"role": "assistant", "content": content}


def cache_last(messages: list) -> list:
    """Copy the messages and move the cache_control marker to the last user block, as the agent does."""
    msgs = copy.deepcopy(messages)
    for m in msgs:
        if isinstance(m["content"], list):
            for b in m["content"]:
                b.pop("cache_control", None)
    for m in reversed(msgs):
        if m["role"] == "user" and isinstance(m["content"], list) and m["content"]:
            m["content"][-1]["cache_control"] = {"type": "ephemeral"}
            break
    return msgs


def demo_pass(rnd: random.Random) -> None:
    ids = DemoIds(rnd)
    session_a, session_b = ids.uuid(), ids.uuid()
    device, account = ids.hex(), ids.uuid()
    system = [
        {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.318.e4f; cc_entrypoint=cli;"},
        {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude.", "cache_control": {"type": "ephemeral"}},
        {"type": "text", "text": DEMO_MAIN_PROMPT, "cache_control": {"type": "ephemeral", "ttl": "1h"}},
    ]
    tools = demo_tools()

    def user_id(session: str) -> str:
        return json.dumps({"device_id": device, "account_uuid": account, "session_id": session})

    def agent_headers(session: str) -> list:
        return [("Host", "localhost:8041"), ("Accept", "application/json"), ("Content-Type", "application/json"),
                ("User-Agent", "claude-cli/2.1.318 (external, cli)"), ("X-Claude-Code-Session-Id", session),
                ("anthropic-version", "2023-06-01"), ("anthropic-beta", DEMO_BETA), ("x-app", "cli"),
                ("Authorization", "Bearer sk-ant-oat01-DEMO-not-a-real-token-0123456789")]

    def send(session: str, body: dict, path: str = "/v1/messages?beta=true") -> Exchange:
        raw = json.dumps(body, ensure_ascii=False).encode()
        return Exchange("POST", path, agent_headers(session) + [("Content-Length", str(len(raw)))], raw)

    def main_body(model: str, session: str, messages: list, tool_list: list) -> dict:
        return {"model": model, "messages": cache_last(messages), "system": system, "tools": tool_list,
                "metadata": {"user_id": user_id(session)}, "max_tokens": 32000,
                "thinking": {"type": "adaptive", "display": "updates"},
                "context_management": {"edits": [{"type": "clear_thinking_20251015", "keep": "all"}]},
                "safeguards": {"mode": "standard"}, "output_config": {"effort": "medium"}, "stream": True}

    def sse_headers() -> list:
        return [("Content-Type", "text/event-stream; charset=utf-8"), ("Cache-Control", "no-cache"),
                ("request-id", ids.req()), ("anthropic-organization-id", ids.uuid()), ("via", "1.1 google")]

    def stream(ex: Exchange, body: str, ok: bool = True, error: str | None = None) -> None:
        time.sleep(rnd.uniform(0.3, 0.7))  # time to first byte
        ex.start(200, "OK", sse_headers())
        for piece in demo_cut(body.encode(), rnd):
            ex.chunk(piece)
            time.sleep(rnd.uniform(0.04, 0.08))
        ex.end(ok, error)

    def gap() -> None:
        time.sleep(1.5)

    # 1. HEAD connectivity check
    ex = Exchange("HEAD", "/api/hello", [("Host", "localhost:8041"), ("User-Agent", "Bun/1.3.2"), ("Accept", "*/*")], b"")
    time.sleep(0.12)
    ex.start(200, "OK", [("Content-Type", "application/json"), ("Content-Length", "0")])
    ex.end(True, None)
    gap()

    # 2. Quota probe (429) and title call at the same time, chunks interleaved
    quota = send(session_a, {"model": DEMO_SMALL, "max_tokens": 1, "messages": [{"role": "user", "content": "quota"}],
                             "metadata": {"user_id": user_id(session_a)}})
    title = send(session_a, {
        "model": DEMO_SMALL, "max_tokens": 512, "messages": [{"role": "user", "content": "Read proxy.py and tell me what it does"}],
        "system": [{"type": "text", "text": DEMO_TITLE_SYSTEM}], "tools": [], "metadata": {"user_id": user_id(session_a)},
        "output_config": {"effort": "medium", "format": {"type": "json_schema", "schema": {
            "type": "object", "properties": {"title": {"type": "string"}}, "required": ["title"], "additionalProperties": False}}},
        "stream": True})
    time.sleep(0.35)
    title.start(200, "OK", sse_headers())
    rid = ids.req()
    quota.start(429, "Too Many Requests", [("Content-Type", "application/json"), ("request-id", rid), ("retry-after", "12")])
    err = json.dumps({"type": "error", "error": {"type": "rate_limit_error", "message": "Error"}, "request_id": rid}).encode()
    t_chunks = demo_cut(demo_sse(rnd, ids, DEMO_SMALL, demo_usage(212, 0, 0), [("text", '{"title":"Proxy summary"}')], "end_turn", 9).encode(), rnd)
    q_chunks = [err[:37], err[37:]]
    while t_chunks or q_chunks:
        if t_chunks:
            title.chunk(t_chunks.pop(0))
            time.sleep(rnd.uniform(0.04, 0.08))
        if q_chunks:
            quota.chunk(q_chunks.pop(0))
            if not q_chunks:
                quota.end(True, None)
            time.sleep(rnd.uniform(0.04, 0.08))
    title.end(True, None)
    gap()

    # 3. Main thread A, request 1
    conv: list = [
        {"role": "user", "content": [{"type": "text", "text": DEMO_REMINDER_1}, {"type": "text", "text": DEMO_REMINDER_2},
                                     {"type": "text", "text": "Read proxy.py and tell me what it does"}]},
        {"role": "system", "content": [
            {"type": "text", "text": demo_agent_context()},
            {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": {
                "name": "mcp__ide__getDiagnostics", "description": "Get language diagnostics from VS Code",
                "input_schema": {"type": "object", "properties": {"uri": {"type": "string"}}}}}},
            {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": {
                "name": "mcp__ide__executeCode", "description": "Execute python code in the Jupyter kernel for the current notebook file",
                "input_schema": {"type": "object", "properties": {"code": {"type": "string"}}, "required": ["code"]}}}},
        ]},
    ]
    read_id = ids.toolu()
    a1 = [("thinking", ids.sig()), ("tool_use", read_id, "Read", {"file_path": f"{DEMO_CWD}/proxy.py"})]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(2, 30315, 0), a1, "tool_use", 112, 41))
    conv += [as_message(a1), {"role": "user", "content": [{"type": "tool_result", "tool_use_id": read_id, "content": demo_read_result()}]}]
    gap()

    # A2: the Read result; reply is model text
    a2 = [("text", DEMO_A2_TEXT)]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(1, 4210, 30315), a2, "end_turn", 286))
    conv.append(as_message(a2))
    gap()

    # Prompt-suggestion side call: looks like a main request plus one extra user message
    sugg = conv + [{"role": "user", "content": [{"type": "text", "text": DEMO_SUGGESTION}]}]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, sugg, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(1, 0, 34525), [("text", "now read SPEC.md")], "end_turn", 6))
    gap()

    # Second session, request 1
    conv_b: list = [{"role": "user", "content": [{"type": "text", "text": DEMO_REMINDER_1}, {"type": "text", "text": DEMO_B_PROMPT}]}]
    write_id = ids.toolu()
    b1 = [("thinking", ids.sig()), ("text", "Here's one — I'll also save it to a file."),
          ("tool_use", write_id, "Write", {"file_path": "/Users/demo/haiku/proxies.txt", "content": DEMO_HAIKU})]
    ex = send(session_b, main_body(DEMO_MODEL_B, session_b, conv_b, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL_B, demo_usage(2, 29870, 0), b1, "tool_use", 96, 22))
    conv_b += [as_message(b1), {"role": "user", "content": [{"type": "tool_result", "tool_use_id": write_id,
                                                             "content": "File created successfully at: /Users/demo/haiku/proxies.txt"}]}]
    gap()

    # A3: new prompt as a plain string; reply has two parallel Bash calls
    conv.append({"role": "user", "content": DEMO_A3_PROMPT})
    bash1, bash2 = ids.toolu(), ids.toolu()
    a3 = [("text", "I'll check the status and commit in parallel."),
          ("tool_use", bash1, "Bash", {"command": "git status --short && git log --oneline -3",
                                       "description": "Show working tree status and recent commits"}),
          ("tool_use", bash2, "Bash", {"command": DEMO_COMMIT_CMD, "description": "Commit the dashboard and proxy changes"})]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(2, 412, 34525), a3, "tool_use", 391, 57, parallel=True))
    conv.append(as_message(a3))
    conv.append({"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": bash1, "content": DEMO_STATUS_RESULT},
        {"type": "tool_result", "tool_use_id": bash2, "content": DEMO_COMMIT_ERROR, "is_error": True}]})
    gap()

    # Second session, request 2
    b2 = [("text", DEMO_B2_TEXT)]
    ex = send(session_b, main_body(DEMO_MODEL_B, session_b, conv_b, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL_B, demo_usage(1, 160, 29870), b2, "end_turn", 64))
    conv_b.append(as_message(b2))
    gap()

    # A4: both results (one is_error); the earlier string prompt is now an array
    conv[5] = {"role": "user", "content": [{"type": "text", "text": DEMO_A3_PROMPT}]}
    search_id = ids.toolu()
    a4 = [("thinking", ids.sig()), ("text", DEMO_A4_TEXT),
          ("tool_use", search_id, "ToolSearch", {"query": "select:TaskStop", "max_results": 1})]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(1, 1630, 34937), a4, "tool_use", 148, 30))
    conv += [as_message(a4), {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": search_id, "content": [{"type": "tool_reference", "tool_name": "TaskStop"}]},
        {"type": "text", "text": "Tool loaded."}]}]
    gap()

    # Second session, request 3
    conv_b.append({"role": "user", "content": DEMO_B3_PROMPT})
    ex = send(session_b, main_body(DEMO_MODEL_B, session_b, conv_b, tools))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL_B, demo_usage(2, 95, 30030), [("text", DEMO_B3_TEXT)], "end_turn", 41))
    gap()

    # A5: tool_reference result; TaskStop added to tools; final answer
    tools5 = tools + [TASKSTOP_TOOL]
    a5 = [("text", DEMO_A5_TEXT)]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools5))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(1, 520, 36567), a5, "end_turn", 203))
    conv += [as_message(a5), {"role": "user", "content": DEMO_A6_PROMPT}]
    gap()

    # A6: the stream breaks before message_stop
    a6 = [("thinking", ids.sig()), ("text", DEMO_A6_TEXT)]
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools5))
    stream(ex, demo_sse(rnd, ids, DEMO_MODEL, demo_usage(2, 260, 37087), a6, "end_turn", 40, complete=False),
           ok=False, error="upstream stream broke: IncompleteRead(312 bytes read)")
    gap()

    # The agent retries; this time the upstream is unreachable (502)
    ex = send(session_a, main_body(DEMO_MODEL, session_a, conv, tools5))
    time.sleep(0.25)
    ex.start(502, "Bad Gateway", [])
    ex.end(False, f"cannot reach {UPSTREAM}: [Errno 61] Connection refused")


def demo_loop() -> None:
    time.sleep(1.0)
    while True:
        try:
            demo_pass(random.Random())
        except Exception as e:  # the demo must never take the proxy down
            print(f"[{stamp()}] demo error: {type(e).__name__}: {e}", flush=True)
        time.sleep(10)


def main() -> None:
    global UPSTREAM
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--demo", action="store_true", help="also replay a built-in sample (no API needed)")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    print(f"dashboard:           http://localhost:{args.port}/__dashboard")
    if args.demo:
        print("demo mode: replaying the built-in sample (loops every pass, 10 s apart)")
        threading.Thread(target=demo_loop, name="demo", daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
