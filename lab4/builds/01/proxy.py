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

On the side, every exchange is recorded (last 500, in memory) and published as
Server-Sent Events on `/__events`; `/__dashboard` serves `dashboard.html` from
this file's directory. Paths starting with `/__` are never forwarded or printed.

    python3 proxy.py --port 9000
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
    python3 proxy.py --demo                # also replays a built-in sample
"""

from __future__ import annotations

import argparse
import codecs
import html
import http.client
import json
import queue
import random
import string
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
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
HERE = Path(__file__).resolve().parent


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


# --------------------------------------------------------------------------
# Exchange store and event publishing (section 4 of SPEC.md)
# --------------------------------------------------------------------------

MAX_EXCHANGES = 500
QUEUE_LIMIT = 1000
KEEPALIVE_S = 15

_lock = threading.Lock()
_store: "OrderedDict[int, dict]" = OrderedDict()  # id -> record (encoded events)
_subscribers: list[queue.Queue] = []
_next_id = 1


def now_ms() -> float:
    return time.time() * 1000.0


def shorten(name: str, value: str) -> str:
    if name.lower() in ("authorization", "x-api-key"):
        return value[:16] + "..."
    return value


def header_pairs(items) -> list[list[str]]:
    return [[str(k), shorten(str(k), str(v))] for k, v in items]


def _encode(ev: dict) -> bytes:
    return json.dumps(ev, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _fanout(data: bytes) -> None:
    """Put one encoded event on every page's queue. Caller holds _lock."""
    for q in list(_subscribers):
        if q.qsize() >= QUEUE_LIMIT:  # a stuck page: drop it, never block relaying
            _subscribers.remove(q)
            q.put_nowait(None)
        else:
            q.put_nowait(data)


def begin_exchange(method: str, path: str, headers, body: bytes) -> int:
    """Record a new request and publish its `request` event. Returns the id."""
    global _next_id
    body_json = None
    body_text = None
    if body:
        try:
            body_json = json.loads(body)
        except ValueError:
            body_json = None
        if body_json is None:
            body_text = body.decode("utf-8", "replace")
    pairs = header_pairs(headers)
    t = now_ms()
    with _lock:
        xid = _next_id
        _next_id += 1
    ev = {"type": "request", "id": xid, "t": t, "method": method, "path": path,
          "headers": pairs, "body": body_text, "body_json": body_json}
    data = _encode(ev)  # encode outside the lock: request bodies are large
    with _lock:
        _store[xid] = {"request": data, "start": None, "text": [], "last_t": t,
                       "end": None,
                       "decoder": codecs.getincrementaldecoder("utf-8")(errors="replace")}
        while len(_store) > MAX_EXCHANGES:
            _store.popitem(last=False)
        _fanout(data)
    return xid


def publish_start(xid: int, status: int, reason: str, headers) -> None:
    ev = {"type": "response_start", "id": xid, "t": now_ms(), "status": status,
          "reason": reason, "headers": header_pairs(headers)}
    data = _encode(ev)
    with _lock:
        rec = _store.get(xid)
        if rec is not None:
            rec["start"] = data
        _fanout(data)


def _publish_text(xid: int, rec: dict | None, text: str) -> None:
    """Caller holds _lock."""
    t = now_ms()
    if rec is not None:
        rec["text"].append(text)
        rec["last_t"] = t
    _fanout(_encode({"type": "response_chunk", "id": xid, "t": t, "text": text}))


def publish_chunk(xid: int, chunk: bytes) -> None:
    with _lock:
        rec = _store.get(xid)
        if rec is None:
            return
        text = rec["decoder"].decode(chunk)
        if text:
            _publish_text(xid, rec, text)


def publish_end(xid: int, ok: bool, error: str | None) -> None:
    with _lock:
        rec = _store.get(xid)
        if rec is not None:
            tail = rec["decoder"].decode(b"", final=True)
            if tail:
                _publish_text(xid, rec, tail)
        data = _encode({"type": "response_end", "id": xid, "t": now_ms(),
                        "ok": bool(ok), "error": error})
        if rec is not None:
            rec["end"] = data
        _fanout(data)


def subscribe() -> tuple[queue.Queue, list[bytes]]:
    """Register a page; return its queue and the replay of the store.

    Done under the lock, so every event is either in the replay or on the
    queue, never both and never neither.
    """
    q: queue.Queue = queue.Queue()
    snap = []
    with _lock:
        for xid, rec in _store.items():
            snap.append((xid, rec["request"], rec["start"], list(rec["text"]),
                         rec["last_t"], rec["end"]))
        _subscribers.append(q)
    replay = [_encode({"type": "reset"})]
    for xid, req, start, texts, last_t, end in snap:
        replay.append(req)
        if start is not None:
            replay.append(start)
        if texts:
            replay.append(_encode({"type": "response_chunk", "id": xid, "t": last_t,
                                   "text": "".join(texts)}))
        if end is not None:
            replay.append(end)
    return q, replay


def unsubscribe(q: queue.Queue) -> None:
    with _lock:
        if q in _subscribers:
            _subscribers.remove(q)


def error_page(code: int, message: str) -> str:
    """The body BaseHTTPRequestHandler.send_error() writes, for the dashboard."""
    _short, explain = BaseHTTPRequestHandler.responses.get(code, ("???", "???"))
    return BaseHTTPRequestHandler.error_message_format % {
        "code": code,
        "message": html.escape(message, quote=False),
        "explain": html.escape(explain, quote=False),
    }


# --------------------------------------------------------------------------
# HTTP handler
# --------------------------------------------------------------------------

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

    # ---- local routes: never forwarded, never printed --------------------

    def local(self) -> None:
        path = urlsplit(self.path).path
        self.close_connection = True
        if self.command == "GET" and path == "/__dashboard":
            self.serve_dashboard()
        elif self.command == "GET" and path == "/__events":
            self.serve_events()
        else:
            self.send_error(404, "not found")

    def serve_dashboard(self) -> None:
        try:
            page = (HERE / "dashboard.html").read_bytes()  # from disk every time
        except OSError as e:
            self.send_error(404, f"dashboard.html not found: {e}")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(page)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(page)

    def serve_events(self) -> None:
        q, replay = subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"".join(b"data: " + d + b"\n\n" for d in replay))
            self.wfile.flush()
            del replay
            while True:
                try:
                    item = q.get(timeout=KEEPALIVE_S)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                if item is None:  # dropped for being too slow
                    return
                batch = [item]
                while len(batch) < 200:
                    try:
                        more = q.get_nowait()
                    except queue.Empty:
                        break
                    if more is None:
                        return
                    batch.append(more)
                self.wfile.write(b"".join(b"data: " + d + b"\n\n" for d in batch))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            unsubscribe(q)

    # ---- the proxy itself ------------------------------------------------

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""

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

        xid = begin_exchange(self.command, self.path, self.headers.items(), body)
        ok, error = True, None
        stage = "upstream"
        try:
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
                message = f"proxy: cannot reach {UPSTREAM}: {e}"
                ok, error = False, f"cannot reach {UPSTREAM}: {e}"
                page = error_page(502, message)
                publish_start(xid, 502, message, [
                    ("Content-Type", self.error_content_type),
                    ("Content-Length", str(len(page.encode("utf-8", "replace")))),
                ])
                self.send_error(502, message)
                if self.command != "HEAD":
                    publish_chunk(xid, page.encode("utf-8", "replace"))
                self.close_connection = True
                return
            upstream.sock.settimeout(600)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
            upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
            reply = upstream.getresponse()

            # 3. Stream the reply back as it arrives, printing it on the way.
            print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
            publish_start(xid, reply.status, reply.reason, reply.getheaders())
            stage = "agent"
            self.send_response(reply.status, reply.reason)
            for name, value in reply.getheaders():
                if name.lower() not in SKIP and name.lower() != "content-encoding":
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command == "HEAD":
                self.close_connection = True
                return
            while True:
                stage = "upstream"
                chunk = reply.read1(4096)  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
                if not chunk:
                    break
                stage = "agent"
                self.wfile.write(chunk)
                self.wfile.flush()
                publish_chunk(xid, chunk)
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
            self.close_connection = True
            print(f"\n[{stamp()}] --- done", flush=True)
        except BaseException as e:
            ok = False
            who = "agent disconnected" if stage == "agent" else "upstream failed"
            error = f"{who}: {type(e).__name__}: {e}"
            raise
        finally:
            publish_end(xid, ok, error)


# --------------------------------------------------------------------------
# Demo mode (section 9): a built-in sample, published through the same
# event functions as real traffic.
# --------------------------------------------------------------------------

DEMO_MODEL = "claude-opus-5-5"
DEMO_SMALL = "claude-haiku-5-5"
DEMO_BETA = ("claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,"
             "context-management-2025-06-27,prompt-caching-scope-2026-01-05,effort-2025-11-24,"
             "structured-outputs-2025-12-15,tool-search-tool-2025-10-19,advanced-tool-use-2025-11-20,"
             "fine-grained-tool-streaming-2025-05-14")
DEMO_CWD = "/Users/dev/lab4"

_B62 = string.ascii_letters + string.digits


def _rid(rng: random.Random, prefix: str, n: int = 22) -> str:
    return prefix + "".join(rng.choice(_B62) for _ in range(n))


def _uuid(rng: random.Random) -> str:
    h = "".join(rng.choice("0123456789abcdef") for _ in range(32))
    return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-a{h[17:20]}-{h[20:]}"


def _schema(props: dict, required: list[str]) -> dict:
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {k: ({"type": v} if isinstance(v, str) else v) for k, v in props.items()},
        "required": required,
        "additionalProperties": False,
    }


_BASH_DESC = "\n\n".join([
    "Executes a given bash command and returns its output.",
    "The working directory persists between commands, but shell state does not. "
    "The shell environment is initialized from the user's profile (bash or zsh).",
    "IMPORTANT: Avoid using this tool to run `find`, `grep`, `cat`, `head`, `tail`, "
    "`sed`, `awk`, or `echo` commands, unless explicitly instructed or after you have "
    "verified that a dedicated tool cannot accomplish your task. Instead, use the "
    "appropriate dedicated tool as this will provide a much better experience for the user:",
    " - File search: Use Glob (NOT find or ls)\n - Content search: Use Grep (NOT grep or rg)\n"
    " - Read files: Use Read (NOT cat/head/tail)\n - Edit files: Use Edit (NOT sed/awk)\n"
    " - Write files: Use Write (NOT echo >/cat <<EOF)\n - Communication: Output text directly (NOT echo/printf)",
    "# Instructions\n - If your command will create new directories or files, first use this tool "
    "to run `ls` to verify the parent directory exists and is the correct location.\n"
    " - Always quote file paths that contain spaces with double quotes.\n"
    " - Try to maintain your current working directory throughout the session by using "
    "absolute paths and avoiding usage of `cd`.\n"
    " - You may specify an optional timeout in milliseconds (up to 600000ms / 10 minutes). "
    "By default, your command will timeout after 120000ms (2 minutes).\n"
    " - You can use the `run_in_background` parameter to run the command in the background.",
    "# Committing changes with git\nOnly create commits when requested by the user. If unclear, "
    "ask first. When the user asks you to create a new git commit, follow these steps carefully:\n"
    "1. Run git status, git diff and git log in parallel.\n2. Analyze all staged changes and draft "
    "a commit message.\n3. Add relevant untracked files, create the commit, and run git status "
    "after the commit completes to verify success.\n"
    "Git Safety Protocol:\n - NEVER update the git config\n - NEVER run destructive git commands "
    "(push --force, reset --hard, checkout ., clean -f, branch -D) unless the user explicitly requests these actions\n"
    " - NEVER skip hooks (--no-verify) unless the user explicitly requests it\n"
    " - CRITICAL: Always create NEW commits rather than amending.",
    "# Creating pull requests\nUse the gh command via the Bash tool for ALL GitHub-related tasks "
    "including working with issues, pull requests, checks, and releases. If given a Github URL "
    "use the gh command to get the information needed.",
] * 2)

_TOOL_SPECS = [
    ("Agent", "Launch a new agent to handle complex, multi-step tasks autonomously.\n\n"
              "Each agent type has specific capabilities and tools available to it.",
     {"description": "string", "prompt": "string", "subagent_type": "string"}, ["description", "prompt"]),
    ("AskUserQuestion", "Ask the user a multiple-choice question when you need a decision.",
     {"questions": {"type": "array", "items": {"type": "object"}}}, ["questions"]),
    ("Bash", _BASH_DESC,
     {"command": "string", "description": "string", "timeout": "number", "run_in_background": "boolean"},
     ["command"]),
    ("BashOutput", "Retrieves output from a running or completed background bash shell.",
     {"bash_id": "string", "filter": "string"}, ["bash_id"]),
    ("Edit", "Performs exact string replacements in files.\n\nYou must use your Read tool at least once "
             "in the conversation before editing.",
     {"file_path": "string", "old_string": "string", "new_string": "string", "replace_all": "boolean"},
     ["file_path", "old_string", "new_string"]),
    ("EnterPlanMode", "Use this tool to start planning before writing code for a non-trivial task.",
     {}, []),
    ("ExitPlanMode", "Use this tool when you are in plan mode and have finished presenting your plan.",
     {"plan": "string"}, ["plan"]),
    ("Glob", "Fast file pattern matching tool that works with any codebase size.\n\n"
             "Supports glob patterns like \"**/*.js\" or \"src/**/*.ts\".",
     {"pattern": "string", "path": "string"}, ["pattern"]),
    ("Grep", "A powerful search tool built on ripgrep.\n\nSupports full regex syntax.",
     {"pattern": "string", "path": "string", "glob": "string",
      "output_mode": {"type": "string", "enum": ["content", "files_with_matches", "count"]}}, ["pattern"]),
    ("KillShell", "Kills a running background bash shell by its ID.", {"shell_id": "string"}, ["shell_id"]),
    ("NotebookEdit", "Completely replaces the contents of a specific cell in a Jupyter notebook.",
     {"notebook_path": "string", "cell_id": "string", "new_source": "string"}, ["notebook_path", "new_source"]),
    ("Read", "Reads a file from the local filesystem. You can access any file directly by using this tool.\n\n"
             "Usage:\n- The file_path parameter must be an absolute path, not a relative path\n"
             "- By default, it reads up to 2000 lines starting from the beginning of the file",
     {"file_path": "string", "offset": "number", "limit": "number"}, ["file_path"]),
    ("Skill", "Execute a skill within the main conversation.", {"skill": "string", "args": "string"}, ["skill"]),
    ("SlashCommand", "Execute a slash command within the main conversation.", {"command": "string"}, ["command"]),
    ("TodoWrite", "Use this tool to create and manage a structured task list for your current coding session.",
     {"todos": {"type": "array", "items": {"type": "object"}}}, ["todos"]),
    ("ToolSearch", "Fetches full schema definitions for deferred tools so they can be called.\n\n"
                   "Use \"select:<name>\" to load a tool by name.",
     {"query": "string", "max_results": "number"}, ["query"]),
    ("WebFetch", "Fetches content from a specified URL and processes it using an AI model.",
     {"url": "string", "prompt": "string"}, ["url", "prompt"]),
    ("WebSearch", "Allows Claude to search the web and use the results to inform responses.",
     {"query": "string", "allowed_domains": {"type": "array", "items": {"type": "string"}}}, ["query"]),
    ("Write", "Writes a file to the local filesystem. This tool will overwrite the existing file.",
     {"file_path": "string", "content": "string"}, ["file_path", "content"]),
]


def _demo_tools() -> list[dict]:
    tools = []
    for name, desc, props, req in _TOOL_SPECS:
        t = {"name": name, "description": desc, "input_schema": _schema(props, req)}
        if name == "WebSearch":
            t["defer_loading"] = True
        tools.append(t)
    tools.append({
        "name": "DeferredToolPlaceholder",
        "description": "Placeholder for tools whose full definitions are loaded on demand with ToolSearch.",
        "input_schema": {"type": "object", "properties": {}},
    })
    return tools  # 20 tools


_TASKSTOP = {
    "name": "TaskStop",
    "description": "Stops a running background task by its ID.\n\nUse it when a watcher or server "
                   "started earlier in the session is no longer needed.",
    "input_schema": _schema({"task_id": "string"}, ["task_id"]),
}

_MAIN_PROMPT = "\n\n".join(
    [
        "You are an interactive CLI tool that helps users with software engineering tasks. "
        "Use the instructions below and the tools available to you to assist the user.",
        "IMPORTANT: Assist with authorized security testing, defensive security, CTF challenges, "
        "and educational contexts. Refuse requests for destructive techniques.",
    ]
    + [
        f"# {title}\n" + "\n".join(f"- {line}" for line in lines)
        for title, lines in [
            ("Tone and style", [
                "Your output will be displayed on a command line interface. Keep responses short and concise.",
                "Use Github-flavored markdown for formatting; it is rendered in a monospace font.",
                "Only use emojis if the user explicitly requests it.",
                "Output text to communicate with the user; never use tools such as Bash or code comments "
                "as a means to communicate.",
            ]),
            ("Professional objectivity", [
                "Prioritize technical accuracy and truthfulness over validating the user's beliefs.",
                "Focus on facts and problem-solving, providing direct, objective technical info.",
                "When uncertain, investigate to find the truth first rather than confirming assumptions.",
            ]),
            ("Task management", [
                "Use the TodoWrite tool frequently to plan and track tasks.",
                "Mark todos as completed as soon as you are done with a task. Do not batch them.",
                "Break larger, complex tasks into smaller steps.",
            ]),
            ("Doing tasks", [
                "Use the available search tools to understand the codebase and the user's query.",
                "Implement the solution using all tools available to you.",
                "Verify the solution if possible with tests. Never assume a specific test framework.",
                "When you have completed a task, run the lint and typecheck commands if they were provided.",
                "NEVER commit changes unless the user explicitly asks you to.",
            ]),
            ("Tool usage policy", [
                "When doing file search, prefer to use the Agent tool in order to reduce context usage.",
                "You can call multiple tools in a single response. If there are no dependencies between "
                "them, make all independent tool calls in parallel.",
                "Use specialized tools instead of bash commands when possible.",
                "Some tools are deferred: their definitions are loaded with ToolSearch before first use.",
            ]),
            ("Code references", [
                "When referencing specific functions or pieces of code include the pattern "
                "`file_path:line_number` so the user can navigate to the source code location.",
            ]),
        ]
    ]
    + [
        "# Environment\n<env>\nWorking directory: " + DEMO_CWD + "\nIs directory a git repo: Yes\n"
        "Platform: darwin\nOS Version: Darwin 25.6.0\nToday's date: 2026-11-04\n</env>",
        "You are powered by the model named Opus 5.5. The exact model ID is claude-opus-5-5.",
    ]
    + [
        "# Notes on long-running work\n" + "\n".join(
            f"- Guideline {i}: keep the user informed of progress, prefer small verifiable steps, "
            f"and re-read files before editing them when they may have changed." for i in range(1, 41))
    ]
)

_SYSTEM_MSG_TEXT = "\n".join(
    ["<system-reminder>", "As you answer the user's questions, you can use the following context:",
     "# claudeMd", "Codebase and user instructions are shown below. Be sure to adhere to these instructions.",
     f"Contents of {DEMO_CWD}/CLAUDE.md (project instructions, checked into the codebase):", ""]
    + [f"## Rule {i}\nKeep `proxy.py` standard-library only. Forwarding behaviour must never change; "
       f"the dashboard is read-only. Write tests for every parser edge case (rule {i})." for i in range(1, 61)]
    + ["", "# currentDate", "Today's date is 2026-11-04.", "</system-reminder>"]
)


def _tool_addition(name: str, desc: str, props: dict, req: list[str]) -> dict:
    return {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": {
        "name": name, "description": desc, "input_schema": _schema(props, req)}}}


def _read_result() -> str:
    try:
        lines = Path(__file__).read_text(encoding="utf-8").splitlines()[:130]
    except OSError:
        lines = [f"# line {i}" for i in range(1, 131)]
    return "\n".join(f"{i:>6}\t{line}" for i, line in enumerate(lines, 1))


def _usage(inp: int, cw: int, cr: int, out: int) -> dict:
    return {"input_tokens": inp, "cache_creation_input_tokens": cw, "cache_read_input_tokens": cr,
            "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": cw},
            "output_tokens": out, "service_tier": "standard"}


def _pieces(s: str, rng: random.Random, lo: int, hi: int) -> list[str]:
    out, i = [], 0
    while i < len(s):
        n = rng.randint(lo, hi)
        out.append(s[i:i + n])
        i += n
    return out


def _sse(events: list[dict], rng: random.Random) -> str:
    parts = []
    for ev in events:
        data = json.dumps(ev, ensure_ascii=False)
        pad = " " * rng.randint(1, 4) if ev["type"] == "ping" or rng.random() < 0.2 else ""
        parts.append(f"event: {ev['type']}\ndata: {data}{pad}\n\n")
    return "".join(parts)


def _reply_sse(rng: random.Random, model: str, usage: dict, blocks: list[dict], stop_reason: str,
               *, interleave: bool = False, broken: bool = False, thinking_tokens: int = 0) -> str:
    start_usage = dict(usage, output_tokens=min(usage["output_tokens"], 3))
    head = [{"type": "message_start", "message": {
        "id": _rid(rng, "msg_01"), "type": "message", "role": "assistant", "model": model,
        "content": [], "stop_reason": None, "stop_sequence": None, "usage": start_usage}}]
    seq: list[dict] = []
    tool_lists: list[list[dict]] = []
    for i, b in enumerate(blocks):
        evs: list[dict] = []
        if b["type"] == "text":
            evs.append({"type": "content_block_start", "index": i, "content_block": {"type": "text", "text": ""}})
            for p in _pieces(b["text"], rng, 6, 22):
                evs.append({"type": "content_block_delta", "index": i, "delta": {"type": "text_delta", "text": p}})
        elif b["type"] == "thinking":
            evs.append({"type": "content_block_start", "index": i,
                        "content_block": {"type": "thinking", "thinking": "", "signature": ""}})
            evs.append({"type": "content_block_delta", "index": i,
                        "delta": {"type": "signature_delta", "signature": b["signature"]}})
        elif b["type"] == "tool_use":
            evs.append({"type": "content_block_start", "index": i, "content_block": {
                "type": "tool_use", "id": b["id"], "name": b["name"], "input": {}, "caller": {"type": "direct"}}})
            evs.append({"type": "content_block_delta", "index": i,
                        "delta": {"type": "input_json_delta", "partial_json": ""}})
            for p in _pieces(json.dumps(b["input"], ensure_ascii=False), rng, 5, 14):
                evs.append({"type": "content_block_delta", "index": i,
                            "delta": {"type": "input_json_delta", "partial_json": p}})
        evs.append({"type": "content_block_stop", "index": i})
        if interleave and b["type"] == "tool_use":
            tool_lists.append(evs)
        else:
            seq.extend(evs)
    # More than one block open at a time: round-robin the tool blocks' events.
    while any(tool_lists):
        for lst in tool_lists:
            if lst:
                seq.append(lst.pop(0))
    body = head + seq[:1] + [{"type": "ping"}] + seq[1:]
    if broken:
        return _sse(body[: max(3, len(body) * 2 // 3)], rng)  # no stop, no message_stop
    final_usage = dict(usage, output_tokens_details={"thinking_tokens": thinking_tokens})
    body += [
        {"type": "message_delta", "delta": {"stop_reason": stop_reason, "stop_sequence": None},
         "usage": final_usage},
        {"type": "message_stop"},
    ]
    return _sse(body, rng)


def _split_bytes(data: bytes, rng: random.Random, lo: int = 40, hi: int = 190) -> list[bytes]:
    """Random cuts: some land mid-line, some mid UTF-8 character."""
    out, i = [], 0
    while i < len(data):
        n = rng.randint(lo, hi)
        out.append(data[i:i + n])
        i += n
    return out


class DemoPass:
    """One run through the sample. New ids (session, toolu_, msg_, req_) every pass."""

    def __init__(self, n: int):
        self.rng = random.Random(1000 + n)
        rng = self.rng
        self.session = _uuid(rng)
        self.token = "Bearer sk-ant-oat01-" + "".join(rng.choice(_B62) for _ in range(60))
        self.user_id = json.dumps({"device_id": "".join(rng.choice("0123456789abcdef") for _ in range(64)),
                                   "account_uuid": _uuid(rng), "session_id": self.session})
        self.org = _uuid(rng)

    # ---- request/response parts ------------------------------------------

    def headers(self, body: bytes, ua: str = "claude-cli/2.1.318 (external, cli)") -> list[tuple[str, str]]:
        return [
            ("Host", "localhost:8041"), ("Accept", "application/json"),
            ("Authorization", self.token), ("Content-Type", "application/json"),
            ("User-Agent", ua), ("X-Claude-Code-Session-Id", self.session),
            ("X-Stainless-Lang", "js"), ("X-Stainless-Retry-Count", "0"),
            ("anthropic-beta", DEMO_BETA), ("anthropic-version", "2023-06-01"),
            ("anthropic-dangerous-direct-browser-access", "true"), ("x-app", "cli"),
            ("Content-Length", str(len(body))),
        ]

    def sse_headers(self) -> list[tuple[str, str]]:
        return [
            ("Date", time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())),
            ("Content-Type", "text/event-stream; charset=utf-8"), ("Cache-Control", "no-cache"),
            ("request-id", _rid(self.rng, "req_011C", 20)), ("anthropic-organization-id", self.org),
            ("x-envoy-upstream-service-time", str(self.rng.randint(300, 900))), ("server", "cloudflare"),
        ]

    def system(self) -> list[dict]:
        return [
            {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.318.a7f; cc_entrypoint=cli;"},
            {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude.",
             "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            {"type": "text", "text": _MAIN_PROMPT, "cache_control": {"type": "ephemeral", "ttl": "1h"}},
        ]

    def main_body(self, messages: list[dict], tools: list[dict]) -> dict:
        return {
            "model": DEMO_MODEL, "messages": messages, "system": self.system(), "tools": tools,
            "metadata": {"user_id": self.user_id}, "max_tokens": 32000,
            "thinking": {"type": "adaptive", "display": "updates"},
            "context_management": {"edits": [{"type": "clear_thinking_20251015", "keep": "all"}]},
            "safeguards": {"level": "default"}, "output_config": {"effort": "medium"}, "stream": True,
        }

    # ---- the conversation ------------------------------------------------

    def build(self) -> None:
        rng = self.rng
        self.read_id = _rid(rng, "toolu_01")
        self.bash1_id = _rid(rng, "toolu_01")
        self.bash2_id = _rid(rng, "toolu_01")
        self.search_id = _rid(rng, "toolu_01")
        sig = "".join(rng.choice(_B62 + "+/") for _ in range(420)) + "=="
        self.prompt1 = "Read proxy.py and tell me what it does"
        self.prompt2 = "Are there HTML fixtures in tests/ that could break the dashboard? Also run the tests."
        self.prompt3 = "Yes, stop the background test watcher too."
        self.r1 = [{"type": "thinking", "signature": sig},
                   {"type": "tool_use", "id": self.read_id, "name": "Read",
                    "input": {"file_path": f"{DEMO_CWD}/proxy.py"}}]
        self.r2 = [{"type": "text", "text": (
            "`proxy.py` is a small reverse proxy for watching an agent's API traffic.\n\n"
            "- It listens on `127.0.0.1:8041` and forwards every request to `https://api.anthropic.com` "
            "unchanged — headers intact, except the hop-by-hop ones in `SKIP`.\n"
            "- Each request is printed to the terminal: method, path, headers (with `Authorization` "
            "shortened to 16 characters) and the pretty-printed JSON body.\n"
            "- Replies stream back chunk by chunk with `read1(4096)` → the agent sees tokens as they arrive.\n"
            "- `--upstream` points it at another API (e.g. Codex) and `--port` changes the port.\n\n"
            "It uses only the standard library, so there is nothing to install.")}]
        self.r3 = [{"type": "text", "text": "I'll search the fixtures and run the test suite in parallel."},
                   {"type": "tool_use", "id": self.bash1_id, "name": "Bash",
                    "input": {"command": "grep -rn \"<script>\" tests/fixtures",
                              "description": "Find fixtures that contain script tags"}},
                   {"type": "tool_use", "id": self.bash2_id, "name": "Bash",
                    "input": {"command": "python3 -m pytest -q tests/", "description": "Run the test suite"}}]
        self.r4 = [{"type": "text", "text": (
            "One fixture contains inline HTML with a script tag (`tests/fixtures/evil.html`), and one test "
            "fails: `test_relay_streams` expects two chunks but gets one. A watcher from earlier may still be "
            "running; let me load the tool for stopping background tasks.")},
                   {"type": "tool_use", "id": self.search_id, "name": "ToolSearch",
                    "input": {"query": "select:TaskStop", "max_results": 1}}]
        self.r5 = [{"type": "text", "text": (
            "Summary:\n\n"
            "1. `tests/fixtures/evil.html` contains `<b>hi</b><script>alert(1)</script>` — the dashboard must "
            "show it as text, never as markup.\n"
            "2. `test_relay_streams` fails because the fake upstream sends both chunks in one write, so "
            "`read1()` returns them together. The proxy is fine; the test should send them separately.\n\n"
            "`TaskStop` is now loaded, so I can stop the background watcher if you want.")}]
        self.r6 = [{"type": "text", "text": (
            "Stopping the watcher now. First I'll list the running background tasks so I pick the right "
            "one, then call `TaskStop` with its id and confirm that it exited cleanly.")}]

    def messages(self, upto: int) -> list[dict]:
        """The conversation as sent in main request `upto` (1-based)."""
        reminder1 = ("<system-reminder>\nThis is a reminder that your todo list is currently empty. "
                     "DO NOT mention this to the user explicitly because they are already aware.\n</system-reminder>")
        reminder2 = ("<system-reminder>\nThe user opened the file " + DEMO_CWD + "/proxy.py in the IDE. "
                     "This may or may not be related to the current task.\n</system-reminder>")
        m: list[dict] = [
            {"role": "user", "content": [{"type": "text", "text": reminder1},
                                         {"type": "text", "text": reminder2},
                                         {"type": "text", "text": self.prompt1}]},
            {"role": "system", "content": [
                {"type": "text", "text": _SYSTEM_MSG_TEXT},
                _tool_addition("mcp__ide__getDiagnostics", "Get language diagnostics from the IDE.",
                               {"uri": "string"}, []),
                _tool_addition("mcp__ide__executeCode", "Execute Python code in the Jupyter kernel of the "
                               "current notebook file.", {"code": "string"}, ["code"]),
            ]},
        ]
        if upto >= 2:
            m.append({"role": "assistant", "content": [
                {"type": "thinking", "thinking": "", "signature": self.r1[0]["signature"]},
                {"type": "tool_use", "id": self.read_id, "name": "Read", "input": self.r1[1]["input"],
                 "caller": {"type": "direct"}}]})
            m.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": self.read_id, "content": _read_result()}]})
        if upto >= 3:
            m.append({"role": "assistant", "content": [dict(self.r2[0])]})
            # A plain string here; the next request resends it as a one-element array.
            m.append({"role": "user", "content": self.prompt2 if upto == 3
                      else [{"type": "text", "text": self.prompt2}]})
        if upto >= 4:
            m.append({"role": "assistant", "content": [
                {"type": "text", "text": self.r3[0]["text"]},
                {"type": "tool_use", "id": self.bash1_id, "name": "Bash", "input": self.r3[1]["input"],
                 "caller": {"type": "direct"}},
                {"type": "tool_use", "id": self.bash2_id, "name": "Bash", "input": self.r3[2]["input"],
                 "caller": {"type": "direct"}}]})
            m.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": self.bash1_id, "content":
                    "tests/fixtures/evil.html:3:<b>hi</b><script>alert(1)</script>\n"
                    "tests/fixtures/evil.html:7:<img src=x onerror=\"alert(2)\">"},
                {"type": "tool_result", "tool_use_id": self.bash2_id, "is_error": True, "content":
                    "Exit code 1\n..F\n=================================== FAILURES ===================================\n"
                    "______________________________ test_relay_streams ______________________________\n\n"
                    "    def test_relay_streams():\n        chunks = run_fake_upstream([b\"a\", b\"b\"])\n"
                    ">       assert chunks == [b\"a\", b\"b\"]\n"
                    "E       AssertionError: assert [b'ab'] == [b'a', b'b']\n\n"
                    "tests/test_proxy.py:42: AssertionError\n"
                    "=========================== short test summary info ============================\n"
                    "FAILED tests/test_proxy.py::test_relay_streams - AssertionError\n1 failed, 2 passed in 0.31s"},
            ]})
        if upto >= 5:
            m.append({"role": "assistant", "content": [
                {"type": "text", "text": self.r4[0]["text"]},
                {"type": "tool_use", "id": self.search_id, "name": "ToolSearch", "input": self.r4[1]["input"],
                 "caller": {"type": "direct"}}]})
            m.append({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": self.search_id,
                 "content": [{"type": "tool_reference", "tool_name": "TaskStop"}]},
                {"type": "text", "text": "Tool loaded."}]})
        if upto >= 6:
            m.append({"role": "assistant", "content": [{"type": "text", "text": self.r5[0]["text"]}]})
            m.append({"role": "user", "content": self.prompt3})
        # cache_control moves: it sits on the last block of the newest array message.
        for msg in reversed(m):
            if isinstance(msg["content"], list) and msg["role"] != "system":
                msg["content"][-1]["cache_control"] = {"type": "ephemeral"}
                break
        return m

    # ---- playback --------------------------------------------------------

    def play(self, method: str, path: str, headers: list, body: bytes, status: int, reason: str,
             resp_headers: list, reply: bytes, *, ok: bool = True, error: str | None = None,
             ttfb: float = 0.35, chunk_range: tuple[int, int] = (40, 190)) -> None:
        xid = begin_exchange(method, path, headers, body)
        ok_final, err_final = ok, error
        try:
            time.sleep(ttfb)
            publish_start(xid, status, reason, resp_headers)
            for chunk in _split_bytes(reply, self.rng, *chunk_range) if reply else []:
                time.sleep(self.rng.uniform(0.04, 0.08))
                publish_chunk(xid, chunk)
        except BaseException as e:  # never leave a row pending
            ok_final, err_final = False, f"demo: {e}"
            raise
        finally:
            publish_end(xid, ok_final, err_final)

    def play_main(self, n: int, tools: list[dict], reply: bytes, *, broken: bool = False) -> None:
        body = json.dumps(self.main_body(self.messages(n), tools), ensure_ascii=False).encode("utf-8")
        self.play("POST", "/v1/messages?beta=true", self.headers(body), body, 200, "OK",
                  self.sse_headers(), reply, ok=not broken,
                  error="upstream failed: IncompleteRead: stream ended before message_stop" if broken else None,
                  ttfb=self.rng.uniform(0.4, 0.8))

    def run(self) -> None:
        rng = self.rng
        self.build()
        tools = _demo_tools()

        # 1. Connectivity check.
        self.play("HEAD", "/api/hello", [("Host", "localhost:8041"), ("User-Agent", "Bun/1.3.1"),
                                          ("Accept", "*/*")],
                  b"", 200, "OK", [("Content-Type", "application/json"), ("Content-Length", "27"),
                                   ("server", "cloudflare")], b"", ttfb=0.15)
        time.sleep(1.5)

        # 2. Quota probe and title call at the same time; their chunks interleave.
        title_body = json.dumps({
            "model": DEMO_SMALL, "max_tokens": 512,
            "messages": [{"role": "user", "content": self.prompt1}],
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.318.a7f; cc_entrypoint=cli;"},
                {"type": "text", "text": "Generate a concise, sentence-case session name (3-6 words) that "
                 "captures the main topic of the user's first message. Return JSON with a single "
                 "\"title\" field."}],
            "tools": [], "metadata": {"user_id": self.user_id}, "temperature": 1,
            "output_config": {"effort": "medium", "format": {"type": "json_schema", "schema": {
                "type": "object", "properties": {"title": {"type": "string"}},
                "required": ["title"], "additionalProperties": False}}},
            "stream": True,
        }).encode("utf-8")
        title_reply = _reply_sse(rng, DEMO_SMALL, _usage(412, 0, 0, 12),
                                 [{"type": "text", "text": "{\"title\":\"Proxy summary\"}"}], "end_turn").encode()
        quota_body = json.dumps({"model": DEMO_SMALL, "max_tokens": 1,
                                 "messages": [{"role": "user", "content": "quota"}],
                                 "metadata": {"user_id": self.user_id}}).encode("utf-8")
        req_id = _rid(rng, "req_011C", 20)
        quota_reply = json.dumps({"type": "error", "error": {"type": "rate_limit_error", "message": "Error"},
                                  "request_id": req_id}).encode()
        t_title = threading.Thread(target=self.play, daemon=True, args=(
            "POST", "/v1/messages?beta=true", self.headers(title_body), title_body, 200, "OK",
            self.sse_headers(), title_reply), kwargs={"ttfb": 0.3, "chunk_range": (30, 70)})

        def quota() -> None:
            time.sleep(0.1)
            self.play("POST", "/v1/messages?beta=true", self.headers(quota_body), quota_body, 429,
                      "Too Many Requests", [("Content-Type", "application/json"), ("retry-after", "12"),
                                            ("request-id", req_id), ("x-should-retry", "true")],
                      quota_reply, ttfb=0.35, chunk_range=(50, 70))

        t_quota = threading.Thread(target=quota, daemon=True)
        t_title.start()
        t_quota.start()
        t_title.join()
        t_quota.join()
        time.sleep(1.5)

        # 3. The main thread.
        m = DEMO_MODEL
        replies = [
            _reply_sse(rng, m, _usage(2, 30315, 0, 87), self.r1, "tool_use", thinking_tokens=41),
            _reply_sse(rng, m, _usage(1, 3120, 30315, 412), self.r2, "end_turn"),
            _reply_sse(rng, m, _usage(1, 540, 33435, 168), self.r3, "tool_use", interleave=True),
            _reply_sse(rng, m, _usage(1, 860, 33975, 121), self.r4, "tool_use"),
            _reply_sse(rng, m, _usage(1, 420, 34835, 233), self.r5, "end_turn"),
            _reply_sse(rng, m, _usage(1, 310, 35255, 40), self.r6, "end_turn", broken=True),
        ]
        for n, reply in enumerate(replies, 1):
            self.play_main(n, tools if n < 5 else tools + [_TASKSTOP], reply.encode("utf-8"), broken=(n == 6))
            time.sleep(1.5)

        # 4. The same request again, to an upstream that cannot be reached.
        body = json.dumps(self.main_body(self.messages(6), tools + [_TASKSTOP]), ensure_ascii=False).encode()
        err = "[Errno 61] Connection refused"
        message = f"proxy: cannot reach {UPSTREAM}: {err}"
        page = error_page(502, message).encode("utf-8")
        self.play("POST", "/v1/messages?beta=true", self.headers(body), body, 502, message,
                  [("Content-Type", BaseHTTPRequestHandler.error_content_type),
                   ("Content-Length", str(len(page)))], page,
                  ok=False, error=f"cannot reach {UPSTREAM}: {err}", ttfb=0.05, chunk_range=(4096, 4096))


def demo_loop() -> None:
    time.sleep(1.0)
    n = 0
    while True:
        n += 1
        try:
            DemoPass(n).run()
        except Exception as e:  # keep the proxy alive whatever happens
            print(f"[{stamp()}] demo pass {n} failed: {e}", file=sys.stderr, flush=True)
        time.sleep(10)


def main() -> None:
    global UPSTREAM
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--demo", action="store_true", help="also replay a built-in sample on the dashboard")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    print(f"dashboard:           http://localhost:{args.port}/__dashboard")
    if args.demo:
        print("demo mode: replaying the built-in sample (loops every pass)")
        threading.Thread(target=demo_loop, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
