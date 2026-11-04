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

On the side, every request and its reply are tagged with an exchange id and
published as structured events on `/__events` (Server-Sent Events), which the
page at `/__dashboard` renders. All interpretation happens in the page.

    python3 proxy.py --port 9000
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
    python3 proxy.py --demo                # also replay a built-in sample, no API needed
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
# Exchange store and event bus (section 4.2 / 4.3 of SPEC.md)
# ---------------------------------------------------------------------------

STORE_MAX = 500          # exchanges kept in memory, oldest dropped first
QUEUE_MAX = 1000         # a page whose queue reaches this many items is dropped
KEEPALIVE_S = 15

_lock = threading.Lock()
_store: "OrderedDict[int, dict]" = OrderedDict()
_subs: list["_Subscriber"] = []
_next_id = 0


class _Subscriber:
    def __init__(self) -> None:
        self.q: queue.Queue = queue.Queue()
        self.dropped = False


def now_ms() -> float:
    return time.time() * 1000.0


def shorten(name: str, value: str) -> str:
    if name.lower() in ("authorization", "x-api-key"):
        return value[:16] + "..."
    return value


def clean_headers(items) -> list:
    return [[str(k), shorten(str(k), str(v))] for k, v in items]


def _publish_locked(ev: dict) -> None:
    """Record `ev` in the store and hand it to every connected page. Caller holds _lock."""
    kind = ev["type"]
    if kind == "request":
        _store[ev["id"]] = {"request": ev, "start": None, "text": [], "end": None}
        while len(_store) > STORE_MAX:
            _store.popitem(last=False)
    else:
        ex = _store.get(ev["id"])
        if ex is not None:
            if kind == "response_start":
                ex["start"] = ev
            elif kind == "response_chunk":
                ex["text"].append(ev["text"])
            elif kind == "response_end":
                ex["end"] = ev
    for sub in list(_subs):
        if sub.q.qsize() >= QUEUE_MAX:  # slow or stuck page: drop it, never block relaying
            sub.dropped = True
            _subs.remove(sub)
            sub.q.put(None)
        else:
            sub.q.put(ev)


def publish(ev: dict) -> None:
    with _lock:
        _publish_locked(ev)


def _replay_locked() -> list:
    out = []
    for ex in _store.values():
        out.append(ex["request"])
        if ex["start"] is not None:
            out.append(ex["start"])
        text = "".join(ex["text"])
        if text:
            out.append({"type": "response_chunk", "id": ex["request"]["id"],
                        "t": ex["start"]["t"] if ex["start"] else ex["request"]["t"], "text": text})
        if ex["end"] is not None:
            out.append(ex["end"])
    return out


class Exchange:
    """One request/reply pair. Real traffic and the demo both go through this."""

    def __init__(self, method: str, path: str, headers, body: bytes) -> None:
        global _next_id
        self.decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.ended = False
        body_json = None
        parsed = False
        if body:
            try:
                body_json = json.loads(body)
                parsed = True
            except ValueError:
                pass
        ev = {
            "type": "request", "method": method, "path": path,
            "headers": clean_headers(headers),
            "body": None if parsed else body.decode("utf-8", "replace"),
            "body_json": body_json,
        }
        with _lock:
            _next_id += 1
            self.id = _next_id
            ev["id"] = self.id
            ev["t"] = now_ms()
            _publish_locked(ev)

    def start(self, status: int, reason: str, headers) -> None:
        publish({"type": "response_start", "id": self.id, "t": now_ms(), "status": status,
                 "reason": reason, "headers": clean_headers(headers)})

    def chunk(self, data: bytes) -> None:
        text = self.decoder.decode(data)
        if text:
            publish({"type": "response_chunk", "id": self.id, "t": now_ms(), "text": text})

    def text(self, text: str) -> None:
        self.chunk(text.encode("utf-8"))

    def end(self, ok: bool, error: str | None = None) -> None:
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

    # -- local routes: never forwarded, never printed --------------------------

    def local(self) -> None:
        route = self.path.split("?", 1)[0]
        self.close_connection = True
        if self.command == "GET" and route == "/__dashboard":
            self.serve_dashboard()
        elif self.command == "GET" and route == "/__events":
            self.serve_events()
        else:
            self.send_error(404, "unknown local route")

    def serve_dashboard(self) -> None:
        try:
            with open(os.path.join(HERE, "dashboard.html"), "rb") as f:  # read on every request
                page = f.read()
        except OSError as e:
            self.send_error(500, f"cannot read dashboard.html: {e}")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(page)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(page)

    def _send_event(self, ev: dict) -> None:
        self.wfile.write(b"data: " + json.dumps(ev, ensure_ascii=False).encode("utf-8") + b"\n\n")

    def serve_events(self) -> None:
        sub = _Subscriber()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            with _lock:  # snapshot + subscribe atomically: no event lost or doubled
                snapshot = _replay_locked()
                _subs.append(sub)
            self._send_event({"type": "reset"})
            for ev in snapshot:
                self._send_event(ev)
            self.wfile.flush()
            while not sub.dropped:
                try:
                    ev = sub.q.get(timeout=KEEPALIVE_S)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                if ev is None:
                    break
                self._send_event(ev)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            with _lock:
                if sub in _subs:
                    _subs.remove(sub)

    # -- the proxy -------------------------------------------------------------

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

        ex = Exchange(self.command, self.path, self.headers.items(), body)
        ok, error = True, None
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
                ok, error = False, f"cannot reach {UPSTREAM}: {e}"
                ex.start(502, "Bad Gateway", [("Content-Type", "text/html;charset=utf-8"), ("Connection", "close")])
                ex.text(f"proxy: cannot reach {UPSTREAM}: {e}")
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
                self.wfile.write(chunk)
                self.wfile.flush()
                ex.chunk(chunk)
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
            self.close_connection = True
            print(f"\n[{stamp()}] --- done", flush=True)
        except (BrokenPipeError, ConnectionResetError) as e:
            ok, error = False, f"agent disconnected: {e}"
            raise
        except BaseException as e:
            ok, error = False, f"{type(e).__name__}: {e}"
            raise
        finally:
            ex.end(ok, error)  # always: a row must never stay "pending"


# ---------------------------------------------------------------------------
# Demo mode (section 9): a built-in sample published through the same
# Exchange class as real traffic. No network access needed.
# ---------------------------------------------------------------------------

MAIN_MODEL = "claude-opus-5-5"
SMALL_MODEL = "claude-haiku-5-5"
BETA = ("claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,"
        "context-management-2025-06-27,prompt-caching-scope-2026-01-05,"
        "fine-grained-tool-streaming-2025-05-14,effort-2025-11-24,tool-search-tool-2025-10-19")
_B62 = string.ascii_letters + string.digits


def _rid(rng: random.Random, prefix: str, n: int = 24) -> str:
    return prefix + "".join(rng.choice(_B62) for _ in range(n))


def _schema(props: dict, required: list) -> dict:
    return {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object",
            "properties": props, "required": required, "additionalProperties": False}


def _s(desc: str) -> dict:
    return {"type": "string", "description": desc}


def _long_desc(first: str, n: int) -> str:
    notes = "\n".join(f"- Usage note {i + 1}: follow the conventions of the surrounding code, prefer the "
                      f"dedicated tool over shell commands, and keep output concise." for i in range(n))
    return f"{first}\n\nUsage:\n{notes}"


def _demo_tools() -> list:
    t = [
        ("Agent", "Launch a new agent to handle complex, multi-step tasks autonomously.", 30,
         _schema({"description": _s("A short task description"), "prompt": _s("The task"),
                  "subagent_type": _s("Agent type")}, ["description", "prompt"])),
        ("Bash", "Executes a given bash command and returns its output.", 40,
         _schema({"command": _s("The command to execute"), "description": _s("What it does"),
                  "timeout": {"type": "number"}, "run_in_background": {"type": "boolean"}}, ["command"])),
        ("Glob", "Fast file pattern matching tool that works with any codebase size.", 8,
         _schema({"pattern": _s("Glob pattern"), "path": _s("Directory")}, ["pattern"])),
        ("Grep", "A powerful search tool built on ripgrep.", 20,
         _schema({"pattern": _s("Regex"), "path": _s("Where"), "glob": _s("Filter"),
                  "output_mode": {"type": "string", "enum": ["content", "files_with_matches", "count"]}},
                 ["pattern"])),
        ("ExitPlanMode", "Prompts the user to exit plan mode and start coding.", 6,
         _schema({"plan": _s("The plan")}, ["plan"])),
        ("Read", "Reads a file from the local filesystem.", 14,
         _schema({"file_path": _s("Absolute path"), "offset": {"type": "number"}, "limit": {"type": "number"}},
                 ["file_path"])),
        ("Edit", "Performs exact string replacements in files.", 12,
         _schema({"file_path": _s("Absolute path"), "old_string": _s("Text to replace"),
                  "new_string": _s("Replacement"), "replace_all": {"type": "boolean", "default": False}},
                 ["file_path", "old_string", "new_string"])),
        ("Write", "Writes a file to the local filesystem.", 8,
         _schema({"file_path": _s("Absolute path"), "content": _s("File content")}, ["file_path", "content"])),
        ("NotebookEdit", "Completely replaces the contents of a specific cell in a Jupyter notebook.", 6,
         _schema({"notebook_path": _s("Path"), "new_source": _s("Cell source")}, ["notebook_path", "new_source"])),
        ("WebFetch", "Fetches content from a specified URL and processes it.", 10,
         _schema({"url": _s("URL"), "prompt": _s("What to extract")}, ["url", "prompt"])),
        ("TodoWrite", "Create and manage a structured task list for the current session.", 25,
         _schema({"todos": {"type": "array", "items": {"type": "object"}}}, ["todos"])),
        ("WebSearch", "Search the web and use the results to inform responses.", 8,
         _schema({"query": _s("Search query")}, ["query"])),
        ("BashOutput", "Retrieves output from a running or completed background bash shell.", 5,
         _schema({"bash_id": _s("Shell id")}, ["bash_id"])),
        ("KillShell", "Kills a running background bash shell by its ID.", 4,
         _schema({"shell_id": _s("Shell id")}, ["shell_id"])),
        ("Skill", "Execute a skill within the main conversation.", 12,
         _schema({"skill": _s("Skill name"), "args": _s("Arguments")}, ["skill"])),
        ("SlashCommand", "Execute a slash command within the main conversation.", 6,
         _schema({"command": _s("The slash command")}, ["command"])),
        ("AskUserQuestion", "Ask the user a multiple-choice question.", 10,
         _schema({"questions": {"type": "array", "items": {"type": "object"}}}, ["questions"])),
        ("ToolSearch", "Fetches full schema definitions for deferred tools so they can be called.", 9,
         _schema({"query": _s("select:<name> or keywords"), "max_results": {"type": "number"}}, ["query"])),
        ("EnterPlanMode", "Switch to plan mode to design an approach before coding.", 5, _schema({}, [])),
        ("DeferredToolPlaceholder", "Placeholder for tools that are loaded on demand with ToolSearch.", 1,
         _schema({}, [])),
    ]
    tools = []
    for name, first, n, schema in t:
        tool = {"name": name, "description": _long_desc(first, n), "input_schema": schema}
        if name == "NotebookEdit":
            tool["defer_loading"] = True
        tools.append(tool)
    return tools


TASKSTOP_TOOL = {
    "name": "TaskStop",
    "description": _long_desc("Stops a running background task by its id.", 4),
    "input_schema": _schema({"task_id": _s("The task to stop")}, ["task_id"]),
}

MAIN_SYSTEM_PROMPT = "\n".join(
    ["You are an interactive CLI tool that helps users with software engineering tasks.",
     "Use the instructions below and the tools available to you to assist the user.", ""]
    + [f"# Section {i + 1}\n- Be concise and direct. Explain non-trivial commands before running them.\n"
       f"- Never introduce code that exposes secrets. Follow existing conventions in the codebase.\n"
       f"- Use the TodoWrite tool to plan multi-step work and mark items done as you go."
       for i in range(30)]
)

LONG_CONTEXT = "\n".join(
    ["<agent-context>", "The following describes the user's environment and project conventions."]
    + [f"Context line {i + 1}: working directory /Users/dev/lab4, platform darwin, shell zsh; "
       f"prefer absolute paths, keep diffs small, and do not commit unless asked." for i in range(175)]
    + ["</agent-context>"]
)

REMINDER_1 = ("<system-reminder>\nAs you answer the user's questions, you can use the following context:\n"
              "# currentDate\nToday's date is 2026-11-04.\n</system-reminder>")
REMINDER_2 = ("<system-reminder>\nThe user opened the file /Users/dev/lab4/proxy.py in the IDE. "
              "This may or may not be related to the current task.\n</system-reminder>")


def _read_result() -> str:
    try:
        with open(os.path.abspath(__file__), encoding="utf-8") as f:
            lines = f.read().splitlines()[:140]
    except OSError:
        lines = [f"# line {i}" for i in range(140)]
    return "\n".join(f"{i + 1:>6}→{line}" for i, line in enumerate(lines))


def _sse(events: list, pad_every: int = 3) -> str:
    out = []
    for i, (name, data) in enumerate(events):
        pad = "    " if name == "ping" or i % pad_every == 1 else ""  # trailing spaces, as in the capture
        out.append(f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False, separators=(',', ':'))}{pad}\n\n")
    return "".join(out)


def _pieces(s: str, size: int, rng: random.Random) -> list:
    out, i = [], 0
    while i < len(s):
        n = rng.randint(max(1, size // 2), size + size // 2)
        out.append(s[i:i + n])
        i += n
    return out


def _usage(inp: int, cw: int, cr: int, out: int, thinking: int | None = None) -> dict:
    u = {"input_tokens": inp, "cache_creation_input_tokens": cw, "cache_read_input_tokens": cr,
         "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": cw},
         "output_tokens": out, "service_tier": "standard"}
    if thinking is not None:
        u["output_tokens_details"] = {"thinking_tokens": thinking}
    return u


def _reply_events(rng: random.Random, msg_id: str, model: str, blocks: list, stop_reason: str | None,
                  usage: dict, interleave_tools: bool = False, complete: bool = True) -> list:
    """Build the SSE event list for one streamed reply.

    blocks: ("text", s) | ("thinking", s) | ("tool_use", id, name, input)
    """
    start_usage = dict(usage, output_tokens=min(usage["output_tokens"], 4))
    start_usage.pop("output_tokens_details", None)
    ev = [("message_start", {"type": "message_start", "message": {
        "id": msg_id, "type": "message", "role": "assistant", "model": model, "content": [],
        "stop_reason": None, "stop_sequence": None, "usage": start_usage}})]
    streams = []
    for idx, b in enumerate(blocks):
        if b[0] == "text":
            cb = {"type": "text", "text": ""}
            deltas = [{"type": "text_delta", "text": p} for p in _pieces(b[1], 24, rng)]
        elif b[0] == "thinking":
            cb = {"type": "thinking", "thinking": "", "signature": ""}
            deltas = [{"type": "thinking_delta", "thinking": p} for p in _pieces(b[1], 24, rng)]
            sig = base64.b64encode(rng.randbytes(360)).decode()
            deltas.append({"type": "signature_delta", "signature": sig})
        else:
            _, tid, name, inp = b
            cb = {"type": "tool_use", "id": tid, "name": name, "input": {}, "caller": {"type": "direct"}}
            deltas = [{"type": "input_json_delta", "partial_json": ""}]
            deltas += [{"type": "input_json_delta", "partial_json": p} for p in _pieces(json.dumps(inp), 14, rng)]
        streams.append((idx, b[0], cb, deltas))

    def emit(idx, cb, deltas):
        out = [("content_block_start", {"type": "content_block_start", "index": idx, "content_block": cb})]
        out += [("content_block_delta", {"type": "content_block_delta", "index": idx, "delta": d}) for d in deltas]
        out.append(("content_block_stop", {"type": "content_block_stop", "index": idx}))
        return out

    first = True
    tools = [s for s in streams if s[1] == "tool_use"]
    for s in streams:
        if interleave_tools and s[1] == "tool_use":
            continue
        ev += emit(s[0], s[2], s[3])
        if first:
            ev.append(("ping", {"type": "ping"}))
            first = False
    if interleave_tools and tools:
        # Two tool blocks open at the same time: deltas must be matched by index.
        parts = [emit(s[0], s[2], s[3]) for s in tools]
        ev += [p[0] for p in parts]
        mids = [p[1:-1] for p in parts]
        while any(mids):
            for m in mids:
                if m:
                    ev.append(m.pop(0))
        ev += [p[-1] for p in parts]
    if not complete:
        return ev
    ev.append(("message_delta", {"type": "message_delta",
                                 "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                                 "usage": usage}))
    ev.append(("message_stop", {"type": "message_stop"}))
    return ev


def _chunks(text: str, rng: random.Random) -> list:
    """Cut the encoded body into chunks of random size: many end mid-line (or mid-character)."""
    data = text.encode("utf-8")
    out, i = [], 0
    while i < len(data):
        n = rng.randint(70, 420)
        out.append(data[i:i + n])
        i += n
    return out


def _stream(ex: Exchange, chunks: list, rng: random.Random) -> None:
    for c in chunks:
        time.sleep(rng.uniform(0.04, 0.08))
        ex.chunk(c)


def _headers(session: str, body: bytes, ua: str = "claude-cli/2.1.318 (external, cli)") -> list:
    return [
        ("Host", "localhost:8041"), ("Accept", "application/json"),
        ("X-Stainless-Lang", "js"), ("X-Stainless-Runtime", "node"),
        ("User-Agent", ua), ("X-Claude-Code-Session-Id", session),
        ("anthropic-beta", BETA), ("anthropic-version", "2023-06-01"),
        ("anthropic-dangerous-direct-browser-access", "true"),
        ("Authorization", "Bearer sk-ant-oat01-DEMOxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
        ("Content-Type", "application/json"), ("Content-Length", str(len(body))),
    ]


def _sse_headers(rng: random.Random) -> list:
    return [("Content-Type", "text/event-stream; charset=utf-8"), ("Cache-Control", "no-cache"),
            ("request-id", _rid(rng, "req_011C", 20)), ("anthropic-organization-id", str(uuid.uuid4())),
            ("anthropic-ratelimit-unified-status", "allowed"), ("via", "1.1 google")]


def _with_cache_marker(messages: list) -> list:
    """Deep-copy and put cache_control on the last block of the last array-form user message (it moves)."""
    msgs = copy.deepcopy(messages)
    for m in reversed(msgs):
        if m["role"] == "user" and isinstance(m["content"], list) and m["content"]:
            m["content"][-1]["cache_control"] = {"type": "ephemeral"}
            break
    return msgs


def _demo_pass(rng: random.Random) -> None:
    session = str(uuid.uuid4())
    meta = {"user_id": json.dumps({"device_id": rng.randbytes(32).hex(), "account_uuid": str(uuid.uuid4()),
                                   "session_id": session})}
    gap = 1.5

    # 1. HEAD /api/hello connectivity check.
    hello = Exchange("HEAD", "/api/hello", [("Host", "localhost:8041"), ("User-Agent", "Bun/1.3.1"),
                                            ("Accept", "*/*")], b"")
    time.sleep(0.12)
    hello.start(200, "OK", [("Content-Type", "text/plain"), ("Content-Length", "0")])
    hello.end(True)
    time.sleep(gap)

    # 2. Quota probe and title call, at the same time, replies interleaved.
    quota_body = json.dumps({"model": SMALL_MODEL, "max_tokens": 1,
                             "messages": [{"role": "user", "content": "quota"}], "metadata": meta}).encode()
    prompt1 = "Read proxy.py and tell me what it does"
    title_body = json.dumps({
        "model": SMALL_MODEL,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt1}]}],
        "system": [{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.318.a1f; cc_entrypoint=cli;"},
                   {"type": "text", "text": "Generate a concise, sentence-case title (2-5 words) that captures the "
                                            "main topic of this coding session. Return JSON with a single "
                                            "\"title\" field."}],
        "tools": [], "metadata": meta, "max_tokens": 512,
        "output_config": {"effort": "medium", "format": {"type": "json_schema", "schema": {
            "type": "object", "properties": {"title": {"type": "string"}}, "required": ["title"],
            "additionalProperties": False}}},
        "stream": True}).encode()
    quota = Exchange("POST", "/v1/messages?beta=true", _headers(session, quota_body), quota_body)
    time.sleep(0.03)
    title = Exchange("POST", "/v1/messages?beta=true", _headers(session, title_body), title_body)
    time.sleep(0.35)
    req_id = _rid(rng, "req_011C", 20)
    quota.start(429, "Too Many Requests", [("Content-Type", "application/json"), ("retry-after", "14"),
                                           ("request-id", req_id), ("x-should-retry", "true")])
    title.start(200, "OK", _sse_headers(rng))
    err = json.dumps({"type": "error", "error": {"type": "rate_limit_error", "message": "Error"},
                      "request_id": req_id}).encode()
    t_events = _sse(_reply_events(rng, _rid(rng, "msg_01"), SMALL_MODEL, [("text", '{"title":"Proxy summary"}')],
                                  "end_turn", _usage(182, 0, 0, 12)))
    t_chunks = _chunks(t_events, rng)
    q_chunks = [err[:37], err[37:]]
    for i in range(max(len(t_chunks), len(q_chunks))):  # interleave the two replies chunk by chunk
        if i < len(q_chunks):
            time.sleep(rng.uniform(0.04, 0.08))
            quota.chunk(q_chunks[i])
            if i == len(q_chunks) - 1:
                quota.end(True)
        if i < len(t_chunks):
            time.sleep(rng.uniform(0.04, 0.08))
            title.chunk(t_chunks[i])
    title.end(True)
    time.sleep(gap)

    # 3. The main thread.
    tools = _demo_tools()
    system = [
        {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.318.a1f; cc_entrypoint=cli;"},
        {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude.",
         "cache_control": {"type": "ephemeral", "ttl": "1h"}},
        {"type": "text", "text": MAIN_SYSTEM_PROMPT, "cache_control": {"type": "ephemeral", "ttl": "1h"}},
    ]

    def body_for(messages, tool_list):
        return json.dumps({
            "model": MAIN_MODEL, "messages": _with_cache_marker(messages), "system": system,
            "tools": tool_list, "metadata": meta, "max_tokens": 32000,
            "thinking": {"type": "adaptive", "display": "updates"},
            "context_management": {"edits": [{"type": "clear_thinking_20251015", "keep": "all"}]},
            "safeguards": {"mode": "default"}, "output_config": {"effort": "medium"}, "stream": True,
        }, ensure_ascii=False).encode("utf-8")

    def turn(messages, tool_list, blocks, stop, usage, interleave=False, complete=True, broken=None):
        body = body_for(messages, tool_list)
        ex = Exchange("POST", "/v1/messages?beta=true", _headers(session, body), body)
        time.sleep(rng.uniform(0.5, 0.9))
        ex.start(200, "OK", _sse_headers(rng))
        events = _reply_events(rng, _rid(rng, "msg_01"), MAIN_MODEL, blocks, stop, usage,
                               interleave_tools=interleave, complete=complete)
        _stream(ex, _chunks(_sse(events), rng), rng)
        if broken:
            time.sleep(0.3)
            ex.end(False, broken)
        else:
            ex.end(True)
        time.sleep(gap)

    sig = lambda: base64.b64encode(rng.randbytes(360)).decode()  # noqa: E731
    read_id, b1_id, b2_id, ts_id = (_rid(rng, "toolu_01") for _ in range(4))
    read_input = {"file_path": "/Users/dev/lab4/proxy.py"}
    b1_input = {"command": "cat notes/snippet.html && git status --short",
                "description": "Show the HTML snippet and the working tree status"}
    b2_input = {"command": "python3 -m pytest -q", "description": "Run the test suite"}
    ts_input = {"query": "select:TaskStop", "max_results": 1}

    reply2 = ("`proxy.py` is a small reverse proxy for watching an agent's API traffic. It listens on "
              "`127.0.0.1:8041`, prints every request (headers, with the bearer token shortened, and the "
              "pretty-printed JSON body), forwards it unchanged to `https://api.anthropic.com`, and streams the "
              "reply back chunk by chunk while echoing it to the terminal. Hop-by-hop headers in `SKIP` are "
              "dropped so the reply comes back uncompressed and readable — nothing else is changed.")
    prompt3 = "Nice. Run the tests and check the git status."
    reply3 = "I'll check the working tree and run the test suite in parallel."
    reply4 = ("The working tree has one modified file. The test run failed: `pytest` isn't installed "
              "(exit code 1). I'll load the task tool so I can stop the background watcher.")
    reply5 = ("Done. Summary:\n\n- `proxy.py` forwards traffic unchanged and prints it.\n"
              "- `git status`: `proxy.py` modified, `dashboard.html` untracked.\n"
              "- Tests could not run because `pytest` is missing — install it with "
              "`python3 -m pip install pytest`.\n\nThe background watcher is stopped.")
    prompt6 = "Thanks. Now sketch a dashboard for it."
    reply6 = "Here's a plan for the dashboard. First, the proxy needs to tag each request and its reply with"

    u0 = {"role": "user", "content": [{"type": "text", "text": REMINDER_1}, {"type": "text", "text": REMINDER_2},
                                      {"type": "text", "text": prompt1}]}
    s1 = {"role": "system", "content": [
        {"type": "text", "text": LONG_CONTEXT},
        {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": {
            "name": "mcp__ide__getDiagnostics", "description": "Get language diagnostics from VS Code.",
            "input_schema": _schema({"uri": _s("File URI")}, [])}}},
        {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": {
            "name": "mcp__ide__executeCode", "description": "Execute python code in the Jupyter kernel.",
            "input_schema": _schema({"code": _s("Code to run")}, ["code"])}}},
    ]}
    a2 = {"role": "assistant", "content": [
        {"type": "thinking", "thinking": "", "signature": sig()},
        {"type": "tool_use", "id": read_id, "name": "Read", "input": read_input, "caller": {"type": "direct"}}]}
    u3 = {"role": "user", "content": [{"type": "tool_result", "tool_use_id": read_id, "content": _read_result()}]}
    a4 = {"role": "assistant", "content": [{"type": "text", "text": reply2}]}
    u5_str = {"role": "user", "content": prompt3}
    u5_arr = {"role": "user", "content": [{"type": "text", "text": prompt3}]}
    a6 = {"role": "assistant", "content": [
        {"type": "text", "text": reply3},
        {"type": "tool_use", "id": b1_id, "name": "Bash", "input": b1_input, "caller": {"type": "direct"}},
        {"type": "tool_use", "id": b2_id, "name": "Bash", "input": b2_input, "caller": {"type": "direct"}}]}
    u7 = {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": b1_id,
         "content": "<b>hi</b><script>alert(1)</script>\n M proxy.py\n?? dashboard.html"},
        {"type": "tool_result", "tool_use_id": b2_id, "is_error": True,
         "content": "Exit code 1\n/usr/bin/python3: No module named pytest"}]}
    a8 = {"role": "assistant", "content": [
        {"type": "text", "text": reply4},
        {"type": "tool_use", "id": ts_id, "name": "ToolSearch", "input": ts_input, "caller": {"type": "direct"}}]}
    u9 = {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": ts_id, "content": [{"type": "tool_reference", "tool_name": "TaskStop"}]},
        {"type": "text", "text": "Tool loaded."}]}
    a10 = {"role": "assistant", "content": [{"type": "text", "text": reply5}]}
    u11 = {"role": "user", "content": prompt6}

    tools21 = tools + [TASKSTOP_TOOL]

    # Request 1: prompt + mid-conversation system message; reply = hidden thinking + Read.
    turn([u0, s1], tools, [("thinking", ""), ("tool_use", read_id, "Read", read_input)], "tool_use",
         _usage(2, 30315, 0, 112, thinking=38))
    # Request 2: the Read result; reply = text.
    turn([u0, s1, a2, u3], tools, [("text", reply2)], "end_turn", _usage(1, 3120, 30315, 241, thinking=0))
    # Request 3: new prompt as a plain string; reply = text + two parallel Bash calls.
    turn([u0, s1, a2, u3, a4, u5_str], tools,
         [("text", reply3), ("tool_use", b1_id, "Bash", b1_input), ("tool_use", b2_id, "Bash", b2_input)],
         "tool_use", _usage(2, 410, 33435, 168, thinking=0), interleave=True)
    # Request 4: both results (one is_error); the same prompt is now an array. Reply = text + ToolSearch.
    turn([u0, s1, a2, u3, a4, u5_arr, a6, u7], tools,
         [("text", reply4), ("tool_use", ts_id, "ToolSearch", ts_input)], "tool_use",
         _usage(1, 1890, 33845, 95, thinking=0))
    # Request 5: tool_reference result + "Tool loaded."; TaskStop added to tools. Final answer.
    turn([u0, s1, a2, u3, a4, u5_str, a6, u7, a8, u9], tools21, [("text", reply5)], "end_turn",
         _usage(1, 640, 35735, 210, thinking=0))
    # Request 6: the stream breaks before message_stop.
    msgs6 = [u0, s1, a2, u3, a4, u5_str, a6, u7, a8, u9, a10, u11]
    turn(msgs6, tools21, [("text", reply6)], None, _usage(1, 380, 36375, 3), complete=False,
         broken="stream broke: upstream connection reset (ConnectionResetError)")

    # 4. A retry that hits an unreachable upstream: the proxy's own 502.
    body = body_for(msgs6, tools21)
    ex = Exchange("POST", "/v1/messages?beta=true", _headers(session, body), body)
    time.sleep(0.2)
    msg = "cannot reach https://api.anthropic.com: [Errno 61] Connection refused"
    ex.start(502, "Bad Gateway", [("Content-Type", "text/html;charset=utf-8"), ("Connection", "close")])
    ex.text(f"proxy: {msg}")
    ex.end(False, msg)


def run_demo() -> None:
    rng = random.Random(41)
    time.sleep(1.0)
    while True:
        try:
            _demo_pass(rng)
        except Exception as e:  # the demo must never take the proxy down
            print(f"[{stamp()}] demo error: {e}", file=sys.stderr, flush=True)
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
        print("demo mode: replaying the built-in sample on the dashboard (loops every pass)")
        threading.Thread(target=run_demo, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
