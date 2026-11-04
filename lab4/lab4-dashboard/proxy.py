#!/usr/bin/env python3
"""proxy.py: the agent-traffic proxy and live dashboard for Lab 4.

A reverse proxy that prints every request and response and forwards them
unchanged. Standard library only; nothing to install.

    python3 proxy.py                       # listens on http://localhost:8041
    ANTHROPIC_BASE_URL=http://localhost:8041 claude

Everything Claude Code sends to the API now passes through this program and is
printed to the terminal. The proxy changes nothing: headers go through untouched
(a subscription login needs its bearer token and its `anthropic-beta` header),
and the reply is streamed back byte for byte as it arrives.

The same traffic is shown live, and easier to read, in the browser:

    open http://localhost:8041/__dashboard
    python3 proxy.py --demo                # replays a built-in sample; no API needed

    python3 proxy.py --port 9000
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
"""

from __future__ import annotations

import argparse
import codecs
import copy
import http.client
import json
import os
import queue
import random
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


MAX_EXCHANGES = 500   # exchanges kept in memory, oldest dropped first
MAX_QUEUED = 1000     # a page this far behind is dropped; it reconnects by itself
KEEPALIVE_SECONDS = 15
PAGE_WRITE_TIMEOUT = 30  # a page that stops reading must not hold a thread forever
DASHBOARD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def shorten(name: str, value: str) -> str:
    """Credentials are never shown in full, in the terminal or on the page."""
    if name.lower() in ("authorization", "x-api-key"):
        return value[:16] + "..."
    return value


# --- Exchange store ----------------------------------------------------------
# The proxy only records and passes data on. All interpretation happens in the
# page. Events are serialized once, when published, and kept in that form: it is
# what replay sends, and it is far smaller than the parsed request bodies.

store_lock = threading.Lock()
exchanges: OrderedDict[int, dict] = OrderedDict()
pages: set[queue.Queue] = set()
last_id = 0


def sse_message(event: dict) -> bytes:
    # json.dumps' default ensure_ascii keeps lone surrogates from a request body
    # from failing at encode time.
    return f"data: {json.dumps(event)}\n\n".encode("ascii")


def broadcast(message: bytes) -> None:
    """Hand a message to every page. Call with store_lock held. Never blocks."""
    for page in list(pages):
        if page.qsize() >= MAX_QUEUED:
            pages.discard(page)
            page.put(None)  # tells that page's handler to hang up
        else:
            page.put(message)


def reject_constant(name: str) -> None:
    raise ValueError(name)  # NaN and Infinity are not JSON; the page could not parse them


def begin_exchange(method: str, path: str, headers, body: bytes) -> int:
    global last_id
    try:
        parsed = json.loads(body, parse_constant=reject_constant)
    except (ValueError, RecursionError):
        parsed = None
    event = {
        "type": "request",
        "t": time.time() * 1000,
        "method": method,
        "path": path,
        "headers": [[name, shorten(name, value)] for name, value in headers],
        "body": body.decode("utf-8", "replace") if parsed is None else None,
        "body_json": parsed,
    }
    with store_lock:
        last_id += 1
        xid = last_id
        message = sse_message({**event, "id": xid})
        exchanges[xid] = {
            "request": message,
            "start": None,
            "text": [],
            "chunk_t": None,
            "end": None,
            "decoder": codecs.getincrementaldecoder("utf-8")(errors="replace"),
        }
        while len(exchanges) > MAX_EXCHANGES:
            exchanges.popitem(last=False)
        broadcast(message)
    return xid


def start_response(xid: int, status: int, reason: str, headers) -> None:
    message = sse_message({
        "type": "response_start",
        "id": xid,
        "t": time.time() * 1000,
        "status": status,
        "reason": reason,
        "headers": [[name, shorten(name, value)] for name, value in headers],
    })
    with store_lock:
        if xid in exchanges:
            exchanges[xid]["start"] = message
        broadcast(message)


def publish_text(xid: int, text: str) -> None:
    """Call with store_lock held."""
    t = time.time() * 1000
    if xid in exchanges:
        exchanges[xid]["text"].append(text)
        exchanges[xid]["chunk_t"] = t
    broadcast(sse_message({"type": "response_chunk", "id": xid, "t": t, "text": text}))


def add_chunk(xid: int, chunk: bytes) -> None:
    with store_lock:
        if xid in exchanges:  # else it was dropped from the store mid-stream
            publish_text(xid, exchanges[xid]["decoder"].decode(chunk))


def end_exchange(xid: int, error: str | None = None) -> None:
    with store_lock:
        if xid in exchanges:
            tail = exchanges[xid]["decoder"].decode(b"", final=True)
            if tail:
                publish_text(xid, tail)
        message = sse_message({
            "type": "response_end",
            "id": xid,
            "t": time.time() * 1000,
            "ok": error is None,
            "error": error,
        })
        if xid in exchanges:
            exchanges[xid]["end"] = message
        broadcast(message)


def subscribe() -> tuple[queue.Queue, list]:
    """Join the live feed and take a snapshot in one step, so that no event is
    missed or sent twice between the replay and the live messages."""
    page: queue.Queue = queue.Queue()
    with store_lock:
        snapshot = [
            (x["request"], x["start"], "".join(x["text"]), x["chunk_t"], x["end"], xid)
            for xid, x in exchanges.items()
        ]
        pages.add(page)
    return page, snapshot


def unsubscribe(page: queue.Queue) -> None:
    with store_lock:
        pages.discard(page)


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

    # --- Local routes: never forwarded, never printed ------------------------

    def local(self) -> None:
        path = urlsplit(self.path).path
        if self.command == "GET" and path == "/__dashboard":
            self.serve_dashboard()
        elif self.command == "GET" and path == "/__events":
            self.serve_events()
        else:
            self.close_connection = True  # any request body stays unread
            self.send_error(404)

    def serve_dashboard(self) -> None:
        try:
            with open(DASHBOARD, "rb") as f:  # read every time: a reload picks up edits
                page = f.read()
        except OSError as e:
            self.send_error(500, f"cannot read dashboard.html: {e.strerror}")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(page)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(page)

    def serve_events(self) -> None:
        self.close_connection = True
        self.connection.settimeout(PAGE_WRITE_TIMEOUT)
        page, snapshot = subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(sse_message({"type": "reset"}))
            for request, start, text, chunk_t, end, xid in snapshot:
                self.wfile.write(request)
                if start:
                    self.wfile.write(start)
                if text:
                    self.wfile.write(sse_message({"type": "response_chunk", "id": xid, "t": chunk_t, "text": text}))
                if end:
                    self.wfile.write(end)
            while True:
                try:
                    message = page.get(timeout=KEEPALIVE_SECONDS)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    continue
                if message is None:
                    return
                self.wfile.write(message)
        except (ConnectionError, TimeoutError):
            pass  # the page went away
        finally:
            unsubscribe(page)

    # --- Relay ---------------------------------------------------------------

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        xid = begin_exchange(self.command, self.path, self.headers.items(), body)
        error = None
        try:
            error = self.forward(xid, body)
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            raise  # handled as it always was; the page just gets told first
        finally:
            end_exchange(xid, error)  # always, or the row stays "pending" forever

    def forward(self, xid: int, body: bytes) -> str | None:
        """Forward one request and stream the reply back. Returns an error
        message if the upstream could not be reached, otherwise None."""
        # 1. Print what the agent sent.
        print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
        for name, value in self.headers.items():
            print(f"    {name}: {shorten(name, value)}")
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
            start_response(xid, 502, "Bad Gateway", [])
            self.close_connection = True
            message = f"proxy: cannot reach {UPSTREAM}: {e}"
            self.send_error(502, message)
            return message
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
        reply = upstream.getresponse()
        start_response(xid, reply.status, reply.reason, reply.getheaders())

        # 3. Stream the reply back as it arrives, printing it on the way.
        print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command == "HEAD":
            self.close_connection = True
            return None
        while chunk := reply.read1(4096):  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
            self.wfile.write(chunk)
            self.wfile.flush()
            add_chunk(xid, chunk)
            sys.stdout.write(chunk.decode("utf-8", "replace"))
            sys.stdout.flush()
        self.close_connection = True
        print(f"\n[{stamp()}] --- done", flush=True)
        return None


# --- Demo mode ---------------------------------------------------------------
# A built-in sample with the shapes of real Claude Code traffic. It goes through
# the same event functions as real traffic, so the page can't tell the difference.

DEMO_GAP = 1.5           # seconds between requests
DEMO_LOOP_PAUSE = 10     # seconds between replays of the sample
MODEL = "claude-opus-5-5"
SMALL_MODEL = "claude-haiku-5-5"
MARKER = {"type": "ephemeral"}
XSS_TEXT = "<b>hi</b><script>alert(1)</script>"

TOOL_SPECS = [
    # name, description, input properties
    ("Agent", "Launch a subagent to handle a multi-step task.", ["description", "prompt"]),
    ("Bash", "Executes a bash command and returns its output.\n\nUse for git, tests and scripts.", ["command", "description"]),
    ("Edit", "Performs exact string replacement in a file.", ["file_path", "old_string", "new_string"]),
    ("Glob", "Finds files by name pattern.", ["pattern", "path"]),
    ("Grep", "Searches file contents with a regular expression.", ["pattern", "path"]),
    ("Read", "Reads a file from the local filesystem.\n\nResults use cat -n format.", ["file_path", "offset", "limit"]),
    ("Write", "Writes a file to the local filesystem.", ["file_path", "content"]),
    ("NotebookEdit", "Replaces a cell in a Jupyter notebook.", ["notebook_path", "new_source"]),
    ("WebFetch", "Fetches a URL and answers a question about it.", ["url", "prompt"]),
    ("WebSearch", "Searches the web.", ["query"]),
    ("TodoWrite", "Updates the task list for this session.", ["todos"]),
    ("AskUserQuestion", "Asks the user to choose between options.", ["questions"]),
    ("Skill", "Runs a packaged skill.", ["skill", "args"]),
    ("ToolSearch", "Loads the full definition of a deferred tool.", ["query", "max_results"]),
    ("TaskCreate", "Starts a background task.", ["subject"]),
    ("TaskList", "Lists background tasks.", ["status"]),
    ("TaskUpdate", "Updates a background task.", ["task_id", "status"]),
    ("ExitPlanMode", "Leaves plan mode and asks for approval.", ["plan"]),
    ("Monitor", "Watches a background process.", ["task_id"]),
    ("DeferredToolPlaceholder", "Stands in for tools whose definitions load on demand.", []),
]


def tool_definition(name: str, description: str, fields: list[str]) -> dict:
    return {
        "name": name,
        "description": description,
        "input_schema": {
            "type": "object",
            "properties": {field: {"type": "string"} for field in fields},
            "required": fields[:1],
            "additionalProperties": False,
        },
    }


def demo_tools() -> list[dict]:
    tools = [tool_definition(*spec) for spec in TOOL_SPECS]
    for tool in tools:
        if tool["name"] == "Monitor":
            tool["defer_loading"] = True
    return tools


def filler(title: str, size: int) -> str:
    """Long, plausible instruction text of about `size` characters."""
    lines = [f"# {title}", ""]
    n = 0
    while sum(len(line) + 1 for line in lines) < size:
        n += 1
        lines.append(f"{n}. Keep changes small, match the style of the code around them, "
                     f"and report what was run and what it printed.")
    return "\n".join(lines)


def demo_system() -> list[dict]:
    return [
        {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.318.a1b; cc_entrypoint=cli;"},
        {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude.", "cache_control": MARKER},
        {"type": "text", "text": filler("Working with the user", 7000), "cache_control": MARKER},
    ]


def demo_read_result() -> str:
    source = [
        "import json", "", "def relay(self):", "    body = self.rfile.read(length)",
        "    print(self.command, self.path)", "    upstream.request(self.command, self.path, body=body)",
        "    reply = upstream.getresponse()", "    while chunk := reply.read1(4096):",
        "        self.wfile.write(chunk)", "",
    ]
    return "\n".join(f"{n:>6}\t{source[n % len(source)]}" for n in range(1, 131))


def new_id(prefix: str) -> str:
    return prefix + uuid.uuid4().hex[:24]


def usage(fresh: int, write: int, read: int, output: int) -> dict:
    return {
        "input_tokens": fresh,
        "cache_creation_input_tokens": write,
        "cache_read_input_tokens": read,
        "output_tokens": output,
        "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": write},
        "service_tier": "standard",
    }


def tool_use(name: str, tool_input: dict) -> dict:
    return {"type": "tool_use", "id": new_id("toolu_"), "name": name, "input": tool_input, "caller": {"type": "direct"}}


def text(value: str) -> dict:
    return {"type": "text", "text": value}


def thinking(value: str = "") -> dict:
    return {"type": "thinking", "thinking": value, "signature": uuid.uuid4().hex * 8}


def assistant(blocks: list[dict]) -> dict:
    return {"role": "assistant", "content": copy.deepcopy(blocks)}


def tool_result(call: dict, content, **extra) -> dict:
    return {"type": "tool_result", "tool_use_id": call["id"], "content": content, **extra}


def pieces(value: str, size: int) -> list[str]:
    return [value[i:i + size] for i in range(0, len(value), size)]


def block_events(index: int, block: dict) -> list[dict]:
    """The stream events for one content block."""
    if block["type"] == "text":
        start = text("")
        deltas = [{"type": "text_delta", "text": part} for part in pieces(block["text"], 24)]
    elif block["type"] == "thinking":
        start = {"type": "thinking", "thinking": "", "signature": ""}
        deltas = [{"type": "thinking_delta", "thinking": part} for part in pieces(block["thinking"], 24)]
        deltas.append({"type": "signature_delta", "signature": block["signature"]})
    elif block["type"] == "tool_use":
        start = {**block, "input": {}}
        deltas = [{"type": "input_json_delta", "partial_json": part}
                  for part in ["", *pieces(json.dumps(block["input"]), 14)]]
    else:
        return [{"type": "content_block_start", "index": index, "content_block": block},
                {"type": "content_block_stop", "index": index}]
    return [
        {"type": "content_block_start", "index": index, "content_block": start},
        *[{"type": "content_block_delta", "index": index, "delta": delta} for delta in deltas],
        {"type": "content_block_stop", "index": index},
    ]


def sse_text(events: list[dict]) -> str:
    out = []
    for n, event in enumerate(events):
        padding = "    " if event["type"] == "ping" or n % 7 == 3 else ""  # real streams have these
        out.append(f"event: {event['type']}\ndata: {json.dumps(event, ensure_ascii=False)}{padding}\n\n")
    return "".join(out)


def stream_reply(blocks: list[dict], use: dict, stop: str = "end_turn", model: str = MODEL,
                 overlap: bool = False, failure: str | None = None) -> bytes:
    """An SSE reply. overlap: blocks are open at the same time, their deltas mixed.
    failure "cut": the stream just stops. "error": it ends with an error event."""
    start_use = {**use, "output_tokens": min(use["output_tokens"], 16)}
    events = [{"type": "message_start", "message": {
        "id": new_id("msg_"), "type": "message", "role": "assistant", "model": model,
        "content": [], "stop_reason": None, "usage": start_use}}]
    per_block = [block_events(i, block) for i, block in enumerate(blocks)]
    if overlap:
        for step in range(max(map(len, per_block))):
            events += [block[step] for block in per_block if step < len(block)]
    else:
        for block in per_block:
            events += block
    events.insert(2, {"type": "ping"})
    if failure == "cut":
        return sse_text(events)[:-40].encode()
    if failure == "error":
        events.append({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"},
                       "request_id": new_id("req_")})
        return sse_text(events).encode()
    events.append({"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None},
                   "usage": {**use, "output_tokens_details": {"thinking_tokens": 0}}})
    events.append({"type": "message_stop"})
    return sse_text(events).encode()


def cut(data: bytes) -> list[bytes]:
    """Split a reply into chunks at arbitrary byte offsets: mid-line, sometimes
    mid-character, as real 4096-byte reads can be."""
    least = max(60, len(data) // 22)
    chunks, at = [], 0
    while at < len(data):
        size = random.randint(least, least * 2)
        chunks.append(data[at:at + size])
        at += size
    return chunks


def pause_between_chunks() -> None:
    time.sleep(random.uniform(0.04, 0.08))


class DemoSession:
    """One agent session: its id, and the request helpers that use it."""

    def __init__(self) -> None:
        self.session_id = str(uuid.uuid4())
        self.user_id = json.dumps({"device_id": uuid.uuid4().hex, "account_uuid": str(uuid.uuid4()),
                                   "session_id": self.session_id})

    def headers(self) -> list[tuple[str, str]]:
        return [
            ("Host", "localhost:8041"),
            ("Authorization", "Bearer sk-ant-oat01-demo-not-a-real-token"),
            ("Content-Type", "application/json"),
            ("User-Agent", "claude-cli/2.1.318 (external, cli)"),
            ("X-Claude-Code-Session-Id", self.session_id),
            ("anthropic-beta", "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,"
                               "context-management-2025-06-27,prompt-caching-scope-2026-01-05"),
            ("anthropic-version", "2023-06-01"),
        ]

    def begin(self, fields: dict) -> int:
        body = {"model": MODEL, **fields, "metadata": {"user_id": self.user_id}}
        return begin_exchange("POST", "/v1/messages?beta=true", self.headers(), json.dumps(body).encode())

    def main_body(self, messages: list[dict], tools: list[dict], system=None) -> dict:
        return {
            "model": MODEL,
            "messages": with_moving_marker(messages),
            "system": system if system is not None else demo_system(),
            "tools": tools,
            "max_tokens": 32000,
            "thinking": {"type": "adaptive", "display": "updates"},
            "context_management": {"edits": [{"type": "clear_thinking_20251015", "keep": "all"}]},
            "safeguards": {"mode": "default"},
            "output_config": {"effort": "medium"},
            "stream": True,
        }

    def turn(self, messages: list[dict], tools: list[dict], reply: bytes, system=None, error: str | None = None) -> None:
        """One request, its streamed reply, and the pause after it."""
        xid = self.begin(self.main_body(messages, tools, system))
        time.sleep(0.35)
        start_response(xid, 200, "OK", sse_headers())
        for chunk in cut(reply):
            add_chunk(xid, chunk)
            pause_between_chunks()
        end_exchange(xid, error)
        time.sleep(DEMO_GAP)


def with_moving_marker(messages: list[dict]) -> list[dict]:
    """The agent marks the newest array-content message for caching, so the
    marker moves from request to request."""
    marked = copy.deepcopy(messages)
    for message in reversed(marked):
        if isinstance(message["content"], list) and message["role"] == "user":
            message["content"][-1]["cache_control"] = MARKER
            break
    return marked


def sse_headers() -> list[tuple[str, str]]:
    return [("Content-Type", "text/event-stream; charset=utf-8"), ("request-id", new_id("req_")),
            ("anthropic-organization-id", "00000000-0000-0000-0000-000000000000")]


def demo_side_calls(session: DemoSession) -> None:
    # 1. Connectivity check.
    xid = begin_exchange("HEAD", "/api/hello", [("Host", "localhost:8041"), ("User-Agent", "Bun/1.3.0")], b"")
    time.sleep(0.3)
    start_response(xid, 200, "OK", [("Content-Length", "0")])
    end_exchange(xid)
    time.sleep(DEMO_GAP)

    # 2. A quota probe and a title call at the same time, their chunks interleaved.
    quota = session.begin({"max_tokens": 1, "messages": [{"role": "user", "content": "quota"}]})
    title = session.begin({
        "model": SMALL_MODEL,
        "max_tokens": 200,
        "messages": [{"role": "user", "content": "Read proxy.py and tell me what it does"}],
        "system": [text("Generate a short session name for this conversation.")],
        "tools": [],
        "output_config": {"effort": "medium", "format": {"type": "json_schema", "schema": {
            "type": "object", "properties": {"title": {"type": "string"}},
            "required": ["title"], "additionalProperties": False}}},
        "stream": True,
    })
    time.sleep(0.35)
    denial = json.dumps({"type": "error", "error": {"type": "rate_limit_error", "message": "Error"},
                         "request_id": new_id("req_")}).encode()
    start_response(title, 200, "OK", sse_headers())
    start_response(quota, 429, "Too Many Requests", [("Content-Type", "application/json"),
                                                     ("request-id", new_id("req_")), ("retry-after", "30")])
    reply = stream_reply([text('{"title":"Proxy summary"}')], usage(120, 0, 0, 12), model=SMALL_MODEL)
    title_chunks = cut(reply)
    denial_chunks = [denial[:40], denial[40:80], denial[80:]]
    for n in range(max(len(title_chunks), len(denial_chunks))):
        if n < len(denial_chunks):
            add_chunk(quota, denial_chunks[n])
            if n == len(denial_chunks) - 1:
                end_exchange(quota)
        if n < len(title_chunks):
            add_chunk(title, title_chunks[n])
        pause_between_chunks()
    end_exchange(title)
    time.sleep(DEMO_GAP)


def demo_main_thread(session: DemoSession) -> None:
    tools = demo_tools()
    first_prompt = {"role": "user", "content": [
        text("<system-reminder>\nAs you answer the user's questions, you can use this context:\n"
             "# currentDate\nToday's date is 2026-11-04.\n</system-reminder>"),
        text("<system-reminder>\nThe user is working in lab4-dashboard. "
             "Its git status is clean.\n</system-reminder>"),
        text("Read proxy.py and tell me what it does"),
    ]}
    agent_context = {"role": "system", "content": [
        text(filler("Agent context", 26000)),
        {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": tool_definition(
            "mcp__notes__search", "Searches the user's notes.", ["query"])}},
        {"type": "tool_addition", "tool": {"type": "tool_definition", "definition": tool_definition(
            "mcp__notes__read", "Reads one note.", ["note_id"])}},
    ]}

    # 1. The Read call; its input streams as JSON fragments.
    read_call = tool_use("Read", {"file_path": "/Users/demo/lab4/proxy.py"})
    blocks = [thinking(), read_call]
    messages = [first_prompt, agent_context]
    session.turn(messages, tools, stream_reply(blocks, usage(2, 30315, 0, 96), "tool_use"))

    # 2. The Read result (string content), and a prompt the user queued meanwhile,
    # sent as an array here and as a plain string from the next request on.
    aside = "Keep the answer short, please."
    history = [*messages, assistant(blocks),
               {"role": "user", "content": [tool_result(read_call, demo_read_result())]}]
    blocks = [text("proxy.py is a small reverse proxy → it prints each request and reply "
                   "to the terminal and forwards them unchanged, streaming the reply back chunk by chunk.")]
    session.turn([*history, {"role": "user", "content": [text(aside)]}], tools,
                 stream_reply(blocks, usage(1, 2204, 30315, 58)))

    # 3. A new prompt as a plain string. The reply makes two parallel Bash calls.
    prompt = "Check that it only listens on localhost, and show me the error log."
    history += [{"role": "user", "content": aside}, assistant(blocks)]
    ok_call = tool_use("Bash", {"command": "grep -n 'ThreadingHTTPServer' proxy.py; cat sample.html",
                                "description": "Find the listen address and show the sample page"})
    bad_call = tool_use("Bash", {"command": "cat proxy-errors.log", "description": "Show the error log"})
    blocks = [ok_call, bad_call]
    session.turn([*history, {"role": "user", "content": prompt}], tools,
                 stream_reply(blocks, usage(1, 310, 32519, 142), "tool_use", overlap=True))

    # 4. Both results, one failed. The prompt from request 3 is an array now.
    history += [{"role": "user", "content": [text(prompt)]}, assistant(blocks), {"role": "user", "content": [
        tool_result(ok_call, f'121:    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)\n{XSS_TEXT}'),
        tool_result(bad_call, "cat: proxy-errors.log: No such file or directory", is_error=True),
    ]}]
    search_call = tool_use("ToolSearch", {"query": "select:TaskStop", "max_results": 1})
    blocks = [text("It binds to 127.0.0.1 only. There is no error log. I'll load the task tool next."), search_call]
    session.turn(history, tools, stream_reply(blocks, usage(1, 420, 32829, 77), "tool_use"))

    # 5. A loaded tool: a tool_reference result, text beside it, and a new tool.
    history += [assistant(blocks), {"role": "user", "content": [
        tool_result(search_call, [{"type": "tool_reference", "tool_name": "TaskStop"}]),
        text("Tool loaded."),
    ]}]
    tools = [*tools, tool_definition("TaskStop", "Stops a running background task.", ["task_id"])]
    blocks = [text("Done: the proxy listens on localhost only, and nothing in it changes the traffic.")]
    session.turn(history, tools, stream_reply(blocks, usage(1, 188, 33249, 41)))

    # 6. The reply stream breaks partway through.
    history += [assistant(blocks), {"role": "user", "content": "Now sum that up in one line."}]
    blocks = [text("In one line: it is a transparent logging proxy between the agent and the model API, "
                   "and it never alters what passes through it.")]
    session.turn(history, tools, stream_reply(blocks, usage(1, 96, 33437, 30), failure="cut"),
                 error="upstream stream broke: connection reset by peer")

    # The agent retries, and this time the upstream cannot be reached.
    xid = session.begin(session.main_body(history, tools))
    time.sleep(0.35)
    start_response(xid, 502, "Bad Gateway", [])
    end_exchange(xid, "proxy: cannot reach https://api.anthropic.com: [Errno 8] nodename nor servname provided")
    time.sleep(DEMO_GAP)


def demo_subagent(session: DemoSession) -> None:
    """A subagent: its own main thread. Covers the remaining shapes of section 3:
    `system` as a plain string, visible thinking, a tool_result with text blocks,
    a block type the page doesn't know, max_tokens, and an SSE error event."""
    tools = demo_tools()[:6]
    system = "You are a search subagent for Claude Code. Report what you find, briefly."
    grep_call = tool_use("Grep", {"pattern": "SKIP", "path": "proxy.py"})
    blocks = [thinking("The user wants to know which headers are dropped. Search for the set."), grep_call]
    messages = [{"role": "user", "content": "Find which headers the proxy drops."}]
    session.turn(messages, tools, stream_reply(blocks, usage(3, 4120, 0, 64), "tool_use"), system)

    history = [*messages, assistant(blocks), {"role": "user", "content": [
        tool_result(grep_call, [text("32:SKIP = {"), text('98:    if name.lower() not in SKIP')]),
        {"type": "search_note", "note": "A block type the dashboard has never seen.", "hits": 2},
    ]}]
    blocks = [text("The proxy drops hop-by-hop headers: host, connection, keep-alive, transfer-encoding, and")]
    session.turn(history, tools, stream_reply(blocks, usage(1, 180, 4120, 8000), "max_tokens"), system)

    history += [assistant(blocks), {"role": "user", "content": "Continue."}]
    blocks = [text("…and also accept-encoding and")]
    session.turn(history, tools, stream_reply(blocks, usage(1, 40, 4300, 12), failure="error"), system)


def demo_loop() -> None:
    while True:
        session = DemoSession()  # fresh session and ids, so each replay is new traffic
        demo_side_calls(session)
        demo_main_thread(session)
        demo_subagent(session)
        time.sleep(DEMO_LOOP_PAUSE)


def main() -> None:
    global UPSTREAM
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--demo", action="store_true", help="also replay the built-in sample; no API needed")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    if args.demo:
        print(f"demo traffic on:     http://localhost:{args.port}/__dashboard")
        threading.Thread(target=demo_loop, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
