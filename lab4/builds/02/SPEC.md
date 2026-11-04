# SPEC: Live Agent Traffic Dashboard

## 1. Goal

`proxy.py` sits between a coding agent (Claude Code) and the model API. It
prints every request and reply to the terminal, and forwards them unchanged.
Build a browser dashboard that shows the same traffic live and is easy to read.

With the proxy running, the user opens `http://localhost:8041/__dashboard` and
sees every exchange between the agent and the model as it happens, newest
first. **Readability is the main goal.** Messages pile up fast, and at a glance
the user must be able to tell apart:

- their own prompt
- the model's reply text
- a tool call
- the tool call's result

The user must also be able to follow the conversation as it grows.

The starting folder contains only `SPEC.md` and `proxy.py`. Everything the
build needs is in this document. There is no `traffic.log`. Section 3 describes
the real traffic; build the parser and the `--demo` sample from it.

## 2. Deliverables and constraints

| File | What |
|---|---|
| `proxy.py` | The existing proxy, extended as described in section 4. |
| `dashboard.html` | One self-contained page: inline CSS and plain JavaScript. |

- **Python standard library only.** Nothing to install, no build step.
- **The page makes no external requests**: no CDN, no fonts, no frameworks.
- **Forwarding behavior must not change.**
  - Headers pass through exactly as today (same `SKIP` set).
  - Replies stream back to the agent chunk by chunk as they arrive.
  - The terminal output keeps its current format.
- The server still listens on `127.0.0.1` only.
- The page is served from disk: `proxy.py` reads `dashboard.html` from its own
  directory on every request, so a page reload picks up edits.

Run it:

```
python3 proxy.py                # proxy + dashboard on :8041
python3 proxy.py --demo         # also replays the built-in sample (section 9); no API needed
ANTHROPIC_BASE_URL=http://localhost:8041 claude
open http://localhost:8041/__dashboard
```

## 3. What the real traffic looks like

This section was taken from a real capture of Claude Code 2.1.x talking to
`api.anthropic.com` through `proxy.py`. The parser must handle every shape listed
here. The demo sample (section 9) must contain every one of them.

### 3.1 Replies are not labelled with their request

`proxy.py` is a `ThreadingHTTPServer`. The agent often has 2–3 calls running at
once, so their terminal output gets mixed together. In the capture, a
`200 OK` (from a title request) and a `429` (from a quota probe) print back to
back. Two streamed replies finish at the same second, and nothing in the output
says which request each one answers.

**Consequence:** the terminal output cannot be parsed reliably. The proxy itself
must tag every request and its reply with a shared exchange id (section 4).

### 3.2 The agent makes side calls alongside the main conversation

The capture contains three kinds of side call:

- **`HEAD /api/hello`**: a connectivity check. No body. Headers include
  `User-Agent: Bun/...`.
- **Quota probe**: `POST /v1/messages?beta=true` with body
  `{"model": "...", "max_tokens": 1, "messages": [{"role": "user", "content": "quota"}], "metadata": {...}}`.
  Here it was answered with a 429.
- **Session-title generation**: `"tools": []`. The system prompt asks for a
  session name, and `"output_config": {"effort": "medium", "format": {"type":
  "json_schema", "schema": {"type": "object", "properties": {"title": {"type":
  "string"}}, ...}}}`. The reply is streamed text that is JSON:
  `{"title":"Proxy.py summary"}`.

The main agent requests have a non-empty `tools` array (about 20 tools).

### 3.3 Request shape: each request resends the whole conversation

`POST /v1/messages?beta=true` with JSON body keys:
`model, messages, system, tools, metadata, max_tokens, thinking,
context_management, safeguards, output_config, stream`. Example values:

- `"thinking": {"type": "adaptive", "display": "updates"}`
- `"context_management": {"edits": [{"type": "clear_thinking_20251015", "keep": "all"}]}`
- `"output_config": {"effort": "medium"}`
- `"stream": true`

Other body fields:

- **`system`**: an array of text blocks, for example:
  1. `"x-anthropic-billing-header: cc_version=...; cc_entrypoint=cli;"` (70 chars)
  2. `"You are Claude Code, Anthropic's official CLI for Claude."`
  3. the main system prompt (about 7 KB)

  Some blocks carry `"cache_control": {...}`. `system` may also be a plain string.
- **`tools`**: an array of `{name, description, input_schema}`. Descriptions can
  be several KB. Some entries have `"defer_loading": true`. One special entry is
  named `DeferredToolPlaceholder`. The list can grow between requests; in the
  capture, `TaskStop` appeared partway through the session (20 tools, then 21).
- **`messages`**: the whole conversation is resent every time. Request N
  contains every message from request N‑1, plus usually one assistant message
  (the previous reply) and one user message (tool results or a new prompt).
- **`metadata.user_id`**: a JSON-encoded string containing `device_id`,
  `account_uuid` and `session_id`.

Request headers of interest:

- `X-Claude-Code-Session-Id: <uuid>`: the same for all calls of one agent
  session, including side calls.
- `User-Agent: claude-cli/2.1.318 (external, cli)`
- `anthropic-beta: ...`: a long comma-separated list.
- `Authorization: Bearer sk-ant-...`: already shortened by the proxy to its
  first 16 characters plus `...`.

### 3.4 Content block types seen in `messages`

`content` is **either a plain string or an array of blocks**. The same message
can switch between the two from one request to the next. In the capture, a
user prompt was a one-element `[{"type":"text",...}]` array in one request and a
plain string in the next. `cache_control` markers also move between messages
from request to request.

| Where | Block | Notes |
|---|---|---|
| user | `{"type":"text","text":...}` | The user's prompt. The first user message holds several text blocks. Blocks whose text starts with `<system-reminder>` are context the agent added, not something the user typed. |
| user | `{"type":"tool_result","tool_use_id":"toolu_...","content":...,"is_error"?:true}` | `content` is a string **or** an array of blocks, e.g. `[{"type":"tool_reference","tool_name":"TaskStop"}]` or `[{"type":"text",...}]`. |
| user | text next to a tool_result | One user message may hold a `tool_result` followed by a `text` block (e.g. `"Tool loaded."`). |
| **system** | `{"role":"system","content":[...]}` | A system message in the middle of `messages`, not a top-level field. Contains a long `text` block (about 26 KB of agent context) and `{"type":"tool_addition","tool":{"type":"tool_definition","definition":{name, description, input_schema}}}` blocks, which are tools added during the session. |
| assistant | `{"type":"text","text":...}` | Model text. |
| assistant | `{"type":"thinking","thinking":"","signature":"<long base64>"}` | Thinking is usually **empty** (hidden) and only carries a signature. |
| assistant | `{"type":"tool_use","id":"toolu_...","name":"Bash","input":{...}}` | May also carry `"caller":{"type":"direct"}`. One assistant message can contain text, thinking and several tool_use blocks. |

The agent's tool results come back in the **next** request, as a `tool_result`
in a user message, with `tool_use_id` equal to the `id` of the `tool_use`.

Any block type not listed here must still be shown, as a generic grey block
with its JSON. It must never break the page.

### 3.5 Streamed replies (Server-Sent Events)

A successful reply is `200 OK` with an SSE body: lines `event: <name>` and
`data: <json>`, with a blank line between events. In the capture, **data lines
can have trailing spaces after the JSON** (e.g. `{"type":"ping"}    `). The
proxy forwards 4096-byte reads as they arrive, so **a chunk can end in the
middle of a line**.

The event sequence:

```
message_start        {"type":"message_start","message":{"id":"msg_...","model":"...","role":"assistant","content":[],"usage":{...}}}
content_block_start  {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}
                     | {"type":"tool_use","id":"toolu_...","name":"Read","input":{},"caller":{...}}
                     | {"type":"thinking","thinking":"","signature":""}
ping                 {"type":"ping"}
content_block_delta  {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"..."}}
                     | {"type":"input_json_delta","partial_json":"{\"file_"}   (tool input arrives as JSON fragments)
                     | {"type":"thinking_delta","thinking":"..."}
                     | {"type":"signature_delta","signature":"..."}
content_block_stop   {"type":"content_block_stop","index":0}
message_delta        {"type":"message_delta","delta":{"stop_reason":"end_turn"|"tool_use"|"max_tokens",...},"usage":{...}}
message_stop         {"type":"message_stop"}
```

More than one content block can be open at a time, so deltas must be matched
to their block by `index`. Tool input is the concatenation of all its
`partial_json` fragments, parsed at `content_block_stop`. The first fragment
can be `""`.

**Usage.** `message_start.message.usage` has `input_tokens`,
`cache_creation_input_tokens`, `cache_read_input_tokens`, `output_tokens`,
`cache_creation: {ephemeral_5m_input_tokens, ephemeral_1h_input_tokens}`,
`service_tier`, and so on. `message_delta.usage` repeats them with the **final**
`output_tokens`, plus `output_tokens_details.thinking_tokens`. Final usage is
the `message_start` values overwritten by the `message_delta` values.

Example: `input_tokens: 2, cache_creation_input_tokens: 30315,
cache_read_input_tokens: 0, output_tokens: 16`. The context size is the sum of
the three input fields.

### 3.6 Errors

An error is **not a stream** but a plain JSON body. For example, with status
`429 Too Many Requests`:

```
{"type":"error","error":{"type":"rate_limit_error","message":"Error"},"request_id":"req_..."}
```

An SSE stream can also contain `event: error` with the same `data` shape.
When the upstream cannot be reached, `proxy.py` itself replies with `502`.
A `HEAD` reply has no body.

## 4. How the dashboard gets its data (changes to `proxy.py`)

**Decision:** extend `proxy.py` so it records structured events and serves the
page on its own port. *Reason:* parsing the terminal output can't match replies
to requests (3.1), and adding a second proxy is more moving parts for no gain.

### 4.1 Local routes (never forwarded, never printed)

Paths starting with `/__` are handled by the proxy itself:

| Route | Response |
|---|---|
| `GET /__dashboard` | `dashboard.html` from the directory of `proxy.py` (`text/html; charset=utf-8`). |
| `GET /__events` | A Server-Sent Events stream (`text/event-stream`, `Cache-Control: no-cache`). |

Every other path goes through the existing `relay()`.

### 4.2 Exchange store and events

- Every relayed request gets an exchange id: an integer that starts at 1 and
  increases by one.
- The proxy keeps the last **500** exchanges in memory, oldest dropped first.
  Access to the store is protected by a lock, because handlers run in threads.

Each SSE message the proxy sends is `data: <one JSON object>\n\n`. Its `type`
is one of:

| type | fields | when |
|---|---|---|
| `reset` | none | First message on every new `/__events` connection, before the replay. |
| `request` | `id, t, method, path, headers, body, body_json` | After the request body is read, before forwarding. |
| `response_start` | `id, t, status, reason, headers` | When upstream response headers arrive (or the 502 is sent). |
| `response_chunk` | `id, t, text` | For every chunk written back to the agent. |
| `response_end` | `id, t, ok, error` | After the last chunk. `ok:false` and `error:"<message>"` if the upstream connection failed, the stream broke, or the agent disconnected. |

Field rules:

- `t` is milliseconds since the Unix epoch (float).
- `headers` is a list of `[name, value]` pairs. `Authorization` and `x-api-key`
  are shortened the same way as in the terminal (first 16 characters plus
  `...`).
- `body_json` is the parsed body, or `null` if the body is empty or not JSON.
  In that case `body` holds the text, otherwise `body` is `null`.
- `text` must be decoded with an **incremental** UTF‑8 decoder for each
  exchange (`codecs.getincrementaldecoder("utf-8")(errors="replace")`), so a
  multi-byte character split across chunks is not broken.

`response_end` must always be sent, even after an exception (use
`try/finally`). Without it, a row would show "pending" forever.

### 4.3 Replay and live updates

- When a page connects to `/__events`, the proxy sends `reset`. It then replays
  every stored exchange: `request`, `response_start` if there is one, **one**
  `response_chunk` holding all reply text so far, and `response_end` if the
  exchange is finished. After that it sends live events.
- Each connected page has its own `queue.Queue`. Publishing an event appends it
  to the store and puts it on every queue. A slow or closed page must never
  block or slow down relaying. If a queue is full (1000 or more items), that
  page is dropped.
- The `/__events` handler sends `: keepalive\n\n` every 15 s and exits quietly on
  `BrokenPipeError`/`ConnectionResetError`.
- The page uses `EventSource`, which reconnects automatically. On `reset` the
  page clears its state and rebuilds it from the replay, so a reload loses
  nothing.

The proxy only records and passes data on. **All interpretation (SSE parsing,
threads, pairing, diffs, stats) happens in the page.**

## 5. Understanding the data (in the page)

### 5.1 Exchange kinds and threads

Classify each exchange:

| Kind | Rule | Label |
|---|---|---|
| probe | method is not `POST`, or path does not start with `/v1/messages` | `probe` |
| probe | `max_tokens == 1` | `probe: quota` |
| side | `tools` missing or empty | `side: <name>`, where name is the first property of `output_config.format.schema.properties` if present (→ `side: title`), else `side` |
| main | non-empty `tools` | none |

Group exchanges into **threads**:

- Thread key = session id header + the text of the last `system` block (first
  300 characters) + the first user message normalized as in 5.2 (first 300
  characters).
- Thread names are assigned in order of first appearance: `A`, `B`, `C`, ...
  Each thread gets a color from a fixed palette (blue, teal, amber, pink,
  slate).
- Probes with no body go into no thread.

A subagent (the agent's `Agent` tool) therefore shows up as its own main thread.

### 5.2 Normalizing messages

To compare messages between requests:

- convert string `content` to `[{"type":"text","text": content}]`
- remove every `cache_control` key
- compare with `JSON.stringify`, with object keys sorted

### 5.3 "New this turn" and "changed since previous"

For a request R in thread T, P is the previous request in T.

- **New this turn** = `R.messages` from index L onward, where L is the length
  of the longest common prefix of the normalized `P.messages` and
  `R.messages`. With no P, the new messages are the last user message (and any
  system message right before it).
- If L is shorter than `P.messages`, the earlier history was rewritten (for
  example after compaction or cleared thinking). Show a warning: "history
  changed at message #L".
- **System:** "unchanged", or "changed" with a line-level diff against P on
  request.
- **Tools:** "unchanged (N)", or "+Added −Removed" with tool names. If only a
  description changed, show "~Name".
- **Params** (`model, max_tokens, thinking, output_config, context_management`):
  list the keys that changed.

### 5.4 Tool call ↔ result pairing

Keep one global index: `tool_use_id → {name, input, callExchangeId,
resultExchangeId, resultBlock}`.

- Entries are filled from three places:
  - every `tool_use` in a reply (5.5)
  - every `tool_use` in a request's messages
  - every `tool_result` in a request's messages, which sets `resultExchangeId`
    to the **first** request that contains that result
- Pairing is by id only, never by position.
- A call with no result yet shows **"waiting for result…"**. When the request
  carrying the result arrives, the waiting call updates in place (live).

### 5.5 Rebuilding the reply from the stream

For each exchange, buffer `response_chunk` text, split it into complete SSE
events (handle partial lines and trailing spaces), and apply each event:

- **`message_start`**: record message id, model and usage.
- **`content_block_start`**: create block `[index]`.
- **Deltas**:
  - `text_delta` → append the text
  - `thinking_delta` → append the thinking
  - `signature_delta` → mark the block as signed
  - `input_json_delta` → append to a raw string
- **`content_block_stop`**: parse the tool input JSON. If parsing fails, keep
  the raw string and show a warning.
- **`message_delta`**: record `stop_reason` and merge usage.
- **`message_stop`**: mark the reply complete.
- **`error`**: mark the exchange as an error.
- **`ping`**: ignore.

If the reply is not SSE, try to parse it as JSON: `type:"error"` → error;
`type:"message"` → render its `content`. Otherwise show it as text. Replies
rebuild **as they stream**: text grows and tool cards appear live.

## 6. Layout and display

### 6.1 Overall

Two panes:

- **Top bar** (sticky):
  - live/reconnecting dot
  - totals: requests, **errors**, **tool errors**, input tokens (fresh / cache
    write / cache read), output tokens, tool calls. The two error counts follow
    the levels in 6.5 and are never added together.
  - **Tool filter** dropdown: "all tools" or one tool name, built from every
    tool name seen
  - **Errors only** toggle
  - **Hide side calls** toggle (off by default)
  - **Follow newest** toggle (on by default)
- **Left pane (~38% width): exchange list**, newest first, one row per
  exchange.
- **Right pane: details** of the selected exchange. When nothing is selected,
  it shows the newest main request.

### 6.2 Exchange row (left pane)

One compact row of 2 lines:

- **Line 1:**
  - `#id`
  - time (`HH:MM:SS`)
  - thread chip (letter, thread color)
  - kind label
  - status badge: spinner while pending, green `200`, red `4xx/5xx/!`
  - model
  - duration
- **Line 2**, a summary:
  - `+N msgs` (new this turn)
  - then `→` and the reply summary: tool chips (`Bash`, `Read ×2`) and/or the
    first ~70 characters of the reply text
  - output tokens

Display states:

- Side and probe rows: opacity 0.55, with their label. Hidden when **Hide side
  calls** is on.
- Error rows (level 1 in 6.5): a red 4px left border and a red badge.
- Tool-error rows (level 2): an amber 4px left border and an amber
  `tool error` chip on line 2. The status badge stays green `200`.
- Probe rows keep their status badge color (a quota probe shows a red `429`)
  but get no red border, because that reply is expected.
- Exchanges still streaming: a pulsing badge.

Scrolling:

- If the user has scrolled down, the list does not jump when new rows arrive.
  A pill "↑ N new" appears instead and scrolls to the top when clicked.
- With **Follow newest** on and the list at the top, the newest *main* exchange
  is selected automatically. Clicking a row turns Follow off.

### 6.3 Detail pane (right), top to bottom

Each section is a collapsible panel with a header showing a count or a diff
badge.

1. **Summary line**:
   - id, thread, kind
   - status, model, `stop_reason`
   - timing: total, time to first byte (`response_start − request`), stream
     time
   - tokens: input / cache write / cache read / output / context size
2. **New this turn** (open): the new messages from 5.3, rendered as message
   cards (6.4).
3. **Reply** (open): the rebuilt reply, as an assistant card.
   - Every tool call shows its paired result underneath (5.4), or "waiting for
     result…", plus a link "result in #N" that selects that exchange.
   - For errors: the error type, the message and the `request_id`, in red.
4. **Conversation (N messages)** (open): the full `messages` thread in order,
   as cards.
   - Messages that were already in the previous request are collapsed into
     groups: "▸ 12 earlier messages", which expands.
   - Each `tool_use` card has its result attached directly under it. The
     `tool_result` in its own (later) user message becomes a one-line stub:
     "↳ result of Bash, shown with its call".
5. **System instructions** (collapsed, header shows block count, total
   characters and changed/unchanged): every system block **in full**, one
   panel each, with its own character count and a `cache` tag if it has
   `cache_control`. A mid-conversation `role:"system"` message appears in
   Conversation, not here.
6. **Tools (N)** (collapsed, header shows the diff badge): one line per tool
   with the name, the first line of its description, and a `deferred` tag.
   Expanding a tool shows the full description and `input_schema` as a JSON
   tree.
7. **Request parameters** (collapsed): everything except
   `messages/system/tools`, as a JSON tree, with the keys changed since the
   previous request highlighted.
8. **Headers** (collapsed): request and response headers.
9. **Raw** (collapsed): the raw request JSON tree and the raw response text.

### 6.4 Message cards — telling things apart at a glance

Every block is a card with a colored left bar (4px), a small uppercase label and
an icon character. The colors and labels are fixed:

| Content | Label | Color (bar / light background) |
|---|---|---|
| user text typed by the user | `YOU` | `#2563eb` / `#eff6ff` |
| user text starting with `<system-reminder>` | `INJECTED CONTEXT` | `#9ca3af` / `#f9fafb`, collapsed to one line by default |
| `role:"system"` message text | `SYSTEM (MID-CONVERSATION)` | `#6b7280` / `#f3f4f6`, collapsed |
| `tool_addition` | `+ TOOL ADDED: <name>` | `#6b7280`, one line, expands to the definition |
| assistant text | `MODEL` | `#16a34a` / `#f0fdf4` |
| thinking | `THINKING` | `#9333ea` / `#faf5ff`, italic. If empty: "hidden (signature only)" |
| tool_use | `TOOL CALL · <Name>` | `#ea580c` / `#fff7ed` |
| tool_result | `RESULT · <Name>` | `#0d9488` / `#f0fdfa` |
| tool_result with `is_error` | `ERROR RESULT · <Name>` | `#dc2626` / `#fef2f2` |
| tool_reference | `LOADED TOOL: <tool_name>` | inside the result card |
| unknown type | `<type>` | `#9ca3af`, JSON tree |

Inside each card:

- **Tool call card:**
  - a one-line summary of the input in monospace: `Bash` → `command`;
    `Read`/`Edit`/`Write` → `file_path`; `Grep`/`Glob` → `pattern`; otherwise
    the first string field
  - the optional `description` field in grey
  - the full input as a JSON tree under "input"
- **Result card:** monospace, `white-space: pre-wrap`, and the result length.
  An array result shows each part.
- **Long text** (more than 15 lines or 1200 characters) is clamped, with
  "show all (N lines, M chars)". It also has a **copy** button.
- **JSON tree:**
  - collapsible objects and arrays, open to depth 2
  - key count or item count shown when collapsed
  - long strings clamped like long text
  - keys in a muted color; strings, numbers and booleans in distinct colors

Message order and role are always visible. Each message gets a small header:
`#index · user|assistant|system`. Assistant messages are indented 24px from the
left, user messages are not. This makes the back-and-forth easy to scan.

### 6.5 Filters

- **Tool filter X:** show only exchanges whose reply calls X, or whose new
  messages contain a result of X. In the detail pane, cards for X get a thin
  outline.
- **Errors only:** shows exchanges at any of these three levels. They are
  different things and must look different:
  1. **Error (red), counted in "errors":** the exchange itself failed. Status
     ≥ 400, `response_end.ok == false`, a stream with no `message_stop`, or an
     SSE `error` event. **Probes are never counted**: the agent sends them as
     side calls and a `429` on a quota probe is normal (in a real capture, the
     only "error" of a healthy session was that probe).
  2. **Tool error (amber), counted in "tool errors":** the API call succeeded
     (`200`), but its new messages contain an `is_error` tool result, i.e. a
     tool the agent ran failed (a command exited non-zero, a permission was
     denied). This is the agent's work failing, not the traffic. Count each
     `tool_use_id` once, in the first request that carries its result.
  3. **Warning (amber, not counted):** `stop_reason == "max_tokens"`.

## 7. Decisions

| # | Decision | Reason |
|---|---|---|
| 1 | Extend `proxy.py`: exchange ids, in-memory store, SSE `/__events`, page at `/__dashboard`, same port. | Replies mix together in the terminal (3.1); only the proxy knows which reply belongs to which request. |
| 2 | One `dashboard.html`, plain JS, no outside dependencies, served from disk. | Nothing for a fresh build to get wrong; works offline; reload shows edits. |
| 3 | Two panes: exchange list (newest first) + sectioned detail. | Scan many exchanges quickly, then go deep on one without losing your place. |
| 4 | Pair calls and results by `tool_use_id` across requests; update live. | The result arrives in a later request (3.4); position-based pairing breaks with parallel calls. |
| 5 | "New this turn" = messages after the longest common normalized prefix with the previous request in the same thread. | Each request resends everything (3.3); this shows only what's new and also handles string/array switches and moving `cache_control`. |
| 6 | Group into threads. Side calls and probes are shown by default, dimmed and labelled; a toggle hides them. | They are real traffic (3.2) but shouldn't distract from the main conversation. |
| 7 | Proxy keeps the last 500 exchanges and replays them on connect. | Reloading or opening the page late loses nothing. |
| 8 | First version: errors highlighted, tool filter, tokens and timing, totals bar, changed-since-previous. **No cost.** | Most useful with no pricing data to maintain. |
| 9 | Credentials shortened as in the terminal; listen on 127.0.0.1 only; `metadata.user_id` only in Raw/Params. | The page shows a live view of a logged-in session. |
| 10 | `--demo` flag replays a built-in sample matching section 3. | The build folder has no traffic.log and no guaranteed API access; the acceptance checks must still be doable. |
| 11 | All data is added to the page with `textContent` / `createElement`, **never `innerHTML` with data**. | Tool results contain arbitrary HTML and code; it must show as text, never run. |
| 12 | Detail sections are built only for the selected exchange; list rows are cheap. | Each main request is hundreds of KB (system + tools + full conversation); building everything would freeze the page. |

## 8. Non-goals

- Cost estimates.
- Saving traffic to disk.
- Editing or replaying requests.
- Authentication.
- Supporting other API formats. `--upstream` for Codex still forwards
  correctly; the page shows those bodies only as raw JSON or text.

## 9. Demo mode (`--demo`)

`python3 proxy.py --demo` starts the normal proxy and dashboard. A background
thread then publishes a built-in sample through **the same event functions**
as real traffic, so the page can't tell the difference. No network access is
needed.

- Requests are spaced about 1.5 s apart. Each reply streams as SSE chunks every
  40–80 ms. Some chunks are cut in the middle of a line, and some data lines
  have trailing spaces.
- After the sample ends, it waits 10 s and loops with new ids, so the "live"
  behavior can be checked at any time.
- The sample data is written as Python dicts and lists inside `proxy.py`.

The sample must include, using the shapes in section 3:

1. A `HEAD /api/hello` probe (200, no body).
2. A quota probe (`max_tokens: 1`, `"quota"`) answered `429` with the
   rate_limit_error JSON. It runs **at the same time as** a title-generation
   side call (`tools: []`, `output_config.format` with `title`) that streams
   `{"title":"Proxy summary"}`. The two replies' chunks are interleaved.
3. A main thread (same `X-Claude-Code-Session-Id`, about 20 tools including one
   `defer_loading` tool and `DeferredToolPlaceholder`, 3 system blocks with
   `cache_control` on 2 of them) of at least 6 requests:
   1. First user message: two `<system-reminder>` text blocks and the user's
      prompt "Read proxy.py and tell me what it does". It is followed by a
      `role:"system"` message with a long text block and two `tool_addition`
      blocks. The reply has an empty signed thinking block and a `Read`
      tool_use whose input streams as `input_json_delta` fragments
      (first fragment `""`).
   2. The `Read` tool_result (string content). The reply is model text.
   3. A new user prompt given as a **plain string**, after the previous request
      had it as an array. The reply holds **two parallel** `Bash` tool_use
      blocks.
   4. Both results: one normal, and one with `is_error: true`. The reply is
      text plus a `ToolSearch` call.
   5. A tool_result whose content is `[{"type":"tool_reference","tool_name":"TaskStop"}]`
      followed by a `text` block `"Tool loaded."`. `TaskStop` is **added** to
      `tools`. The reply is a final text answer with `stop_reason:"end_turn"`.
   6. A request whose reply stream stops without `message_stop`, ending with
      `response_end ok:false`.
4. One main request (with `tools`) to an unreachable upstream (`502`, `ok:false`).
5. Realistic usage numbers (e.g. `cache_creation_input_tokens: 30315`, then
   cache reads on later turns), real-looking `toolu_`/`msg_`/`req_` ids, and
   one tool result longer than 100 lines. One tool result must contain the
   literal text `<b>hi</b><script>alert(1)</script>`, for check 15.

## 10. Acceptance checks (all doable by looking at the page)

Run `python3 proxy.py --demo` and open `http://localhost:8041/__dashboard`:

1. Rows appear one by one with no reload, newest at the top. A pending row
   shows a spinner that becomes a status badge.
2. The quota probe (red `429`), the HEAD probe and the title call are dimmed
   and labelled (`probe: quota`, `probe`, `side: title`). **Hide side calls**
   removes them, and turning it off brings them back.
3. Even though the 429 and the title replies were interleaved, each reply is
   shown under the right request: the 429 under the quota probe, and
   `{"title":...}` under the title call.
4. In a main request, **YOU**, **MODEL**, **TOOL CALL**, **RESULT** and
   **INJECTED CONTEXT** cards each have their own color and label, so they can
   be told apart without reading the text.
5. Selecting request 1 of the main thread: the Reply shows a `Read` tool call
   saying "waiting for result…" until request 2 arrives. Then the result
   appears under the call with a working "result in #N" link.
6. Request 3's reply shows two `Bash` calls, each with its own result
   underneath. The failing call's card shows its result in red
   (`ERROR RESULT · Bash`). **Errors only** shows request 4 of the main thread,
   the request that carries the `is_error` result, with an amber border and a
   `tool error` chip, not a red border.
7. "New this turn" shows only the new messages (e.g. `+2 msgs`), not the whole
   history. The string/array switch in request 3 does **not** cause a "history
   changed" warning.
8. The Tools header on request 5 shows `+TaskStop`. System says "unchanged".
   Expanding System shows every block in full, with character counts.
9. The tool result over 100 lines is clamped with "show all"; expanding and
   collapsing works. Tool inputs and Raw show as collapsible JSON trees.
10. The summary line shows input / cache write / cache read / output tokens and
    durations. The top bar totals grow as exchanges arrive.
11. The broken stream (request 6) and the 502 are marked as errors. No row
    stays "pending" forever.
12. After one pass of the demo the top bar shows **2 errors** (the broken stream
    and the 502) and **1 tool error** (main request 4). The quota probe's `429`
    is red on its own row but is not counted and has no red border.
13. The tool filter set to `Bash` shows only exchanges involving Bash.
14. Reloading the page brings back all exchanges so far. Stopping the proxy
    shows "reconnecting", and restarting it reconnects by itself.
15. A tool result containing `<b>hi</b><script>alert(1)</script>` shows as
    literal text. No alert appears and nothing turns bold.

With a real agent (`ANTHROPIC_BASE_URL=http://localhost:8041 claude`, then ask
it to read a file):

16. The agent works exactly as without the dashboard. The terminal still prints
    the same output, and `/__` requests are not printed.
17. The real session shows up as main thread `A`, with its title call and
    probes as side rows. The `Read` call pairs with its result.
