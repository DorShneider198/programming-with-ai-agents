# Spec history: Live Agent Traffic Dashboard

From `git log --follow -p lab4/SPEC.md`, oldest first. **Source** says where each reason came from. The spec grew from 618 lines and 16 checks to 726 lines and 21 checks.

| # | Commit | What changed | Reason (what the build before it showed) · **Source** |
|---|---|---|---|
| 1 | `4039757` first draft | The whole spec: `proxy.py` keeps exchange ids, the last 500 exchanges and an SSE `/__events` stream; the page parses the stream, groups threads, shows "new this turn", pairs calls and results by id; two panes, colored cards, `--demo`, 16 checks. | No build before it: written as prep, from a real capture. **Source:** lab handout; next commit `9538bb8` built the prep dashboard from it. |
| 2 | `70b0486` repo set-up | No text change; moved to `lab4/SPEC.md`. | One spec for every build, apart from build output. **Source:** commit message. |
| 3 | `89ef888` errors vs tool errors | Three levels: error (red, counted, never probes), tool error (amber, counted apart), `max_tokens` warning. Check: demo shows 2 + 1. | Prep build on live traffic said **3 errors** though every model call succeeded: an expected quota-probe `429` and two `200`s carrying failed tool results. **Source:** commit message; the build's `/__events` replay. |
| 4 | `1cdbc05` suggestion calls | New `side: suggestion` (last user text starts `[SUGGESTION MODE:`); side calls never serve as "previous request". | Prep build showed main-thread "replies" like "push the changes": prompt predictions with full tools, so they counted as main and broke the next turn's diff. **Source:** commit message; request bodies #24/#25. |
| 5 | `9df2dde` one-line tool summary | Summary is exactly one line (`⏎`, ellipsis) + `full command`; `description` in the header. | A `Bash` heredoc made the "one-line" summary 6 lines and pushed the result off screen. **Source:** commit message; user's screenshot. |
| 6 | `e26de5c` thread chips | Chips per thread that filter the list; filters and Follow combine; demo gets a 2nd session. | Three sessions (ours + two builds) shared one proxy, rows interleaved, no way to see one. **Source:** commit message; replay showed 3 session ids. |
| 7 | `a1b8bf4` favicon | `/favicon.ico` → `204`, never forwarded, printed or stored. | **Build 01** relayed the browser's favicon request to the API; a `404` row appeared that the agent never made. Build 02 did the same. **Source:** commit message; `builds/01/server.log`. |
| 8 | `e557217` real checks without demo | Real-agent checks run on a proxy restarted without `--demo`. | In build 01 the demo session took letter `A`, so "real session is `A`" could not pass. **Source:** commit message; our check of build 01. |
| 9 | `ec2104c` string/array switch | Request 3's prompt is a string, request 4 resends it as an array; check 7 points at request 4. | Build 01 said it "couldn't fit an actual switch into request 3": the spec asked for something impossible. **Source:** commit message; `builds/01/build.log`. |
| 10 | `6c7629b` builds must self-test | Required test before finishing (compile, demo on :8099, curl page and events, parse scripts); must say what ran. | Builds 01 and 02 could not run one command and reported checks "by reading the code". **Source:** commit message; both build logs. Loop change too: builds now get `--allowedTools` for `python3`/`curl`. |
| 11 | `9a82575` pin two gaps | Letters for main threads only; text beside a `tool_result` is `INJECTED CONTEXT`. | Prep build, build 01 and build 02 each made both choices on their own. **Source:** commit message; the three builds' reports. |

**Builds.** Prep build (`9538bb8`) drove changes 3–6. **01** and **02** (spec 3) passed everything we could check outside the page (page `200`, scripts parse, no `innerHTML` with data, correct demo events, a real `claude -p` read a file through each with terminal output unchanged) and drove 7–11. **03** (spec 6) and **04** (spec 11, first build allowed to test itself) were still running at the time of writing.

**Rebuild → edit switch:** none. Every build started from scratch from `SPEC.md` and the original `proxy.py`.

## Findings in the traffic

From the proxy's replay (93 exchanges, 4 sessions) and our session; `README.md` had no notes of its own.

- **Hidden traffic:** about 1 in 9 requests (10/93) was a prompt-suggestion call guessing the user's next message; the real conversation never includes it.
- **Side calls are normal, some fail on purpose:** `HEAD /api/hello`, a `max_tokens: 1` quota probe answered `429`, and a title call (`tools: []`) returning `{"title": …}`.
- **Everything is resent every turn** (system, ~20 tools, whole conversation), but ~95% of input tokens were cache reads (≈8.1M read, ≈0.37M written, ≈0.06M fresh).
- **Thinking is hidden:** thinking blocks are empty and only carry a signature.
- **`safeguards`** on every main request carries the permission classifier's context (mode, cwd, allow rules); a denied action comes back as an `is_error` tool result.
- **Browsers leak in:** opening the dashboard sent `/favicon.ico` through the proxy to the API.
