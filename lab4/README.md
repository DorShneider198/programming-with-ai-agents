# Lab 4: Spec-Build an Agent Traffic Monitor

We build a live dashboard of an agent's traffic from `SPEC.md` alone, then
improve the spec and rebuild, again and again.

## Layout

```
SPEC.md        the spec: the only file we edit between builds
proxy.py       the original proxy, never edited (each build starts from it)
builds/NN/     one folder per build from scratch (SPEC.md + proxy.py + what the agent made)
spec-history.md  written at the end from `git log --follow -p SPEC.md`
```

## The loop

1. **Build** in a fresh folder, with a fresh agent session:
   ```
   mkdir builds/01 && cp SPEC.md proxy.py builds/01/ && cd builds/01
   ANTHROPIC_BASE_URL=http://localhost:<previous build's port> claude -p "Build the dashboard described in SPEC.md" \
     --permission-mode acceptEdits --allowedTools "Bash(python3:*)" "Bash(curl:*)" "Bash(osascript:*)" "Bash(kill:*)"
   ```
   Without `--allowedTools`, a non-interactive build can write files but cannot run
   anything, so it cannot test its own code (this happened to build 01).
2. **Run it on live traffic**, on its own port so it doesn't clash with the previous build:
   ```
   python3 proxy.py --port 8042            # or: python3 proxy.py --port 8042 --demo
   ANTHROPIC_BASE_URL=http://localhost:8042 claude
   open http://localhost:8042/__dashboard
   ```
3. **Improve `SPEC.md`** (the one at the top of `lab4/`, not the copy inside the build),
   then commit it with what the last build showed:
   ```
   git add SPEC.md builds/01 && git commit -m "Lab 4 spec: <what build 01 showed and what we changed>"
   ```

While a build runs, point the building agent at the previous build's proxy
(`ANTHROPIC_BASE_URL=http://localhost:8041 claude ...`) and watch the traffic live.

If we switch from rebuilding to editing an existing build, commit first and
say so in the commit message.

## Notes on the traffic

Add notes here as we go; they become "Findings in the traffic" in `spec-history.md`.

-
