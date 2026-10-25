// Drum Timing Trainer
// Metronome clicks are scheduled on the AudioContext clock, and the onset
// processor reports mic hits on that same clock. Calibration plays clicks through
// the speakers and times when the mic hears them, which gives the exact
// speaker-to-mic delay to subtract from every hit.

const $ = (id) => document.getElementById(id);

const ui = {
  bpm: $("bpm"),
  bpmRange: $("bpm-range"),
  sessionLength: $("session-length"),
  sessionCustom: $("session-custom"),
  customWrap: $("custom-wrap"),
  countdown: $("countdown"),
  summary: $("summary"),
  sumSd: $("sum-sd"),
  sumSub: $("sum-sub"),
  sumCompare: $("sum-compare"),
  sumSources: $("sum-sources"),
  newSession: $("new-session"),
  threshold: $("threshold"),
  thresholdValue: $("threshold-value"),
  meterFill: $("meter-fill"),
  meterThreshold: $("meter-threshold"),
  mic: $("mic"),
  micWarning: $("mic-warning"),
  latency: $("latency"),
  calibrate: $("calibrate"),
  start: $("start"),
  pause: $("pause"),
  reset: $("reset"),
  status: $("status"),
  lastOffset: $("last-offset"),
  lastLabel: $("last-label"),
  statCount: $("stat-count"),
  statMean: $("stat-mean"),
  statSd: $("stat-sd"),
  chart: $("chart"),
  tooltip: $("tooltip"),
  tableBody: $("table-body"),
};

// ---------- Audio setup ----------

let ctx = null;
let onsetNode = null;
let micStream = null;
let micSource = null;

async function ensureAudio() {
  if (ctx) {
    await ctx.resume();
    return;
  }
  ctx = new AudioContext({ latencyHint: "interactive" });
  await ctx.audioWorklet.addModule("onset-processor.js");
  onsetNode = new AudioWorkletNode(ctx, "onset-processor");
  onsetNode.port.onmessage = onWorkletMessage;
  onsetNode.port.postMessage({ threshold: Number(ui.threshold.value) });

  // Keep the processor running without sending the mic to the speakers.
  const mute = ctx.createGain();
  mute.gain.value = 0;
  onsetNode.connect(mute).connect(ctx.destination);

  // Device names are only visible after mic permission, so open the default
  // mic first, then switch if a better one exists.
  await openMic();
  const preferred = await preferredMicId();
  if (preferred && preferred !== currentMicId()) await openMic(preferred);
  await fillMicList();
  navigator.mediaDevices.addEventListener("devicechange", onDeviceChange);
}

// Turn off browser voice processing: it smears and delays drum transients.
const MIC_PROCESSING = { echoCancellation: false, noiseSuppression: false, autoGainControl: false };

async function openMic(deviceId) {
  if (micSource) micSource.disconnect();
  if (micStream) micStream.getTracks().forEach((t) => t.stop());
  const audio = deviceId ? { ...MIC_PROCESSING, deviceId: { exact: deviceId } } : MIC_PROCESSING;
  micStream = await navigator.mediaDevices.getUserMedia({ audio });
  micSource = ctx.createMediaStreamSource(micStream);
  micSource.connect(onsetNode);
  invalidateCalibration(); // a different mic has a different delay
  updateMicWarning();
}

function currentMicId() {
  return micStream && micStream.getAudioTracks()[0].getSettings().deviceId;
}

// A Bluetooth headset mic puts the headphones into low-quality "call" mode,
// which adds delay and makes drums quiet, so prefer any other mic.
const BLUETOOTH = /bluetooth|airpods|hands-free|headset/i;

async function listMics() {
  const devices = await navigator.mediaDevices.enumerateDevices();
  // Chrome adds "default" and "communications" aliases that duplicate real devices.
  return devices.filter(
    (d) => d.kind === "audioinput" && d.deviceId !== "default" && d.deviceId !== "communications"
  );
}

async function preferredMicId() {
  const mics = await listMics();
  const builtIn = mics.find((m) => /built-in|macbook/i.test(m.label));
  const wired = mics.find((m) => !BLUETOOTH.test(m.label));
  return (builtIn || wired || {}).deviceId;
}

async function fillMicList() {
  const mics = await listMics();
  ui.mic.innerHTML = "";
  for (const m of mics) {
    const opt = document.createElement("option");
    opt.value = m.deviceId;
    opt.textContent = m.label || "Microphone";
    ui.mic.append(opt);
  }
  ui.mic.value = currentMicId();
}

function updateMicWarning() {
  const label = micStream.getAudioTracks()[0].label;
  ui.micWarning.textContent = BLUETOOTH.test(label)
    ? "Bluetooth mic: adds delay and picks up drums poorly. Pick the built-in mic if you can."
    : "";
}

async function onDeviceChange() {
  await fillMicList();
  // A device plugged in or out may change the audio path.
  if (!calibrating) invalidateCalibration();
}

function onWorkletMessage(e) {
  const msg = e.data;
  if (msg.type === "level") {
    ui.meterFill.style.width = Math.min(100, msg.peak * 100) + "%";
  } else if (msg.type === "onset") {
    handleHit(msg.time);
  } else if (msg.type === "click") {
    calResults.push(msg);
  }
}

// ---------- Metronome ----------

const LOOKAHEAD = 0.1; // schedule clicks this many seconds ahead
const TICK_MS = 25;

let running = false;
let schedulerTimer = null;
let nextClickTime = 0;
let beat = 0;
let bpm = 90;
let clickTimes = [];

// A high, pure tone with a soft 2 ms start: it has almost no energy in the low
// band where hits are detected (a hard start would add a low-frequency thump).
function playClick(time, accent) {
  const osc = ctx.createOscillator();
  const gain = ctx.createGain();
  osc.frequency.value = accent ? 2000 : 1500;
  gain.gain.setValueAtTime(0, time);
  gain.gain.linearRampToValueAtTime(0.6, time + 0.002);
  gain.gain.exponentialRampToValueAtTime(0.001, time + 0.04);
  osc.connect(gain).connect(ctx.destination);
  osc.start(time);
  osc.stop(time + 0.05);
}

function scheduler() {
  while (nextClickTime < ctx.currentTime + LOOKAHEAD) {
    if (calibrating) {
      if (beat >= CAL_CLICKS) return;
      onsetNode.port.postMessage({ click: nextClickTime });
    }
    playClick(nextClickTime, beat % 4 === 0);
    clickTimes.push(nextClickTime);
    if (clickTimes.length > 1000) clickTimes.shift();
    nextClickTime += 60 / bpm;
    beat++;
  }
}

function startMetronome(tempo) {
  bpm = tempo;
  clickTimes = [];
  beat = 0;
  nextClickTime = ctx.currentTime + 0.2;
  running = true;
  scheduler();
  schedulerTimer = setInterval(scheduler, TICK_MS);
}

function stopMetronome() {
  running = false;
  clearInterval(schedulerTimer);
}

function nearestClick(time) {
  let best = null;
  for (let i = clickTimes.length - 1; i >= 0; i--) {
    const d = Math.abs(time - clickTimes[i]);
    if (best === null || d < Math.abs(time - best)) best = clickTimes[i];
    else if (clickTimes[i] < time) break; // times only get farther from here
  }
  return best;
}

// ---------- Calibration ----------
// Plays clicks through the speakers while the mic listens. The processor reports
// when each click arrived (giving the speaker-to-mic delay) and how loud it was
// in the hit band. The sensitivity is then kept safely above that level, so the
// click can never count as a hit.

const CAL_CLICKS = 8;
const MIN_CLICK_PEAK = 0.02; // quieter than this, the mic didn't really hear the click
const LEAK_MARGIN = 3;       // hits must be at least 3× louder than the click's leak

let latency = null; // seconds, speaker → mic; null until calibrated
let calibrating = false;
let calResults = [];

async function calibrate() {
  try {
    await ensureAudio();
  } catch (err) {
    return showAudioError(err);
  }
  pause();
  calibrating = true;
  calResults = [];
  ui.calibrate.disabled = true;
  ui.start.disabled = true;
  ui.status.textContent = "Calibrating: stay quiet for about 9 seconds…";
  onsetNode.port.postMessage({ calibrating: true });
  startMetronome(60);
  // Clicks start 0.2 s in, 1 s apart; each is examined for 0.5 s after it plays.
  setTimeout(finishCalibration, (CAL_CLICKS + 1.2) * 1000);
}

function finishCalibration() {
  stopMetronome();
  calibrating = false;
  onsetNode.port.postMessage({ calibrating: false });
  ui.calibrate.disabled = false;

  const heard = calResults.filter((r) => r.peak >= MIN_CLICK_PEAK);
  if (heard.length < CAL_CLICKS - 2) {
    ui.status.textContent =
      `The mic heard only ${heard.length} of ${CAL_CLICKS} clicks. Turn the volume up and calibrate again.`;
    ui.start.disabled = latency === null;
    return;
  }

  const delays = heard.map((r) => r.arrival - r.click).sort((a, b) => a - b);
  latency = delays[Math.floor(delays.length / 2)];
  const spreadMs = (delays[delays.length - 1] - delays[0]) * 1000;

  const leak = Math.max(...calResults.map((r) => r.leak));
  const floor = Math.min(0.8, Math.max(0.01, leak * LEAK_MARGIN));
  ui.threshold.min = floor;
  if (Number(ui.threshold.value) < floor) ui.threshold.value = floor;
  updateThreshold();

  ui.latency.textContent = `${Math.round(latency * 1000)} ms speaker → mic (varied by ${spreadMs.toFixed(1)} ms)`;
  ui.start.disabled = false;
  ui.status.textContent =
    floor > 0.4
      ? "Calibrated, but the click is very loud in the mic, so only hard hits will count. Turn the volume down a little and calibrate again."
      : "Calibrated. Keep the volume where it is (calibrate again if you change it), then press Start.";
}

function invalidateCalibration() {
  latency = null;
  pause();
  ui.start.disabled = true;
  ui.threshold.min = 0.01;
  ui.latency.textContent = "Not calibrated yet";
}

// ---------- Hits ----------

let offsets = [];        // ms, delay-corrected; negative = early
let ignored = 0;         // sounds that weren't scored (see handleHit)
let lastScoredClick = null;

function handleHit(time) {
  if (!running || calibrating || latency === null || clickTimes.length === 0) return;

  const t = time - latency;
  const click = nearestClick(t);
  const offset = (t - click) * 1000;
  // Score only sounds close to a click, and at most one per click, so a
  // single hit can never become two points.
  if (Math.abs(offset) > 60000 / bpm / 4 || click === lastScoredClick) {
    ignored++;
    ui.status.textContent = `Listening… (${ignored} extra sound${ignored === 1 ? "" : "s"} ignored)`;
    return;
  }
  lastScoredClick = click;
  offsets.push(offset);
  render();
}

// ---------- Stats & display ----------

function mean(xs) {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function stdDev(xs) {
  const m = mean(xs);
  return Math.sqrt(xs.reduce((a, x) => a + (x - m) ** 2, 0) / xs.length);
}

function fmt(ms) {
  const r = Math.round(ms);
  return (r > 0 ? "+" : "") + r + " ms";
}

function describe(ms) {
  const r = Math.round(ms);
  if (r === 0) return "right on the click";
  return r < 0 ? "early" : "late";
}

function render() {
  const n = offsets.length;
  ui.statCount.textContent = n;

  if (n === 0) {
    ui.lastOffset.textContent = "–";
    ui.lastLabel.textContent =
      latency === null ? "Press Calibrate first (about 9 seconds)" : "Press Start and play along";
    ui.statMean.textContent = "–";
    ui.statSd.textContent = "–";
    ui.tableBody.innerHTML = "";
  } else {
    const last = offsets[n - 1];
    ui.lastOffset.textContent = fmt(last);
    ui.lastLabel.textContent = describe(last);
    const m = mean(offsets);
    ui.statMean.textContent = `${fmt(m)} (${describe(m)})`;
    ui.statSd.textContent = n > 1 ? `±${Math.round(stdDev(offsets))} ms` : "–";

    const row = document.createElement("tr");
    row.innerHTML = `<td>${n}</td><td>${Math.round(last)}</td>`;
    ui.tableBody.prepend(row);
  }
  drawChart();
}

function clearResults() {
  offsets = [];
  ignored = 0;
  lastScoredClick = null;
  render();
}

// ---------- Timed session ----------

let paused = false;    // a session is in progress but paused
let sessionLength = 0; // seconds
let remainingMs = 0;
let countdownTimer = null;
let lastTick = 0;

function chosenSessionLength() {
  if (ui.sessionLength.value !== "custom") return Number(ui.sessionLength.value);
  return Math.min(600, Math.max(5, Math.round(Number(ui.sessionCustom.value) || 60)));
}

function showCountdown() {
  const s = Math.ceil(remainingMs / 1000);
  ui.countdown.textContent = `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

function startCountdown() {
  lastTick = performance.now();
  showCountdown();
  countdownTimer = setInterval(() => {
    const now = performance.now();
    remainingMs = Math.max(0, remainingMs - (now - lastTick));
    lastTick = now;
    showCountdown();
    if (remainingMs === 0) endSession();
  }, 100);
}

function stopCountdown() {
  clearInterval(countdownTimer);
}

// Tempo and length stay fixed for the whole session, so the summary describes one setting.
function setSessionControlsDisabled(disabled) {
  for (const el of [ui.bpm, ui.bpmRange, ui.sessionLength, ui.sessionCustom]) el.disabled = disabled;
}

function endSession() {
  stopMetronome();
  stopCountdown();
  paused = false;
  ui.start.textContent = "Start";
  ui.start.disabled = latency === null;
  ui.pause.disabled = true;
  ui.calibrate.disabled = false;
  setSessionControlsDisabled(false);
  ui.status.textContent = "Session finished.";
  showSummary();
}

function resetSession() {
  stopMetronome();
  stopCountdown();
  paused = false;
  clearResults();
  ui.countdown.textContent = "";
  ui.summary.hidden = true;
  ui.start.textContent = "Start";
  ui.start.disabled = latency === null;
  ui.pause.disabled = true;
  ui.calibrate.disabled = false;
  setSessionControlsDisabled(false);
  ui.status.textContent = "";
}

// ---------- Session summary ----------
// Reference levels from published research, compared on consistency (the SD of
// each hit's offset from the click). Only levels with verified published values
// of this same measure are listed; others are left out rather than estimated.

// Studies give the spread as a percentage of the beat, so it's converted to ms
// at the session's tempo, using the nearest tempo each study tested.
const REFERENCES = [
  {
    level: "Professional drummer",
    // Fujii et al. (2011), p. 500: the SD of synchronization error as a percentage
    // of the beat interval "was around 2% in the 60 and 120 bpm conditions,
    // whereas it was around 4% at the 200 bpm tempo".
    pctAtTempo: { 60: 2, 120: 2, 200: 4 },
    about:
      "15 professional drummers playing a drum kit with a metronome. Measure: standard " +
      "deviation of each hit's offset from the click, the same as this app's spread.",
    citation:
      "Fujii, S., Hirashima, M., Kudo, K., Ohtsuki, T., Nakamura, Y., & Oda, S. (2011). " +
      "Synchronization error of drum kit playing with a metronome at different tempi by " +
      "professional drummers. Music Perception, 28(5), 491–503.",
    url: "https://doi.org/10.1525/mp.2011.28.5.491",
  },
];

const MIN_HITS_FOR_COMPARISON = 10;

function referenceMs(ref, beatMs) {
  const tested = Object.keys(ref.pctAtTempo).map(Number);
  const nearest = tested.reduce((a, b) => (Math.abs(b - bpm) < Math.abs(a - bpm) ? b : a));
  return (ref.pctAtTempo[nearest] / 100) * beatMs;
}

function showSummary() {
  const n = offsets.length;
  const sd = n > 1 ? stdDev(offsets) : null;
  const m = n ? Math.round(mean(offsets)) : 0;
  const avg = !n ? "" : m === 0 ? "Average right on the click · " : `Average ${Math.abs(m)} ms ${describe(m)} · `;
  ui.sumSd.textContent = sd === null ? "–" : `±${Math.round(sd)} ms`;
  ui.sumSub.textContent = `${avg}${n} hits · ${sessionLength} s at ${bpm} BPM`;

  ui.sumCompare.innerHTML = "";
  if (sd === null || n < MIN_HITS_FOR_COMPARISON) {
    const p = document.createElement("p");
    p.textContent = `Play at least ${MIN_HITS_FOR_COMPARISON} hits to see how you compare.`;
    ui.sumCompare.append(p);
  } else {
    ui.sumCompare.append(levelsTable(sd));
  }

  ui.sumSources.innerHTML = REFERENCES.map(
    (ref) => `<li>${ref.about} <a href="${ref.url}" target="_blank" rel="noopener">${ref.citation}</a></li>`
  ).join("");

  ui.summary.hidden = false;
  ui.summary.scrollIntoView({ behavior: "smooth", block: "start" });
}

// One row per level plus a highlighted "You" row, sorted from tightest spread.
function levelsTable(sd) {
  const beatMs = 60000 / bpm;
  const rows = REFERENCES.map((ref, i) => ({ name: ref.level, ms: referenceMs(ref, beatMs), note: i + 1 }));
  rows.push({ name: "You", ms: sd, you: true });
  rows.sort((a, b) => a.ms - b.ms);

  const wrap = document.createElement("div");
  wrap.innerHTML = `
    <h3>How you compare <span class="muted">at ${bpm} BPM</span></h3>
    <table class="levels">
      <thead><tr><th>Level</th><th>Typical spread</th></tr></thead>
      <tbody>${rows
        .map(
          (r) => `<tr${r.you ? ' class="you"' : ""}>
            <td>${r.name}${r.note ? `<sup><a href="#sum-sources" title="Source ${r.note}">${r.note}</a></sup>` : ""}</td>
            <td>±${Math.round(r.ms)} ms</td></tr>`
        )
        .join("")}</tbody>
    </table>`;
  return wrap;
}

// ---------- Chart ----------

const MAX_POINTS = 60;
let points = []; // screen positions of drawn dots, for hover

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function drawChart() {
  const canvas = ui.chart;
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const g = canvas.getContext("2d");
  g.scale(dpr, dpr);
  g.clearRect(0, 0, w, h);

  const pad = { left: 52, right: 12, top: 12, bottom: 12 };
  const plotW = w - pad.left - pad.right;
  const plotH = h - pad.top - pad.bottom;

  const shown = offsets.slice(-MAX_POINTS);
  const firstIndex = offsets.length - shown.length;
  const maxAbs = Math.max(50, ...shown.map(Math.abs));
  const range = Math.ceil(maxAbs / 25) * 25;
  const y = (ms) => pad.top + plotH / 2 - (ms / range) * (plotH / 2);
  const x = (i) => pad.left + (i / (MAX_POINTS - 1)) * plotW;

  // Grid and labels
  g.font = "12px system-ui, sans-serif";
  g.textAlign = "right";
  g.textBaseline = "middle";
  for (const v of [-range, -range / 2, 0, range / 2, range]) {
    g.strokeStyle = v === 0 ? cssVar("--axis") : cssVar("--grid");
    g.lineWidth = 1;
    g.beginPath();
    g.moveTo(pad.left, Math.round(y(v)) + 0.5);
    g.lineTo(w - pad.right, Math.round(y(v)) + 0.5);
    g.stroke();
    g.fillStyle = cssVar("--text-muted");
    g.fillText(v === 0 ? "0" : (v > 0 ? "+" : "") + v, pad.left - 8, y(v));
  }
  g.fillText("late ↑", w - pad.right, pad.top + 14);
  g.fillText("early ↓", w - pad.right, h - pad.bottom - 14);

  // Dots
  const surface = cssVar("--surface");
  g.fillStyle = cssVar("--series");
  g.strokeStyle = surface;
  g.lineWidth = 2;
  points = shown.map((ms, i) => ({ x: x(i), y: y(ms), ms, n: firstIndex + i + 1 }));
  for (const p of points) {
    g.beginPath();
    g.arc(p.x, p.y, 4.5, 0, Math.PI * 2);
    g.fill();
    g.stroke();
  }
}

ui.chart.addEventListener("mousemove", (e) => {
  const rect = ui.chart.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  let best = null;
  let bestD = 14; // hit radius in px, bigger than the dot
  for (const p of points) {
    const d = Math.hypot(p.x - mx, p.y - my);
    if (d < bestD) { best = p; bestD = d; }
  }
  if (!best) {
    ui.tooltip.hidden = true;
    return;
  }
  ui.tooltip.textContent = `Hit ${best.n}: ${fmt(best.ms)} ${describe(best.ms)}`;
  ui.tooltip.style.left = best.x + "px";
  ui.tooltip.style.top = best.y + "px";
  ui.tooltip.hidden = false;
});
ui.chart.addEventListener("mouseleave", () => (ui.tooltip.hidden = true));

window.addEventListener("resize", drawChart);
window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", drawChart);

// ---------- Controls ----------

function setBpm(value) {
  const v = Math.min(240, Math.max(40, Number(value) || 90));
  ui.bpm.value = v;
  ui.bpmRange.value = v;
  bpm = v; // takes effect from the next click
}

function updateThreshold() {
  const t = Number(ui.threshold.value);
  ui.thresholdValue.textContent = t.toFixed(2);
  ui.meterThreshold.style.left = t * 100 + "%";
  if (onsetNode) onsetNode.port.postMessage({ threshold: t });
}

function showAudioError(err) {
  ui.status.textContent =
    err.name === "NotAllowedError"
      ? "Microphone access was blocked. Allow it in the browser's address bar and try again."
      : "Could not start audio: " + err.message;
}

// Start begins a new timed session; after a pause it resumes the current one.
async function start() {
  if (running || latency === null) return;
  try {
    await ensureAudio();
  } catch (err) {
    return showAudioError(err);
  }
  if (!paused) {
    clearResults();
    sessionLength = chosenSessionLength();
    remainingMs = sessionLength * 1000;
    ui.summary.hidden = true;
  }
  paused = false;
  setSessionControlsDisabled(true);
  startMetronome(Number(ui.bpm.value));
  startCountdown();
  ui.start.disabled = true;
  ui.pause.disabled = false;
  ui.calibrate.disabled = true;
  ui.status.textContent = "Listening…";
}

function pause() {
  if (!running) return;
  stopMetronome();
  stopCountdown();
  paused = true;
  ui.start.disabled = false;
  ui.start.textContent = "Resume";
  ui.pause.disabled = true;
  ui.calibrate.disabled = false;
  ui.status.textContent = "Paused.";
}

ui.bpm.addEventListener("change", () => setBpm(ui.bpm.value));
ui.bpmRange.addEventListener("input", () => setBpm(ui.bpmRange.value));
ui.threshold.addEventListener("input", updateThreshold);
ui.mic.addEventListener("change", async () => {
  try {
    await openMic(ui.mic.value);
  } catch (err) {
    showAudioError(err);
  }
});
ui.calibrate.addEventListener("click", calibrate);
ui.start.addEventListener("click", start);
ui.pause.addEventListener("click", pause);
ui.reset.addEventListener("click", resetSession);
ui.newSession.addEventListener("click", () => {
  resetSession();
  start();
});
// The footnote numbers in the levels table open the Sources list.
ui.sumCompare.addEventListener("click", (e) => {
  if (e.target.closest("sup a")) ui.sumSources.parentElement.open = true;
});
ui.sessionLength.addEventListener("change", () => {
  ui.customWrap.hidden = ui.sessionLength.value !== "custom";
});

ui.start.disabled = true;
updateThreshold();
render();
