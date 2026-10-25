// Runs on the audio thread, on the same clock the metronome clicks are scheduled on.
//
// Hits are detected only in the low frequencies (below ~300 Hz), where drums and
// table knocks are strong but the high-pitched metronome click from the laptop
// speakers is almost absent. So the click doesn't trigger hits, while a hit that
// lands exactly on the click still does.
//
// In calibration mode, hit detection is off. Instead, for each click the main
// thread announces, the processor records the full-range mic signal and reports
// when the click actually arrived, plus how much of it leaked into the hit band.

const CUTOFF = 300;           // Hz
const QS = [0.5412, 1.3066];  // two biquads = 4th-order Butterworth low-pass
// The filter delays its output slightly; subtract that from hit times.
const FILTER_DELAY = (1 / QS[0] + 1 / QS[1]) / (2 * Math.PI * CUTOFF);
const CAL_WINDOW = 0.5;       // seconds of audio examined after each calibration click

// A hit is a sudden jump in loudness: the sound must be above the threshold AND
// at least RISE times louder than it was LOOKBACK seconds ago. A drum or table
// keeps ringing after a hit, but the ring only fades, so it can never re-trigger
// no matter how long it lasts or how low the sensitivity is.
const RISE = 2;
const LOOKBACK = 0.02;        // seconds
const RELEASE = 0.1;          // seconds for the loudness follower to fade
const HOLD = 0.05;            // no new hit within 50 ms of the last one

class LowPass {
  constructor(freq, q) {
    const w = (2 * Math.PI * freq) / sampleRate;
    const cos = Math.cos(w);
    const alpha = Math.sin(w) / (2 * q);
    const a0 = 1 + alpha;
    this.b0 = (1 - cos) / 2 / a0;
    this.b1 = (1 - cos) / a0;
    this.b2 = this.b0;
    this.a1 = (-2 * cos) / a0;
    this.a2 = (1 - alpha) / a0;
    this.x1 = this.x2 = this.y1 = this.y2 = 0;
  }

  process(x) {
    const y = this.b0 * x + this.b1 * this.x1 + this.b2 * this.x2 - this.a1 * this.y1 - this.a2 * this.y2;
    this.x2 = this.x1;
    this.x1 = x;
    this.y2 = this.y1;
    this.y1 = y;
    return y;
  }
}

class OnsetProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.filters = QS.map((q) => new LowPass(CUTOFF, q));
    this.threshold = 0.2;
    this.holdFrames = Math.round(HOLD * sampleRate);
    this.lastOnsetFrame = -Infinity;
    // Loudness follower: jumps up with the signal, fades slowly. Its recent
    // history is kept in a ring buffer so we can compare against LOOKBACK ago.
    this.env = 0;
    this.envFade = Math.exp(-1 / (RELEASE * sampleRate));
    this.envHistory = new Float32Array(Math.round(LOOKBACK * sampleRate));
    this.envPos = 0;
    this.levelPeak = 0;
    this.levelFrames = 0;

    this.calibrating = false;
    this.pendingClicks = [];
    this.window = null;
    this.windowBuf = new Float32Array(Math.round(CAL_WINDOW * sampleRate));

    this.port.onmessage = (e) => {
      const d = e.data;
      if (typeof d.threshold === "number") this.threshold = d.threshold;
      if (typeof d.calibrating === "boolean") {
        this.calibrating = d.calibrating;
        this.pendingClicks = [];
        this.window = null;
      }
      if (typeof d.click === "number") this.pendingClicks.push(d.click);
    };
  }

  process(inputs) {
    const samples = inputs[0] && inputs[0][0];
    if (!samples) return true;

    for (let i = 0; i < samples.length; i++) {
      const frame = currentFrame + i;
      const raw = samples[i];
      const low = Math.abs(this.filters[1].process(this.filters[0].process(raw)));
      if (low > this.levelPeak) this.levelPeak = low;

      this.env = Math.max(low, this.env * this.envFade);
      const envBefore = this.envHistory[this.envPos]; // follower value LOOKBACK ago
      this.envHistory[this.envPos] = this.env;
      this.envPos = (this.envPos + 1) % this.envHistory.length;

      if (this.calibrating) {
        this.calibrationSample(frame, raw, low);
      } else if (
        low >= this.threshold &&
        low >= RISE * envBefore &&
        frame - this.lastOnsetFrame > this.holdFrames
      ) {
        this.lastOnsetFrame = frame;
        this.port.postMessage({ type: "onset", time: frame / sampleRate - FILTER_DELAY });
      }
    }

    // Send the hit-band level ~20 times a second for the meter.
    this.levelFrames += samples.length;
    if (this.levelFrames >= sampleRate / 20) {
      this.port.postMessage({ type: "level", peak: this.levelPeak });
      this.levelPeak = 0;
      this.levelFrames = 0;
    }
    return true;
  }

  calibrationSample(frame, raw, low) {
    if (!this.window && this.pendingClicks.length && frame >= this.pendingClicks[0] * sampleRate) {
      this.window = { click: this.pendingClicks.shift(), start: frame, n: 0, leak: 0 };
    }
    const w = this.window;
    if (!w) return;

    this.windowBuf[w.n++] = Math.abs(raw);
    if (low > w.leak) w.leak = low;
    if (w.n < this.windowBuf.length) return;

    // The click arrived where the signal first reaches 30% of its loudest point.
    let peak = 0;
    for (const v of this.windowBuf) if (v > peak) peak = v;
    let idx = 0;
    while (this.windowBuf[idx] < 0.3 * peak) idx++;
    this.port.postMessage({
      type: "click",
      click: w.click,
      arrival: (w.start + idx) / sampleRate,
      peak,
      leak: w.leak,
    });
    this.window = null;
  }
}

registerProcessor("onset-processor", OnsetProcessor);
