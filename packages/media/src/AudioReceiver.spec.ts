import { afterEach, describe, expect, it, vi } from 'vitest'
import type { RemoteParticipant } from '@telefunc/room'
import { AudioReceiver } from './AudioReceiver.js'
import { MemberClock } from './clock.js'
import { encodeAudioPayload, encodeUnit } from './wire.js'

const FRAME_MS = 20
/** Every frame's quickest way here, and the jitter on top of it. */
const LEAST_MS = 30
const JITTER_MS = 10

describe('AudioReceiver', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  // A link that holds a member's sound for 200 ms, as a TCP retransmission does, then hands it on at once.
  it('drops no frame that comes in time as its delay falls back after a 200 ms stall', () => {
    const { receiver, played, amplitudes } = run(() => 100)
    const lost = [...amplitudes.keys()].filter((frame) => !played.has(frame))
    // Only frames held by the stall come late; every other frame lost was left out to catch up.
    expect(receiver.stats().late).toBe(lost.filter(stalled).length)
    expect(lost.filter((frame) => !stalled(frame)).length).toBe(receiver.stats().skipped)
    expect(receiver.stats().missing).toBe(0)
  })

  it('comes back to its buffer after the stall by leaving out the quietest frame queued, one a second', () => {
    // Speech: every fifth frame a pause.
    const { played, amplitudes, leftAt, queuedAt, receiver } = run((frame) =>
      frame % 5 === 0 ? 0 : 60 + (frame % 7) * 20,
    )
    const left = [...amplitudes.keys()].filter((frame) => !played.has(frame) && !stalled(frame))
    expect(left.length).toBeGreaterThan(0)
    const times = left.map((frame) => leftAt.get(frame)!).sort((a, b) => a - b)
    expect(Math.min(...times.slice(1).map((t, i) => t - times[i]!))).toBeGreaterThanOrEqual(1000)
    for (const frame of left) {
      const queued = queuedAt(leftAt.get(frame)!).filter((other) => other !== frame)
      expect(Math.min(...queued.map((other) => amplitudes.get(other)!))).toBeGreaterThanOrEqual(amplitudes.get(frame)!)
    }
    // Heard after its quickest transit, the jitter, a frame for a missing one's copy, and a frame behind at most.
    expect(receiver.heardAfterMs).toBeLessThanOrEqual(LEAST_MS + JITTER_MS + 2 * FRAME_MS)
  })

  // WebSocket over a lossy path: once a second, a retransmission holds 190 ms of frames and hands them on together.
  it('plays a link that bunches its frames once a second at most a frame later than their 95th percentile', () => {
    const { receiver, heard } = run(() => 100, bunched)
    // Past the buffer the 95th percentile of the last 2 s of delays and a frame make, to the 0.1 ms tick.
    const added = heard.map(
      ({ heardAfterMs, delays }) => heardAfterMs - (LEAST_MS + percentile(delays, 0.95) + FRAME_MS),
    )
    expect(percentile(added, 0.5)).toBeLessThanOrEqual(FRAME_MS + 0.1)
    expect(receiver.stats().late / FRAMES).toBeLessThan(0.01)
  })
})

/** Frames captured in the first 190 ms of each second come together, 190 ms into it; the others come on time. */
function bunched(captured: number, jitter: number): number {
  return captured % 1000 < 190 ? captured - (captured % 1000) + 190 + LEAST_MS : captured + LEAST_MS + jitter
}

/** A 200 ms stall: frames captured from 5.0 s to 5.2 s come together at 5.23 s. */
function stall(captured: number, jitter: number): number {
  return stalled(captured / FRAME_MS) ? 5230 : captured + LEAST_MS + jitter
}

function stalled(frame: number): boolean {
  return frame * FRAME_MS >= 5000 && frame * FRAME_MS < 5200
}

function percentile(values: number[], q: number): number {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor((sorted.length - 1) * q)]!
}

const FRAMES = 1500

/** 30 s of one member's 20 ms frames, each arriving as `arrival` says and as loud as `amplitude(frame)`. Returns the
 *  frames played (started and never stopped before their time), when each frame was last stopped before its time or else
 *  came, and from 2 s on, how long after its capture each frame is heard, with the last 2 s of delays then. */
function run(amplitude: (frame: number) => number, arrival: (captured: number, jitter: number) => number = stall) {
  let now = 0
  vi.spyOn(performance, 'now').mockImplementation(() => now)
  // A whole millisecond: the process's own start has a fraction, which shifts every 0.1 ms tick by up to half of one.
  vi.spyOn(performance, 'timeOrigin', 'get').mockReturnValue(1_700_000_000_000)
  const random = seeded(7)
  const amplitudes = new Map<number, number>()
  const samplesOf = new Map<Float32Array, number>()
  const started = new Map<number, { at: number; stopped: boolean }[]>()
  /** Every start, with when it was made and when it was stopped before its time, if it was. */
  const starts: { frame: number; at: number; madeAt: number; stoppedAt: number | null }[] = []
  /** When each frame was last taken off the schedule, or came. */
  const leftAt = new Map<number, number>()

  vi.stubGlobal(
    'EncodedAudioChunk',
    class {
      readonly data: Uint8Array
      constructor({ data }: { data: Uint8Array }) {
        this.data = data.slice()
      }
    },
  )
  // Decodes at once: a frame's payload is its number and its loudness.
  vi.stubGlobal(
    'AudioDecoder',
    class {
      private readonly output: (data: unknown) => void
      constructor({ output }: { output: (data: unknown) => void }) {
        this.output = output
      }
      readonly state = 'configured'
      configure() {}
      close() {}
      decode({ data }: { data: Uint8Array }) {
        const frame = (data[0]! << 8) | data[1]!
        const level = data[2]! / 255
        this.output({
          numberOfFrames: 960,
          sampleRate: 48_000,
          numberOfChannels: 1,
          copyTo(samples: Float32Array) {
            samples.fill(level)
            samplesOf.set(samples, frame)
          },
          close() {},
        })
      }
    },
  )
  vi.stubGlobal(
    'AudioBufferSourceNode',
    class {
      private readonly frame: number
      private start_: { at: number; stopped: boolean; made: (typeof starts)[number] } | null = null
      constructor(_context: unknown, { buffer }: { buffer: { getChannelData(channel: number): Float32Array } }) {
        this.frame = samplesOf.get(buffer.getChannelData(0))!
      }
      connect() {}
      start(at: number) {
        const made = { frame: this.frame, at, madeAt: now, stoppedAt: null }
        starts.push(made)
        this.start_ = { at, stopped: false, made }
        started.set(this.frame, [...(started.get(this.frame) ?? []), this.start_])
      }
      stop() {
        if (this.start_ === null || this.start_.at <= now / 1000) return
        this.start_.stopped = true
        this.start_.made.stoppedAt = now
        leftAt.set(this.frame, now)
      }
    },
  )
  const context = {
    get currentTime() {
      return now / 1000
    },
    baseLatency: 0,
    outputLatency: 0,
    createBuffer(numberOfChannels: number, length: number, sampleRate: number) {
      const channels = Array.from({ length: numberOfChannels }, () => new Float32Array(length))
      const duration = length / sampleRate
      return { numberOfChannels, length, sampleRate, duration, getChannelData: (channel: number) => channels[channel]! }
    },
  }

  let deliver: (data: Uint8Array) => void = () => {}
  const member = {
    subscribeBinary(listener: (data: Uint8Array) => void) {
      deliver = listener
      return () => {}
    },
  } as unknown as RemoteParticipant
  const receiver = new AudioReceiver({
    member,
    catalog: { track: 'mic@audio', sampleRate: 48_000, numberOfChannels: 1 },
    clock: new MemberClock(),
    context: context as unknown as AudioContext,
    output: {} as AudioNode,
    onSpeaking() {},
  })

  const arrivals: { frame: number; arrives: number }[] = []
  for (let frame = 0; frame < FRAMES; frame++) {
    const captured = frame * FRAME_MS
    const arrives = arrival(captured, random() * JITTER_MS)
    arrivals.push({ frame, arrives })
    amplitudes.set(frame, amplitude(frame))
  }
  arrivals.sort((a, b) => a.arrives - b.arrives || a.frame - b.frame)
  const delays: number[] = []
  const heard: { heardAfterMs: number; delays: number[] }[] = []
  for (const { frame, arrives } of arrivals) {
    now = arrives
    leftAt.set(frame, now)
    const captured = Math.round((performance.timeOrigin + frame * FRAME_MS) * 10) >>> 0
    const payload = new Uint8Array([frame >> 8, frame & 0xff, amplitudes.get(frame)!])
    const header = { key: true, level: 127, group: frame + 1, index: 0, captured }
    deliver(encodeUnit(header, encodeAudioPayload({ frame: payload, previous: new Uint8Array(), previousAgo: 0 })))
    delays.push(arrives - frame * FRAME_MS - LEAST_MS)
    // After the first 2 s, as the buffer has its delays.
    if (frame * FRAME_MS >= 2000 && receiver.heardAfterMs !== null) {
      heard.push({ heardAfterMs: receiver.heardAfterMs, delays: delays.slice(-100) })
    }
  }
  const played = new Set([...started].filter(([, starts]) => starts.some((s) => !s.stopped)).map(([frame]) => frame))
  /** The frames scheduled at `t` to start more than a frame later: those a frame can still be left out of. */
  const queuedAt = (t: number) =>
    starts
      .filter((s) => s.madeAt < t && (s.stoppedAt === null || s.stoppedAt >= t) && s.at > t / 1000 + FRAME_MS / 1000)
      .map((s) => s.frame)
  return { receiver, played, amplitudes, leftAt, queuedAt, heard }
}

/** Deterministic numbers in [0, 1). */
function seeded(seed: number): () => number {
  let state = seed
  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31
    return state / 2 ** 31
  }
}
