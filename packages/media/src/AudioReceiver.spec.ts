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
  it('loses no frame to a 200 ms stall, nor to its delay falling back after it', () => {
    const { receiver, played } = run(() => 100)
    expect(receiver.stats().late).toBe(0)
    expect(receiver.stats().missing).toBe(0)
    expect(played.size + receiver.stats().skipped).toBe(FRAMES)
  })

  it('comes back to its buffer after the stall by leaving out its quietest frames, pauses first', () => {
    // Speech: every fifth frame a pause.
    const { receiver, played, amplitudes, leftAt } = run((frame) => (frame % 5 === 0 ? 0 : 60 + (frame % 7) * 20))
    const left = [...amplitudes.keys()].filter((frame) => !played.has(frame))
    expect(left.length).toBeGreaterThan(0)
    // One at a time, each after the sound stood behind for a second.
    const times = left.map((frame) => leftAt.get(frame)!).sort((a, b) => a - b)
    expect(Math.min(...times.slice(1).map((t, i) => t - times[i]!))).toBeGreaterThanOrEqual(1000)
    // A fifth of the frames are pauses; the frames left out are, but for the odd one when no pause is queued.
    expect(left.filter((frame) => amplitudes.get(frame) === 0).length / left.length).toBeGreaterThanOrEqual(0.8)
    expect(receiver.stats().late).toBe(0)
    // Heard after its quickest transit, the jitter, a frame for a missing one's copy, and a frame behind at most.
    expect(receiver.heardAfterMs).toBeLessThanOrEqual(LEAST_MS + JITTER_MS + 2 * FRAME_MS)
  })
})

const FRAMES = 1500

/** 30 s of one member's 20 ms frames, those captured from 5.0 s to 5.2 s held until 5.23 s; each frame's sound as loud
 *  as `amplitude(frame)`. Returns the frames played (started and never stopped before their time), and when each frame
 *  was last stopped before its time, or else came. */
function run(amplitude: (frame: number) => number) {
  let now = 0
  vi.spyOn(performance, 'now').mockImplementation(() => now)
  const random = seeded(7)
  const amplitudes = new Map<number, number>()
  const samplesOf = new Map<Float32Array, number>()
  const started = new Map<number, { at: number; stopped: boolean }[]>()
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
      private start_: { at: number; stopped: boolean } | null = null
      constructor(_context: unknown, { buffer }: { buffer: { getChannelData(channel: number): Float32Array } }) {
        this.frame = samplesOf.get(buffer.getChannelData(0))!
      }
      connect() {}
      start(at: number) {
        this.start_ = { at, stopped: false }
        started.set(this.frame, [...(started.get(this.frame) ?? []), this.start_])
      }
      stop() {
        if (this.start_ === null || this.start_.at <= now / 1000) return
        this.start_.stopped = true
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
    const arrives = captured >= 5000 && captured < 5200 ? 5230 : captured + LEAST_MS + random() * JITTER_MS
    arrivals.push({ frame, arrives })
    amplitudes.set(frame, amplitude(frame))
  }
  arrivals.sort((a, b) => a.arrives - b.arrives || a.frame - b.frame)
  for (const { frame, arrives } of arrivals) {
    now = arrives
    leftAt.set(frame, now)
    const captured = Math.round((performance.timeOrigin + frame * FRAME_MS) * 10) >>> 0
    const payload = new Uint8Array([frame >> 8, frame & 0xff, amplitudes.get(frame)!])
    const header = { key: true, level: 127, group: frame + 1, index: 0, captured }
    deliver(encodeUnit(header, encodeAudioPayload({ frame: payload, previous: new Uint8Array(), previousAgo: 0 })))
  }
  const played = new Set([...started].filter(([, starts]) => starts.some((s) => !s.stopped)).map(([frame]) => frame))
  return { receiver, played, amplitudes, leftAt }
}

/** Deterministic numbers in [0, 1). */
function seeded(seed: number): () => number {
  let state = seed
  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31
    return state / 2 ** 31
  }
}
