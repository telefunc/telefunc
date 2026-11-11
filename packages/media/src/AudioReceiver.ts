export { AudioReceiver }
export type { AudioReceiverStats }

import type { RemoteParticipant } from '@telefunc/room'
import { contextTimeOf, type MemberClock } from './clock.js'
import { decodeAudioPayload, decodeUnit, ticksSince, wallClock, type SourceCatalog } from './wire.js'

/** Opus's frame, as `AudioSender` sends it. */
const FRAME_MS = 20
/** The delays a buffer's depth is read from: two seconds of frames. */
const DELAYS_KEPT = 100
/** NetEq sets its buffer at the 95th percentile of how late frames come (webrtc `delay_manager.cc`). */
const DELAY_QUANTILE = 0.95
/** A buffer deeper than its frames need halves the difference this often: one stalled frame never moves it for long. */
const DEPTH_HALF_LIFE_MS = 2000
/** How long the sound must stay more than a frame past its depth before a frame is left out to catch up. */
const CATCH_UP_AFTER_MS = 1000
/** A frame after a gap fades in over this, as a click would mark the gap. */
const FADE_IN_MS = 2.5
/** LiveKit's: a member speaks while its sound is above −35 dBov, read every 400 ms (`audiolevel.go`). */
const SPEAKING_LEVEL = 35
const SPEAKING_HOLD_MS = 400

type AudioReceiverStats = {
  /** Frames played, those played from the next frame's copy among them. */
  played: number
  recovered: number
  /** Frames that came too late to play, and those that never came. */
  late: number
  missing: number
  /** Frames left out, the quietest first, to bring the sound back to the buffer's depth. */
  skipped: number
  /** In ms, how long the buffer holds a frame past the quickest a frame came. */
  bufferMs: number
}

type AudioReceiverOptions = {
  member: RemoteParticipant
  catalog: NonNullable<SourceCatalog['audio']>
  clock: MemberClock
  context: AudioContext
  output: AudioNode
  onSpeaking(speaking: boolean): void
}

/** A decoded frame scheduled to play: until it starts, it can still be left out. */
type Scheduled = { node: AudioBufferSourceNode; buffer: AudioBuffer; at: number; energy: number }

/** One member's sound: its frames in a buffer as deep as their delays vary, the copy in the next frame for one that is
 *  missing, decoded and played at their capture time plus that depth. */
class AudioReceiver {
  private readonly clock: MemberClock
  private readonly context: AudioContext
  private readonly output: AudioNode
  private readonly onSpeaking: (speaking: boolean) => void
  private readonly catalog: AudioReceiverOptions['catalog']
  /** Opened by the first frame, and again by the frame after one failed. */
  private decoder: AudioDecoder | null = null
  /** The capture tick of each frame the decoder holds, in order: it hands one sound per frame, in the order taken, and
   *  stamps its own timestamps from the first frame's on. */
  private decoding: number[] = []
  private readonly unsubscribe: () => void
  private readonly delays: number[] = []
  private depthMs = FRAME_MS
  /** `performance.now()` at the last delay noted. */
  private notedAt = 0
  private lastFrame = 0
  /** In context seconds, where the newest scheduled frame ends. */
  private playedUntil = 0
  /** The frames scheduled that start later than a frame from now, in order. */
  private scheduled: Scheduled[] = []
  /** In ms, how much later than the buffer's depth the newest frame plays, until frames left out catch it up. */
  private behindMs = 0
  /** In context seconds, since when the sound has stood more than a frame past the buffer's depth. */
  private behindSince: number | null = null
  /** A frame was left out: the next one played fades in. */
  private afterSkip = false
  private heardAfter: number | null = null
  private speaking = false
  private quietTimer: ReturnType<typeof setTimeout> | null = null
  private readonly counts = { played: 0, recovered: 0, late: 0, missing: 0, skipped: 0 }

  constructor({ member, catalog, clock, context, output, onSpeaking }: AudioReceiverOptions) {
    this.clock = clock
    this.context = context
    this.output = output
    this.onSpeaking = onSpeaking
    this.catalog = catalog
    this.unsubscribe = member.subscribeBinary((data) => this.receive(data), { track: catalog.track })
  }

  /** In ms, how long a frame waits here past the quickest a frame came: video waits as long, to stay in step. */
  get delayMs(): number {
    return this.depthMs + this.behindMs
  }

  /** In ms, how long after its capture the newest scheduled frame is heard: its wait in the context, and the context's
   *  own latency to the output. Null before a frame is scheduled. */
  get heardAfterMs(): number | null {
    return this.heardAfter
  }

  stats(): AudioReceiverStats {
    return { ...this.counts, bufferMs: this.delayMs }
  }

  stop(): void {
    this.unsubscribe()
    if (this.decoder !== null && this.decoder.state !== 'closed') this.decoder.close()
    this.decoder = null
    if (this.quietTimer !== null) clearTimeout(this.quietTimer)
  }

  private receive(data: Uint8Array): void {
    const unit = decodeUnit(data)
    const payload = unit && decodeAudioPayload(unit.payload)
    if (unit === null || payload === null) return
    const { group: frame, captured, level } = unit.header
    this.noteDelay(this.clock.arrived(captured))
    this.noteLevel(level)
    if (frame <= this.lastFrame) return
    const lost = this.lastFrame === 0 ? 0 : frame - this.lastFrame - 1
    if (lost > 0 && payload.previous.byteLength > 0) {
      this.decode(payload.previous, (captured - payload.previousAgo) >>> 0)
      this.counts.recovered++
      this.counts.missing += lost - 1
    } else {
      this.counts.missing += lost
    }
    this.lastFrame = frame
    this.decode(payload.frame, captured)
  }

  private decode(data: Uint8Array, captured: number): void {
    const decoder = this.decoder ?? this.openDecoder()
    this.decoding.push(captured)
    decoder.decode(new EncodedAudioChunk({ type: 'key', timestamp: captured * 100, data }))
  }

  private openDecoder(): AudioDecoder {
    const { sampleRate, numberOfChannels } = this.catalog
    const decoding: number[] = []
    const decoder = new AudioDecoder({
      output: (data) => this.schedule(data, decoding.shift()!),
      error: (err) => {
        console.error('[telefunc:media] An audio decoder failed:', err)
        if (this.decoder === decoder) this.decoder = null
      },
    })
    decoder.configure({ codec: 'opus', sampleRate, numberOfChannels })
    this.decoder = decoder
    this.decoding = decoding
    return decoder
  }

  /** The buffer's depth: what most frames' delays reach, and a frame more, for a missing one's copy to come. It deepens
   *  at once for a frame that comes later than it, and falls back over seconds. */
  private noteDelay(delay: number): void {
    this.delays.push(delay)
    if (this.delays.length > DELAYS_KEPT) this.delays.shift()
    const sorted = [...this.delays].sort((a, b) => a - b)
    const reach = Math.max(sorted[Math.floor((sorted.length - 1) * DELAY_QUANTILE)]!, delay) + FRAME_MS
    const now = performance.now()
    const kept = 0.5 ** ((now - this.notedAt) / DEPTH_HALF_LIFE_MS)
    this.depthMs = reach >= this.depthMs ? reach : reach + (this.depthMs - reach) * kept
    this.notedAt = now
  }

  private schedule(data: AudioData, captured: number): void {
    const playAt = (this.clock.earliestArrival(captured) + Math.round(this.depthMs * 10)) >>> 0
    const due = contextTimeOf(this.context, playAt)
    const { currentTime, baseLatency, outputLatency = 0 } = this.context
    // A frame that would overlap the one before it goes right after it; one whose time has passed is late.
    let at = Math.max(due, this.playedUntil)
    if (at < currentTime) {
      this.counts.late++
      return data.close()
    }
    const buffer = this.context.createBuffer(data.numberOfChannels, data.numberOfFrames, data.sampleRate)
    for (let channel = 0; channel < data.numberOfChannels; channel++) {
      data.copyTo(buffer.getChannelData(channel), { planeIndex: channel, format: 'f32-planar' })
    }
    data.close()
    this.scheduled = this.scheduled.filter((frame) => frame.at > currentTime + FRAME_MS / 1000)
    const energy = energyOf(buffer)
    if (this.staysBehind(at - due, currentTime)) {
      if (this.skipQuietest(energy)) return
      at = Math.max(due, this.playedUntil)
    }
    this.behindMs = (at - due) * 1000
    this.play(buffer, energy, at)
    this.heardAfter = ticksSince(wallClock(), captured) + (at - currentTime + baseLatency + outputLatency) * 1000
  }

  /** Whether the sound has stood more than a frame past the buffer's depth (`behind`, in s) for `CATCH_UP_AFTER_MS`;
   *  each time it has, the count starts over. */
  private staysBehind(behind: number, currentTime: number): boolean {
    if (behind <= FRAME_MS / 1000) {
      this.behindSince = null
      return false
    }
    this.behindSince ??= currentTime
    if (currentTime - this.behindSince < CATCH_UP_AFTER_MS / 1000) return false
    this.behindSince = currentTime
    return true
  }

  /** Leaves out the quietest of the frames yet to start and the one at hand (as loud as `energy`), and plays those
   *  after it a frame earlier. Returns whether it is the one at hand. */
  private skipQuietest(energy: number): boolean {
    this.counts.skipped++
    this.afterSkip = true
    let quietest = -1
    for (const [i, frame] of this.scheduled.entries()) {
      if (frame.energy < (this.scheduled[quietest]?.energy ?? energy)) quietest = i
    }
    if (quietest === -1) return true
    const [left, ...later] = this.scheduled.splice(quietest)
    for (const frame of [left!, ...later]) {
      frame.node.stop()
      this.counts.played--
    }
    this.playedUntil = left!.at
    for (const frame of later) this.play(frame.buffer, frame.energy, frame.at - left!.buffer.duration)
    return false
  }

  /** Plays `buffer` at context time `at`, faded in after a gap or a frame left out. */
  private play(buffer: AudioBuffer, energy: number, at: number): void {
    if (this.afterSkip || at > this.playedUntil + 0.001) fadeIn(buffer)
    this.afterSkip = false
    const node = new AudioBufferSourceNode(this.context, { buffer })
    node.connect(this.output)
    node.start(at)
    this.scheduled.push({ node, buffer, at, energy })
    this.playedUntil = at + buffer.duration
    this.counts.played++
  }

  private noteLevel(level: number): void {
    if (level > SPEAKING_LEVEL) return
    if (this.quietTimer !== null) clearTimeout(this.quietTimer)
    this.quietTimer = setTimeout(() => this.setSpeaking(false), SPEAKING_HOLD_MS)
    this.setSpeaking(true)
  }

  private setSpeaking(speaking: boolean): void {
    if (speaking === this.speaking) return
    this.speaking = speaking
    this.onSpeaking(speaking)
  }
}

/** A frame's loudness: the mean square of its first channel's samples, 0 for silence. */
function energyOf(buffer: AudioBuffer): number {
  const samples = buffer.getChannelData(0)
  let sum = 0
  for (const sample of samples) sum += sample * sample
  return sum / samples.length
}

function fadeIn(buffer: AudioBuffer): void {
  const length = Math.min(buffer.length, Math.round((buffer.sampleRate * FADE_IN_MS) / 1000))
  for (let channel = 0; channel < buffer.numberOfChannels; channel++) {
    const samples = buffer.getChannelData(channel)
    for (let i = 0; i < length; i++) samples[i]! *= i / length
  }
}
