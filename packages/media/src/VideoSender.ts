export { VideoSender }
export type { VideoSenderStats }

import type { LocalParticipant } from '@telefunc/room'
import { readFrames } from './capture.js'
import { Receipts } from './Receipts.js'
import { encodeUnit, videoTrackOf, wallClock, type VideoSize } from './wire.js'

type CodecConfig = Omit<VideoEncoderConfig, 'width' | 'height'>

/** What a codec is asked for: the first one this page encodes is the one it publishes. VP8's two time layers make it
 *  follow a lower bitrate, which plain VP8 doesn't. */
const CODECS: CodecConfig[] = [
  { codec: 'vp8', scalabilityMode: 'L1T2' },
  { codec: 'avc1.42001f', avc: { format: 'annexb' } },
  { codec: 'vp09.00.10.08' },
]
/** LiveKit's simulcast ladder and its bitrates at 30 frames a second (client-sdk-js `VideoPresets`). */
const LADDER = [
  { height: 180, bitrate: 160_000 },
  { height: 360, bitrate: 450_000 },
  { height: 720, bitrate: 1_700_000 },
]
/** A viewer that starts watching, or lost a frame, waits at most this long for a key frame: MoQ's group length
 *  (moq `publish/src/video/encoder.ts`). A key frame in realtime mode is 1.3 to 3.6 delta frames, so this costs at
 *  most 4 % of the bitrate. */
const KEY_FRAME_INTERVAL_MS = 2_000
/** A size answers requests for a key frame at most this often, as LiveKit's server asks for them (`receiver_base.go`). */
const KEY_FRAME_REQUEST_GAP_MS = 1_000
/** A size nobody watches any more stops this long after, as LiveKit's dynacast does (`config.go`). */
const UNWATCHED_STOP_MS = 5_000
/** A size whose share of the uplink is under this part of its bitrate stops, as the smaller ones look better; the
 *  smallest gets at least this part of its bitrate, and its frames wait for their receipts where the uplink is slower. */
const FIT_FLOOR = 0.5

type SizeStats = VideoSize & { bitrate: number; frames: number; keyFrames: number; bytes: number }
type VideoSenderStats = { codec: string | null; sizes: SizeStats[] }

/** One video source on its Room tracks, one per size: each frame is encoded in every size someone watches, within its
 *  share of the page's uplink, a key frame every `KEY_FRAME_INTERVAL_MS` and on request. */
class VideoSender {
  private readonly sizes: SizeEncoder[]
  private readonly stopReading: () => void
  private config: CodecConfig | null = null
  /** Resolves once the page found a codec it encodes, or with null where it has none. */
  readonly codec: Promise<string | null>

  constructor({ me, name, track }: { me: LocalParticipant; name: string; track: MediaStreamTrack }) {
    const { width = 640, height = 360, frameRate = 30 } = track.getSettings()
    this.sizes = ladderFor(width, height).map(
      (size) => new SizeEncoder({ me, track: videoTrackOf(name, size.height), frameMs: 1000 / frameRate, ...size }),
    )
    this.codec = this.findCodec()
    this.stopReading = readFrames(track, (frame) => this.encode(frame))
  }

  get catalog(): VideoSize[] {
    return this.sizes.map(({ track, width, height }) => ({ track, width, height }))
  }

  /** Someone watches `track` now, or no one does any more; `null`: every track. */
  onDemand(track: string | null, wanted: boolean): void {
    for (const size of this.sizes) if (track === null || track === size.track) size.setWanted(wanted)
  }

  requestKeyFrame(track: string): void {
    this.sizes.find((size) => size.track === track)?.requestKeyFrame()
  }

  /** Shares `bitsPerSecond` among the sizes someone watches, from the smallest; returns what they take. */
  setBudget(bitsPerSecond: number): number {
    let taken = 0
    for (const [i, size] of this.sizes.entries()) {
      if (!size.wanted) continue
      const share = Math.min(size.maxBitrate, bitsPerSecond - taken)
      const floor = size.maxBitrate * FIT_FLOOR
      const bitrate = i === 0 ? Math.max(share, floor) : share >= floor ? share : 0
      size.setBitrate(Math.round(bitrate))
      taken += bitrate
    }
    return taken
  }

  stats(): VideoSenderStats {
    return { codec: this.config?.codec ?? null, sizes: this.sizes.map((size) => size.stats()) }
  }

  stop(): void {
    this.stopReading()
    for (const size of this.sizes) size.close()
  }

  private async findCodec(): Promise<string | null> {
    const { width, height } = this.sizes.at(-1)!
    for (const config of CODECS) {
      const { supported } = await VideoEncoder.isConfigSupported({ ...config, width, height, latencyMode: 'realtime' })
      if (supported) {
        this.config = config
        return config.codec
      }
    }
    return null
  }

  private encode(frame: VideoFrame): void {
    const captured = wallClock()
    if (this.config !== null) for (const size of this.sizes) size.encode(frame, captured, this.config)
    frame.close()
  }
}

/** The ladder's sizes under the source's, and the source's own, up to the ladder's largest. */
function ladderFor(width: number, height: number) {
  const top = Math.min(height, LADDER.at(-1)!.height)
  const heights = [...LADDER.map((rung) => rung.height).filter((h) => h < top), top]
  return heights.map((h) => ({
    height: h,
    // Even, as 4:2:0 sampling needs.
    width: Math.round((width * h) / height / 2) * 2,
    maxBitrate: LADDER.find((rung) => rung.height === h)?.bitrate ?? Math.round(LADDER[1]!.bitrate * (h / 360) ** 2),
  }))
}

type SizeEncoderOptions = Omit<VideoSize, 'track'> & {
  me: LocalParticipant
  track: string
  maxBitrate: number
  frameMs: number
}

/** One size of a source: its encoder, its track, and whether someone watches it. */
class SizeEncoder {
  readonly track: string
  readonly width: number
  readonly height: number
  readonly maxBitrate: number
  wanted = false
  private readonly me: LocalParticipant
  private readonly frameMs: number
  private readonly receipts = new Receipts()
  private encoder: VideoEncoder | null = null
  private config: CodecConfig | null = null
  private bitrate = 0
  /** The capture time of each frame the encoder holds, in order. */
  private readonly capturedTimes: { timestamp: number; captured: number }[] = []
  private group = 0
  private index = 0
  private keyFrameDue = true
  private lastKeyFrameAt = Number.NEGATIVE_INFINITY
  private stopTimer: ReturnType<typeof setTimeout> | null = null
  private frames = 0
  private keyFrames = 0
  private bytes = 0

  constructor({ me, track, width, height, maxBitrate, frameMs }: SizeEncoderOptions) {
    this.me = me
    this.track = track
    this.width = width
    this.height = height
    this.maxBitrate = maxBitrate
    this.frameMs = frameMs
  }

  setWanted(wanted: boolean): void {
    if (this.stopTimer !== null) clearTimeout(this.stopTimer)
    this.stopTimer = null
    if (wanted) {
      this.wanted = true
      return
    }
    this.stopTimer = setTimeout(() => {
      this.wanted = false
      this.close()
    }, UNWATCHED_STOP_MS)
  }

  requestKeyFrame(): void {
    if (performance.now() - this.lastKeyFrameAt >= KEY_FRAME_REQUEST_GAP_MS) this.keyFrameDue = true
  }

  setBitrate(bitrate: number): void {
    if (bitrate === this.bitrate) return
    this.bitrate = bitrate
    if (bitrate === 0) return this.close()
    // A new bitrate costs no key frame: the encoder takes it from its next frame.
    if (this.encoder !== null && this.config !== null) this.encoder.configure(this.configOf(this.config))
  }

  encode(frame: VideoFrame, captured: number, config: CodecConfig): void {
    if (!this.wanted || this.bitrate === 0) return
    // A frame the encoder or the uplink would hold only waits behind those it holds: the next one goes instead.
    if ((this.encoder?.encodeQueueSize ?? 0) > 0 || this.receipts.behind(2 * this.frameMs)) return
    const encoder = this.encoder ?? this.open(config)
    const keyFrame = this.keyFrameDue || performance.now() - this.lastKeyFrameAt >= KEY_FRAME_INTERVAL_MS
    if (keyFrame) {
      this.keyFrameDue = false
      this.lastKeyFrameAt = performance.now()
    }
    this.capturedTimes.push({ timestamp: frame.timestamp, captured })
    encoder.encode(frame, { keyFrame })
  }

  stats(): SizeStats {
    const { track, width, height, bitrate, frames, keyFrames, bytes } = this
    return { track, width, height, bitrate, frames, keyFrames, bytes }
  }

  close(): void {
    if (this.encoder !== null && this.encoder.state !== 'closed') this.encoder.close()
    this.encoder = null
    this.capturedTimes.length = 0
    this.keyFrameDue = true
  }

  private configOf(config: CodecConfig): VideoEncoderConfig {
    const { width, height, bitrate } = this
    return { ...config, width, height, bitrate, framerate: 1000 / this.frameMs, latencyMode: 'realtime' }
  }

  private open(config: CodecConfig): VideoEncoder {
    const encoder = new VideoEncoder({
      output: (chunk) => this.send(chunk),
      error: (err) => {
        console.error('[telefunc:media] A video encoder failed:', err)
        if (this.encoder === encoder) this.close()
      },
    })
    encoder.configure(this.configOf(config))
    this.encoder = encoder
    this.config = config
    this.keyFrameDue = true
    return encoder
  }

  private send(chunk: EncodedVideoChunk): void {
    let captured = wallClock()
    // The encoder hands its chunks in the order it took the frames, and may leave one out.
    while (this.capturedTimes.length > 0) {
      const next = this.capturedTimes.shift()!
      if (next.timestamp !== chunk.timestamp) continue
      captured = next.captured
      break
    }
    const key = chunk.type === 'key'
    if (key) {
      this.group++
      this.index = 0
      this.keyFrames++
    } else {
      this.index++
    }
    this.frames++
    this.bytes += chunk.byteLength
    const data = new Uint8Array(chunk.byteLength)
    chunk.copyTo(data)
    const unit = encodeUnit({ key, level: 0, group: this.group, index: this.index, captured }, data)
    this.receipts.add(this.me.publishBinary(unit, { track: this.track, lossy: true, delta: !key }))
  }
}
