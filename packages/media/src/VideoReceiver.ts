export { VideoReceiver }
export type { VideoReceiverStats }

import type { RemoteParticipant } from '@telefunc/room'
import type { AudioReceiver } from './AudioReceiver.js'
import type { MemberClock } from './clock.js'
import { decodeUnit, ticksSince, wallClock, type SourceCatalog, type VideoSize } from './wire.js'

/** A frame older than the path's delay plus this is late: the bar the meeting bench holds video to. */
const LATE_MS = 200
/** This many late frames in a row take the picture a size down, as a slow path would only make them later. */
const LATE_FRAMES_DOWN = 10
/** A size up is tried after this long with every frame on time: once its key frame lands, the old size stops. */
const ON_TIME_UP_MS = 4_000
/** W3C's freeze: a gap between shown frames over three times their mean, and over the mean plus 150 ms
 *  (webrtc-stats `freezeCount`). */
const FREEZE_FACTOR = 3
const FREEZE_PLUS_MS = 150
/** The capture-to-display times and audio/video offsets kept for `stats()`: the last minute at 30 frames a second. */
const DISPLAY_TIMES_KEPT = 1_800

type VideoReceiverStats = {
  track: string | null
  width: number
  height: number
  /** Frames shown; frames that came but couldn't be shown, as a frame before them in their group was missing, or a newer
   *  frame was due first. */
  shown: number
  dropped: number
  /** Key-frame groups the server cut short or skipped for this page. */
  skippedGroups: number
  freezes: number
  frozenMs: number
  /** In ms, capture to display: the clocks of two pages on one machine agree. */
  captureToDisplay: { p50: number; p95: number } | null
  /** In ms, when each frame is shown less when the sound captured with it is heard: negative where the sound comes
   *  late (ITU-R BT.1359-1's sign). Its 5th percentile is the sound's latest, its 95th its earliest. Null without sound. */
  audioVideoOffset: { p5: number; p50: number; p95: number } | null
}

type VideoReceiverOptions = {
  member: RemoteParticipant
  catalog: NonNullable<SourceCatalog['video']>
  clock: MemberClock
  element: HTMLVideoElement
  /** The member's sound, whose buffer its picture waits as long as, and against which its offset is measured. */
  audio(): Pick<AudioReceiver, 'delayMs' | 'heardAfterMs'> | null
}

/** One member's picture in `element`: the size that fits the element and comes on time, each frame checked against its
 *  group, decoded, and shown at its capture time plus the sound's delay. */
class VideoReceiver {
  readonly track: MediaStreamTrack
  private readonly options: VideoReceiverOptions
  private readonly sizes: VideoSize[]
  private readonly show: (frame: VideoFrame) => void
  private readonly observers: { disconnect(): void }[] = []
  /** The size shown, and the one that takes its place once its first key frame decodes. */
  private active: SizeReceiver | null = null
  private next: SizeReceiver | null = null
  /** The largest size the path is taken to carry, as an index into `sizes`. */
  private cap = 0
  private visible = true
  private lateInRow = 0
  private onTimeSince = performance.now()
  /** Decoded frames by when they are due, which a timer shows. */
  private readonly due: { at: number; frame: VideoFrame; captured: number }[] = []
  private showTimer: ReturnType<typeof setTimeout> | null = null
  private lastShownAt = 0
  private meanGapMs = 0
  private readonly displayTimes: number[] = []
  private readonly offsets: number[] = []
  private readonly counts = { shown: 0, dropped: 0, skippedGroups: 0, freezes: 0, frozenMs: 0 }

  constructor(options: VideoReceiverOptions) {
    this.options = options
    this.sizes = [...options.catalog.sizes].sort((a, b) => a.height - b.height)
    const output = videoOutput()
    this.track = output.track
    this.show = output.show
    this.watch(options.element)
    this.choose()
  }

  stats(): VideoReceiverStats {
    const size = this.active?.size
    const display = quantiles(this.displayTimes)
    const offset = quantiles(this.offsets)
    return {
      track: size?.track ?? null,
      width: size?.width ?? 0,
      height: size?.height ?? 0,
      ...this.counts,
      captureToDisplay: display && { p50: display(0.5), p95: display(0.95) },
      audioVideoOffset: offset && { p5: offset(0.05), p50: offset(0.5), p95: offset(0.95) },
    }
  }

  stop(): void {
    for (const observer of this.observers) observer.disconnect()
    this.active?.stop()
    this.next?.stop()
    this.active = this.next = null
    if (this.showTimer !== null) clearTimeout(this.showTimer)
    for (const { frame } of this.due.splice(0)) frame.close()
    this.track.stop()
  }

  /** The element's size and whether it shows pick the largest size worth sending it. */
  private watch(element: HTMLVideoElement): void {
    const resize = new ResizeObserver(() => this.choose())
    resize.observe(element)
    const intersection = new IntersectionObserver(([entry]) => {
      this.visible = entry?.isIntersecting ?? true
      this.choose()
    })
    intersection.observe(element)
    this.observers.push(resize, intersection)
  }

  /** The smallest size at least as tall as the element shows, within what the path carries; none while it is hidden. */
  private choose(): void {
    if (!this.visible) {
      this.active?.stop()
      this.next?.stop()
      this.active = this.next = null
      return
    }
    const shown = this.options.element.clientHeight * devicePixelRatio
    const fits = this.sizes.findIndex((size) => size.height >= shown)
    const wanted = this.sizes[Math.min(fits === -1 ? this.sizes.length - 1 : fits, this.cap)]!
    if ((this.next ?? this.active)?.size === wanted) return
    if (this.active?.size === wanted) {
      this.next?.stop()
      this.next = null
      return
    }
    this.next?.stop()
    this.next = new SizeReceiver({
      size: wanted,
      codec: this.options.catalog.codec,
      member: this.options.member,
      onUnit: (receiver, captured, skippedGroup) => this.onUnit(receiver, captured, skippedGroup),
      onFrame: (receiver, frame, captured) => this.onFrame(receiver, frame, captured),
    })
  }

  /** A unit of `receiver`'s size arrived: whether it came on time moves the size the path is taken to carry. */
  private onUnit(receiver: SizeReceiver, captured: number, skippedGroup: boolean): void {
    const late = this.options.clock.arrived(captured) > LATE_MS
    if (skippedGroup) this.counts.skippedGroups++
    if (receiver !== this.active) return
    this.lateInRow = late ? this.lateInRow + 1 : 0
    if (late || skippedGroup) this.onTimeSince = performance.now()
    if ((skippedGroup || this.lateInRow >= LATE_FRAMES_DOWN) && this.cap > 0) {
      // The smallest size is as far down as it goes.
      this.cap = Math.max(0, Math.min(this.cap, this.sizes.indexOf(receiver.size)) - 1)
      this.lateInRow = 0
      return this.choose()
    }
    const up = this.sizes.indexOf(receiver.size) + 1
    if (performance.now() - this.onTimeSince >= ON_TIME_UP_MS && up < this.sizes.length && this.cap < up) {
      this.cap = up
      this.onTimeSince = performance.now()
      this.choose()
    }
  }

  private onFrame(receiver: SizeReceiver, frame: VideoFrame, captured: number): void {
    if (receiver === this.next) {
      // The new size's first key frame decoded: it takes the old one's place.
      this.active?.stop()
      this.active = receiver
      this.next = null
    }
    if (receiver !== this.active) return frame.close()
    const audioDelayMs = this.options.audio()?.delayMs ?? 0
    const at = (this.options.clock.earliestArrival(captured) + Math.round(audioDelayMs * 10)) >>> 0
    this.due.push({ at, frame, captured })
    this.showDue()
  }

  /** Shows the newest frame that is due, and drops those it overtook; waits for the next. */
  private showDue(): void {
    if (this.showTimer !== null) clearTimeout(this.showTimer)
    this.showTimer = null
    const now = wallClock()
    let newest: (typeof this.due)[number] | null = null
    while (this.due.length > 0 && ticksSince(now, this.due[0]!.at) >= 0) {
      if (newest !== null) {
        newest.frame.close()
        this.counts.dropped++
      }
      newest = this.due.shift()!
    }
    if (newest !== null) this.display(newest.frame, newest.captured)
    if (this.due.length > 0) {
      this.showTimer = setTimeout(() => this.showDue(), ticksSince(this.due[0]!.at, now))
    }
  }

  private display(frame: VideoFrame, captured: number): void {
    const now = performance.now()
    if (this.counts.shown > 0) {
      const gap = now - this.lastShownAt
      if (this.counts.shown > 1 && gap > Math.max(FREEZE_FACTOR * this.meanGapMs, this.meanGapMs + FREEZE_PLUS_MS)) {
        this.counts.freezes++
        this.counts.frozenMs += gap
      }
      this.meanGapMs = this.counts.shown === 1 ? gap : this.meanGapMs + (gap - this.meanGapMs) / 16
    }
    this.lastShownAt = now
    this.counts.shown++
    const shownAfter = ticksSince(wallClock(), captured)
    keep(this.displayTimes, shownAfter)
    const heardAfter = this.options.audio()?.heardAfterMs
    if (heardAfter != null) keep(this.offsets, shownAfter - heardAfter)
    this.show(frame)
  }
}

type SizeReceiverOptions = {
  size: VideoSize
  codec: string
  member: RemoteParticipant
  onUnit(receiver: SizeReceiver, captured: number, skippedGroup: boolean): void
  onFrame(receiver: SizeReceiver, frame: VideoFrame, captured: number): void
}

/** One size's track: its units, each checked against its group before the decoder gets it. A decoder shown a frame
 *  whose predecessor is missing shows a broken picture, often without an error. */
class SizeReceiver {
  readonly size: VideoSize
  private readonly options: SizeReceiverOptions
  /** Opened by the first key frame, and again by the key frame after one failed. */
  private decoder: VideoDecoder | null = null
  private readonly unsubscribe: () => void
  /** The group being decoded, and its newest frame's index; null until a key frame, and after a frame went missing. */
  private group: number | null = null
  private index = 0
  /** The track's newest `info.seq`: a gap is what the server skipped of it. */
  private lastSeq: number | null = null

  constructor(options: SizeReceiverOptions) {
    this.options = options
    this.size = options.size
    // Its publisher hears that this page watches, and sends a key frame.
    this.unsubscribe = options.member.subscribeBinary((data, info) => this.receive(data, info.seq), {
      track: this.size.track,
    })
  }

  stop(): void {
    this.unsubscribe()
    if (this.decoder !== null && this.decoder.state !== 'closed') this.decoder.close()
    this.decoder = null
  }

  private receive(data: Uint8Array, seq: number): void {
    // Once the server skips a frame of a group, it skips the rest of it: the key frame after a gap begins anew.
    const skipped = this.lastSeq !== null && seq > this.lastSeq + 1
    this.lastSeq = seq
    const unit = decodeUnit(data)
    if (unit === null) return
    const { key, group, index, captured } = unit.header
    this.options.onUnit(this, captured, key && skipped)
    if (key) {
      this.group = group
      this.index = 0
    } else if (group === this.group && index === this.index + 1 && this.decoder !== null) {
      this.index = index
    } else {
      // What follows a missing frame, or a failed decoder, waits for the next key frame.
      this.group = null
      return
    }
    // The capture tick rides the chunk's timestamp, which the decoded frame keeps.
    const chunk = new EncodedVideoChunk({ type: key ? 'key' : 'delta', timestamp: captured, data: unit.payload })
    ;(this.decoder ?? this.openDecoder()).decode(chunk)
  }

  private openDecoder(): VideoDecoder {
    const decoder = new VideoDecoder({
      output: (frame) => this.options.onFrame(this, frame, frame.timestamp >>> 0),
      error: (err) => {
        console.error('[telefunc:media] A video decoder failed:', err)
        // The next frame of its group asks for the key frame a new one starts at.
        if (this.decoder === decoder) this.decoder = null
      },
    })
    decoder.configure({ codec: this.options.codec, optimizeForLatency: true })
    this.decoder = decoder
    return decoder
  }
}

function keep(samples: number[], sample: number): void {
  samples.push(sample)
  if (samples.length > DISPLAY_TIMES_KEPT) samples.shift()
}

/** The quantiles of `samples`, or null where there are none. */
function quantiles(samples: number[]): ((q: number) => number) | null {
  if (samples.length === 0) return null
  const sorted = [...samples].sort((a, b) => a - b)
  return (q) => sorted[Math.floor((sorted.length - 1) * q)]!
}

/** A track that shows the frames handed to it: Chrome's generator, else a canvas's stream. */
function videoOutput(): { track: MediaStreamTrack; show(frame: VideoFrame): void } {
  const Generator = (
    globalThis as {
      MediaStreamTrackGenerator?: new (init: { kind: 'video' }) => MediaStreamTrack & {
        writable: WritableStream<VideoFrame>
      }
    }
  ).MediaStreamTrackGenerator
  if (Generator) {
    const track = new Generator({ kind: 'video' })
    const writer = track.writable.getWriter()
    return { track, show: (frame) => void writer.write(frame).catch(() => frame.close()) }
  }
  const canvas = document.createElement('canvas')
  const context = canvas.getContext('2d')!
  const [track] = canvas.captureStream().getVideoTracks()
  return {
    track: track!,
    show(frame) {
      canvas.width = frame.displayWidth
      canvas.height = frame.displayHeight
      context.drawImage(frame, 0, 0)
      frame.close()
    },
  }
}
