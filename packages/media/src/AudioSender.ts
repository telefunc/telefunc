export { AudioSender, AUDIO_BITRATE }
export type { AudioSenderStats }

import type { LocalParticipant } from '@telefunc/room'
import { readAudio } from './capture.js'
import { audioContext } from './clock.js'
import { audioTrackOf, encodeAudioPayload, encodeUnit, wallClock } from './wire.js'

/** Speech in Opus: 72 B a 20 ms frame. With the frame before it in each unit, the uplink carries about twice it. */
const AUDIO_BITRATE = 32_000
/** Opus's frame, at which Chrome's WebRTC sends it. */
const FRAME_US = 20_000
/** RFC 6464's floor: a level of 127 is silence. */
const SILENT_LEVEL = 127

type AudioSenderStats = { track: string; frames: number; bytes: number }

/** One audio source on its Room track: Opus with discontinuous transmission, so silence costs 2 frames a second where
 *  sound costs 50, each frame with the one before it and its loudness. */
class AudioSender {
  readonly track: string
  /** Set from the source's first sound, which the encoder takes as it comes. */
  sampleRate = 48_000
  numberOfChannels = 1
  private readonly me: LocalParticipant
  private readonly stopReading: () => void
  private encoder: AudioEncoder | null = null
  private wanted = false
  private frame = 0
  private previous: { data: Uint8Array; captured: number } | null = null
  /** The loudest of what the encoder took since its last frame, in −dBov. */
  private level = SILENT_LEVEL
  /** The wall-clock tick of each sound the encoder holds, by its timestamp. */
  private readonly capturedTimes = new Map<number, number>()
  private frames = 0
  private bytes = 0

  constructor({ me, name, track }: { me: LocalParticipant; name: string; track: MediaStreamTrack }) {
    this.me = me
    this.track = audioTrackOf(name)
    this.stopReading = readAudio(track, audioContext(), (data) => this.encode(data))
  }

  /** Someone listens now, or no one does any more. */
  setWanted(wanted: boolean): void {
    this.wanted = wanted
  }

  stats(): AudioSenderStats {
    const { track, frames, bytes } = this
    return { track, frames, bytes }
  }

  stop(): void {
    this.stopReading()
    if (this.encoder !== null && this.encoder.state !== 'closed') this.encoder.close()
    this.encoder = null
  }

  private encode(data: AudioData): void {
    if (this.wanted) {
      this.level = Math.min(this.level, levelOf(data))
      this.capturedTimes.set(data.timestamp, wallClock())
      ;(this.encoder ?? this.open(data)).encode(data)
    }
    data.close()
  }

  private open({ sampleRate, numberOfChannels }: AudioData): AudioEncoder {
    this.sampleRate = sampleRate
    this.numberOfChannels = numberOfChannels
    const encoder = new AudioEncoder({
      output: (chunk) => this.send(chunk),
      error: (err) => {
        console.error('[telefunc:media] An audio encoder failed:', err)
        if (this.encoder === encoder) this.encoder = null
      },
    })
    encoder.configure({
      codec: 'opus',
      sampleRate,
      numberOfChannels,
      bitrate: AUDIO_BITRATE,
      opus: { frameDuration: FRAME_US, usedtx: true },
    })
    this.encoder = encoder
    return encoder
  }

  private send(chunk: EncodedAudioChunk): void {
    const captured = this.capturedAt(chunk.timestamp)
    const data = new Uint8Array(chunk.byteLength)
    chunk.copyTo(data)
    const { previous } = this
    const payload = encodeAudioPayload({
      frame: data,
      previous: previous?.data ?? new Uint8Array(0),
      previousAgo: previous === null ? 0 : (captured - previous.captured) >>> 0,
    })
    const unit = encodeUnit({ key: true, level: this.level, group: ++this.frame, index: 0, captured }, payload)
    void this.me.publishBinary(unit, { track: this.track, lossy: true })
    this.previous = { data, captured }
    this.level = SILENT_LEVEL
    this.frames++
    this.bytes += unit.byteLength
  }

  /** A frame's capture time: that of the newest sound the encoder took at or before its timestamp. */
  private capturedAt(timestamp: number): number {
    let captured = wallClock()
    for (const [taken, at] of this.capturedTimes) {
      if (taken > timestamp) break
      captured = at
      this.capturedTimes.delete(taken)
    }
    return captured
  }
}

/** RFC 6464's level: the sound's power under a full-scale square wave, in dB, 0 to 127. */
function levelOf(data: AudioData): number {
  const samples = new Float32Array(data.numberOfFrames)
  data.copyTo(samples, { planeIndex: 0, format: 'f32-planar' })
  let power = 0
  for (const sample of samples) power += sample * sample
  const dbov = 10 * Math.log10(power / samples.length || 1e-13)
  return Math.min(SILENT_LEVEL, Math.round(-dbov))
}
