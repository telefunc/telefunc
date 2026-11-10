export { play }
export type { Playback, PlaybackStats }

import type { RemoteParticipant } from '@telefunc/room'
import { AudioReceiver, type AudioReceiverStats } from './AudioReceiver.js'
import { audioContext, MemberClock } from './clock.js'
import { VideoReceiver, type VideoReceiverStats } from './VideoReceiver.js'
import { isCatalog, MEDIA_TRACK, type Catalog, type SourceCatalog } from './wire.js'

type PlaybackStats = { video: VideoReceiverStats | null; audio: AudioReceiverStats | null }

type Playback = {
  /** Stops playing; the element is left without a source. */
  stop(): void
  /** Whether the member speaks now. */
  readonly speaking: boolean
  onSpeakingChange(callback: (speaking: boolean) => void, options?: { signal?: AbortSignal }): () => void
  stats(): PlaybackStats
}

/** Plays what `member` publishes in `element`, its video and sound in step, for as long as it is in the room: the
 *  source `options.name` names, `'camera'` unless given. An `HTMLAudioElement` gets the sound alone. */
function play(
  member: RemoteParticipant,
  element: HTMLVideoElement | HTMLAudioElement,
  options: { name?: string } = {},
): Playback {
  const playback = new MemberPlayback(member, element, options.name ?? 'camera')
  return {
    stop: () => playback.stop(),
    get speaking() {
      return playback.speaking
    },
    onSpeakingChange: (callback, options) => playback.onSpeakingChange(callback, options),
    stats: () => playback.stats(),
  }
}

/** What this page holds of one member: its catalog, read once however many elements play it, and its clock. */
class MemberMedia {
  readonly clock = new MemberClock()
  catalog: Catalog | null = null
  private readonly listeners = new Set<() => void>()
  private readonly unsubscribe: () => void

  constructor(member: RemoteParticipant) {
    // A second listener on the track wouldn't get its retained catalog again, so a member has one.
    this.unsubscribe = member.subscribe((message) => this.receive(message), { track: MEDIA_TRACK })
  }

  /** Calls `listener` with each catalog from now on; the returned function stops it, and the last one's the
   *  subscription. */
  listen(id: string, listener: () => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
      if (this.listeners.size > 0) return
      this.unsubscribe()
      members.delete(id)
    }
  }

  private receive(message: unknown): void {
    if (!isCatalog(message)) return
    this.catalog = message
    for (const listener of this.listeners) listener()
  }
}

const members = new Map<string, MemberMedia>()

class MemberPlayback {
  speaking = false
  private readonly member: RemoteParticipant
  private readonly element: HTMLVideoElement | HTMLAudioElement
  private readonly name: string
  private readonly media: MemberMedia
  private readonly abort = new AbortController()
  private readonly stopListening: () => void
  private readonly speakingListeners = new Set<(speaking: boolean) => void>()
  private source: SourceCatalog | null = null
  private audio: AudioReceiver | null = null
  private audioOutput: MediaStreamAudioDestinationNode | null = null
  private video: VideoReceiver | null = null

  constructor(member: RemoteParticipant, element: HTMLVideoElement | HTMLAudioElement, name: string) {
    this.member = member
    this.element = element
    this.name = name
    let media = members.get(member.id)
    if (media === undefined) {
      media = new MemberMedia(member)
      members.set(member.id, media)
    }
    this.media = media
    this.stopListening = media.listen(member.id, () => this.update())
    member.onLeave(() => this.stop(), { signal: this.abort.signal })
    this.update()
  }

  onSpeakingChange(callback: (speaking: boolean) => void, options?: { signal?: AbortSignal }): () => void {
    if (options?.signal?.aborted) return () => {}
    this.speakingListeners.add(callback)
    const remove = () => void this.speakingListeners.delete(callback)
    options?.signal?.addEventListener('abort', remove, { once: true })
    return remove
  }

  stats(): PlaybackStats {
    return { video: this.video?.stats() ?? null, audio: this.audio?.stats() ?? null }
  }

  stop(): void {
    if (this.abort.signal.aborted) return
    this.abort.abort()
    this.stopListening()
    this.stopReceivers()
    this.element.srcObject = null
  }

  /** The member's catalog changed: what this element plays follows it. */
  private update(): void {
    const source = this.media.catalog?.sources[this.name] ?? null
    if (JSON.stringify(source) === JSON.stringify(this.source)) return
    this.stopReceivers()
    this.source = source
    if (source === null) return
    const tracks: MediaStreamTrack[] = []
    if (source.audio) {
      const context = audioContext()
      this.audioOutput = context.createMediaStreamDestination()
      this.audio = new AudioReceiver({
        member: this.member,
        catalog: source.audio,
        clock: this.media.clock,
        context,
        output: this.audioOutput,
        onSpeaking: (speaking) => this.setSpeaking(speaking),
      })
      tracks.push(...this.audioOutput.stream.getAudioTracks())
    }
    if (source.video && this.element instanceof HTMLVideoElement) {
      const audio = this.audio
      this.video = new VideoReceiver({
        member: this.member,
        catalog: source.video,
        clock: this.media.clock,
        element: this.element,
        audio: () => audio,
      })
      tracks.push(this.video.track)
    }
    this.element.srcObject = new MediaStream(tracks)
    void this.element.play().catch(() => {})
  }

  private stopReceivers(): void {
    this.audio?.stop()
    this.video?.stop()
    this.audioOutput?.disconnect()
    this.audio = this.video = this.audioOutput = null
    this.setSpeaking(false)
  }

  private setSpeaking(speaking: boolean): void {
    if (speaking === this.speaking) return
    this.speaking = speaking
    for (const listener of this.speakingListeners) listener(speaking)
  }
}
