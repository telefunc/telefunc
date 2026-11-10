export { publish }
export type { Publication, PublicationStats }

import type { LocalParticipant } from '@telefunc/room'
import { AudioSender, AUDIO_BITRATE, type AudioSenderStats } from './AudioSender.js'
import { VideoSender, type VideoSenderStats } from './VideoSender.js'
import { MEDIA_TRACK, type Catalog, type SourceCatalog } from './wire.js'

/** What of the path's rate the page offers, as GCC backs off from what arrived (draft-ietf-rmcat-gcc-02 §5.5). */
const UPLINK_SHARE = 0.85
/** How often the bitrates follow the page's uplink: a bitrate change costs no key frame. */
const BUDGET_INTERVAL_MS = 500

type PublicationStats = {
  /** In bits per second, what the page's path to the server carries: `Infinity` until measured. */
  uplinkRate: number
  video: VideoSenderStats | null
  audio: AudioSenderStats | null
}

type Publication = {
  /** Stops sending the source; its tracks stay the app's to stop. */
  stop(): void
  stats(): PublicationStats
}

/** Sends a camera, a microphone or a screen to the room, for the pages that `play()` it. `source` is a `MediaStream` or
 *  one of its tracks; `options.name` tells a second source, such as `'screen'`, from the first. Muting is the track's
 *  own: `track.enabled = false`. */
function publish(
  me: LocalParticipant,
  source: MediaStream | MediaStreamTrack,
  options: { name?: string } = {},
): Publication {
  const name = options.name ?? 'camera'
  let publisher = publishers.get(me.id)
  if (publisher === undefined) {
    publisher = new Publisher(me)
    publishers.set(me.id, publisher)
  }
  const published = publisher.add(name, source)
  return { stop: () => publisher.remove(name, published), stats: () => published.stats(me) }
}

/** This page's publishing members, by id. */
const publishers = new Map<string, Publisher>()

/** One member's sources: its catalog, its share of the uplink, and the demand for its tracks, whose new watchers each
 *  get a key frame. */
class Publisher {
  private readonly me: LocalParticipant
  private readonly sources = new Map<string, PublishedSource>()
  /** The tracks someone watches, as `onDemand()` told: a source added later starts with them. */
  private readonly watched = new Set<string | null>()
  private readonly abort = new AbortController()
  private readonly budgetTimer: ReturnType<typeof setInterval>

  constructor(me: LocalParticipant) {
    this.me = me
    const { signal } = this.abort
    me.onDemand((track, wanted) => this.onDemand(track, wanted), { signal })
    me.onWatch((track) => this.onWatch(track), { signal })
    me.onLeave(() => this.removeAll(), { signal })
    this.budgetTimer = setInterval(() => this.shareUplink(), BUDGET_INTERVAL_MS)
  }

  add(name: string, source: MediaStream | MediaStreamTrack): PublishedSource {
    this.sources.get(name)?.stop()
    const published = new PublishedSource(this.me, name, source)
    this.sources.set(name, published)
    for (const track of this.watched) published.onDemand(track, true)
    this.shareUplink()
    void this.publishCatalog()
    return published
  }

  remove(name: string, published: PublishedSource): void {
    if (this.sources.get(name) !== published) return
    published.stop()
    this.sources.delete(name)
    if (this.sources.size === 0) return this.removeAll()
    void this.publishCatalog()
  }

  private removeAll(): void {
    for (const source of this.sources.values()) source.stop()
    this.sources.clear()
    this.abort.abort()
    clearInterval(this.budgetTimer)
    publishers.delete(this.me.id)
    void this.publishCatalog()
  }

  private onDemand(track: string | null, wanted: boolean): void {
    if (wanted) this.watched.add(track)
    else this.watched.delete(track)
    for (const source of this.sources.values()) source.onDemand(track, wanted)
  }

  /** A page starts watching `track`, `null`: every track; it shows nothing before the track's next key frame. */
  private onWatch(track: string | null): void {
    for (const source of this.sources.values()) source.video?.onWatch(track)
  }

  /** Audio always goes; the video gets what is left of the page's share of its uplink, a screen first. */
  private shareUplink(): void {
    const sources = [...this.sources.values()]
    let left = this.me.uplinkRate * 8_000 * UPLINK_SHARE
    for (const { audio } of sources) if (audio !== null) left -= 2 * AUDIO_BITRATE
    sources.sort((a, b) => Number(b.name === 'screen') - Number(a.name === 'screen'))
    for (const { video } of sources) if (video !== null) left -= video.setBudget(Math.max(0, left))
  }

  /** What its page publishes, retained on its media track, so a page that starts watching reads it first. */
  private async publishCatalog(): Promise<void> {
    const sources: Record<string, SourceCatalog> = {}
    for (const [name, source] of this.sources) {
      const catalog = await source.catalog()
      if (catalog !== null) sources[name] = catalog
    }
    const catalog: Catalog = { __media: 'catalog', sources }
    await this.me.publish(catalog, { track: MEDIA_TRACK, retain: true }).catch(() => {})
  }
}

/** One source's senders: its first video track and its first audio track. */
class PublishedSource {
  readonly name: string
  readonly video: VideoSender | null
  readonly audio: AudioSender | null
  private readonly audioTrack: MediaStreamTrack | null

  constructor(me: LocalParticipant, name: string, source: MediaStream | MediaStreamTrack) {
    this.name = name
    const tracks = source instanceof MediaStream ? source.getTracks() : [source]
    const videoTrack = tracks.find((track) => track.kind === 'video') ?? null
    this.audioTrack = tracks.find((track) => track.kind === 'audio') ?? null
    this.video = videoTrack && new VideoSender({ me, name, track: videoTrack })
    this.audio = this.audioTrack && new AudioSender({ me, name, track: this.audioTrack })
  }

  /** What a page needs to play it, once its video's codec is known; null where it carries nothing to play. */
  async catalog(): Promise<SourceCatalog | null> {
    const catalog: SourceCatalog = {}
    const codec = await this.video?.codec
    if (this.video && codec) catalog.video = { codec, sizes: this.video.catalog }
    if (this.audio !== null) {
      const { sampleRate = 48_000, channelCount = 1 } = this.audioTrack!.getSettings()
      catalog.audio = { track: this.audio.track, sampleRate, numberOfChannels: channelCount }
    }
    return catalog.video || catalog.audio ? catalog : null
  }

  /** Someone watches `track` now, or no one does any more; `null`: every track. */
  onDemand(track: string | null, wanted: boolean): void {
    if (track === null || track === this.audio?.track) this.audio?.setWanted(wanted)
    this.video?.onDemand(track, wanted)
  }

  stats(me: LocalParticipant): PublicationStats {
    return {
      uplinkRate: me.uplinkRate * 8_000,
      video: this.video?.stats() ?? null,
      audio: this.audio?.stats() ?? null,
    }
  }

  stop(): void {
    this.video?.stop()
    this.audio?.stop()
  }
}
