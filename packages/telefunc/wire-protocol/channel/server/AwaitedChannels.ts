export { AwaitedChannels }
export type { AwaitedChannel, RecvBacklog }

import type { ChannelFrame } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import type { WireEntry } from './ChannelMux.js'
import type { RegistrationWaits } from './RegistrationWaits.js'

/** What a wire received and hasn't handled yet. */
type RecvBacklog = { bytes: number; frames: number }

/** An initial channel a RECONCILE named while unregistered: left out of the RECONCILED, settled by an ATTACH_RESULT. */
type AwaitedChannel = {
  /** The id of the channel it waits for: the first entry's, which a later one under its index names too. */
  readonly channelId: string
  /** The latest RECONCILE entry naming it. */
  open: ReconcileOpenEntry
  /** The wire awaiting it: the one whose RECONCILE named it, until a barrier moves it to the WebSocket. */
  entry: WireEntry
  /** What the page sent it meanwhile, dispatched after its attach. */
  held: { frame: ChannelFrame; bytes: number }[]
  /** `attached` until what it holds is dispatched; `expired` stays while a RECONCILE names it, never awaited again. */
  phase: 'waiting' | 'attached' | 'expired'
  /** The deadline of its wait for its channel's registration (see `RegistrationWaits`); null once it waits no more. */
  ttl: ReturnType<typeof setTimeout> | null
}

type AwaitedChannelsOptions = {
  /** The wire's recv backlog, which counts what it holds. */
  backlog: RecvBacklog
  /** The mux's, which every wire's awaited channels wait in. */
  waits: RegistrationWaits
}

/** The channels a wire awaits, by index; what it holds for them counts in its recv backlog. */
class AwaitedChannels {
  private readonly byIx = new Map<number, AwaitedChannel>()
  private readonly backlog: RecvBacklog
  private readonly waits: RegistrationWaits
  private attachedCount = 0

  constructor({ backlog, waits }: AwaitedChannelsOptions) {
    this.backlog = backlog
    this.waits = waits
  }

  get(ix: number): AwaitedChannel | undefined {
    return this.byIx.get(ix)
  }

  /** How many it awaits outside the session: an attached one is in it. */
  get unattached(): number {
    return this.byIx.size - this.attachedCount
  }

  /** Awaits it, with what it `held` on the wire that awaited it before. */
  add(awaited: AwaitedChannel, held: AwaitedChannel['held'] = NOTHING_HELD): void {
    for (const { frame, bytes } of held) this.hold(awaited, frame, bytes)
    this.byIx.set(awaited.open.ix, awaited)
  }

  /** Its channel is in the session from now on; it's awaited till what it held is dispatched. */
  attach(awaited: AwaitedChannel): void {
    awaited.phase = 'attached'
    this.attachedCount++
  }

  /** Awaited no more: it holds nothing, and stays while a RECONCILE names it. */
  expire(awaited: AwaitedChannel): void {
    awaited.phase = 'expired'
    this.drop(awaited)
  }

  hold(awaited: AwaitedChannel, frame: ChannelFrame, bytes: number): void {
    awaited.held.push({ frame, bytes })
    this.backlog.bytes += bytes
    this.backlog.frames++
  }

  /** Awaits it no more, and hands back what it held, to dispatch. */
  take(awaited: AwaitedChannel): AwaitedChannel['held'] {
    this.delete(awaited)
    const { held } = awaited
    this.drop(awaited)
    return held
  }

  private drop(awaited: AwaitedChannel): void {
    for (const { bytes } of awaited.held) {
      this.backlog.bytes -= bytes
      this.backlog.frames--
    }
    awaited.held = []
  }

  /** Awaits no more, and drops what it held for, each channel but those `open` names. */
  forgetUnnamed(open: ReconcileOpenEntry[]): void {
    if (this.byIx.size === 0) return
    const named = new Set(open.map((each) => each.ix))
    for (const [ix, awaited] of this.byIx) if (!named.has(ix)) this.forgetOne(awaited)
  }

  /** Awaits no more, and drops what it held for, each channel of `ixes`. */
  forget(ixes: number[]): void {
    for (const ix of ixes) {
      const awaited = this.byIx.get(ix)
      if (awaited) this.forgetOne(awaited)
    }
  }

  /** Awaits it no more, and drops what it held. */
  private forgetOne(awaited: AwaitedChannel): void {
    this.waits.stop(awaited)
    this.drop(awaited)
    this.delete(awaited)
  }

  private delete(awaited: AwaitedChannel): void {
    this.byIx.delete(awaited.open.ix)
    if (awaited.phase === 'attached') this.attachedCount--
  }

  stop(): void {
    for (const awaited of this.byIx.values()) this.waits.stop(awaited)
    this.byIx.clear()
    this.attachedCount = 0
  }
}

const NOTHING_HELD: AwaitedChannel['held'] = []
