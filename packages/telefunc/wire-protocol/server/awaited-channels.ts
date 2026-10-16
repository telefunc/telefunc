export { AwaitedChannels }
export type { AwaitedChannel, RecvBacklog }

import type { ChannelFrame, ReconcileOpenEntry } from '../shared-ws.js'
import type { ConnectionEntry, Wire } from './mux.js'

/** What a wire received and hasn't handled yet. */
type RecvBacklog = { bytes: number; frames: number }

/** An initial channel a RECONCILE on this wire named before the server registered it. The RECONCILED leaves it out,
 *  and an ATTACH_RESULT settles it. */
type AwaitedChannel = {
  /** The latest RECONCILE entry naming it. */
  open: ReconcileOpenEntry
  /** The wire awaiting it: the one whose RECONCILE named it, until a barrier moves it to the WebSocket. */
  entry: ConnectionEntry
  wire: Wire
  /** What the page sent it meanwhile, dispatched after its attach. */
  held: { frame: ChannelFrame; bytes: number }[]
  /** `attached` until what it holds is dispatched. `expired` once `connectTtl` passed, and kept until a RECONCILE no
   *  longer names it, so one that crossed its ATTACH_RESULT doesn't await it again. */
  phase: 'waiting' | 'attached' | 'expired'
  stopWaiting: () => void
}

/** The channels a wire awaits, by index. What it holds for them counts in its recv backlog until it's dispatched or
 *  dropped. */
class AwaitedChannels {
  private readonly byIx = new Map<number, AwaitedChannel>()

  constructor(private readonly backlog: RecvBacklog) {}

  get(ix: number): AwaitedChannel | undefined {
    return this.byIx.get(ix)
  }

  add(awaited: AwaitedChannel): void {
    this.byIx.set(awaited.open.ix, awaited)
  }

  hold(awaited: AwaitedChannel, frame: ChannelFrame, bytes: number): void {
    awaited.held.push({ frame, bytes })
    this.backlog.bytes += bytes
    this.backlog.frames++
  }

  /** Awaits it no more, and hands back what it held, to dispatch. */
  take(awaited: AwaitedChannel): AwaitedChannel['held'] {
    this.byIx.delete(awaited.open.ix)
    const { held } = awaited
    this.drop(awaited)
    return held
  }

  drop(awaited: AwaitedChannel): void {
    for (const { bytes } of awaited.held) {
      this.backlog.bytes -= bytes
      this.backlog.frames--
    }
    awaited.held = []
  }

  /** Awaits no more, and drops what it held for, each channel but those `named`. One attached meanwhile, the RECONCILE
   *  left out of the session, which ended it. */
  forgetUnnamed(named: Set<number>): void {
    for (const [ix, awaited] of this.byIx) {
      if (named.has(ix)) continue
      awaited.stopWaiting()
      this.drop(awaited)
      this.byIx.delete(ix)
    }
  }

  /** The wire `wire` awaits it from now on, with what it held. */
  move(awaited: AwaitedChannel, entry: ConnectionEntry, wire: Wire): void {
    const held = this.take(awaited)
    awaited.entry = entry
    awaited.wire = wire
    for (const { frame, bytes } of held) entry.state.awaited.hold(awaited, frame, bytes)
    entry.state.awaited.add(awaited)
  }

  stop(): void {
    for (const awaited of this.byIx.values()) awaited.stopWaiting()
    this.byIx.clear()
  }
}
