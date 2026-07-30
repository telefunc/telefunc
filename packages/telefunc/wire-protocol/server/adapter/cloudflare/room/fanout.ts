// The ordered at-most-once invariant (I5/I8) — the exact non-poisoning chain of readiness-ordering.md §3.
// One ephemeral promise chain per (incarnation, laneKey): frame N+1's attempt begins only after frame N's
// attempt SETTLED (success or failure), so a failed frame never poisons the lane and each frame's promise
// rejects only on its own failure. The chains live at the unique room authority and are discarded with
// their incarnation, so no state survives recreation and no facade-local tail can reorder a shared room.
//
// `deliver` is the handoff seam: in production the room DO RPCs each target session shard
// (`telefuncRoomDeliver`). The attempt is the backend's ONE handoff — never retried or rolled back.

export type DeliveryInfo = { roomId: string; inc: string; laneKey: string; seq: number; timestamp: number }
export type RouteTarget = { subscriberDoId: string; leaseId: string; generationToken: string }
export type DeliverFn = (target: RouteTarget, frame: Uint8Array, info: DeliveryInfo) => Promise<void>

const noop = (): void => {}

export class Fanout {
  private readonly _deliver: DeliverFn
  private readonly _defer: (resume: () => void) => void
  // inc -> laneKey -> the lane's current chain tail. Nested so an incarnation's chains drop as a unit and
  // no key separator is needed.
  private readonly _chains = new Map<string, Map<string, Promise<void>>>()
  private readonly _incarnationFences = new Map<string, { active: boolean }>()
  private readonly _attempts = new Map<string, Promise<void>>()
  private _tokenSeq = 0

  constructor(deliver: DeliverFn, defer: (resume: () => void) => void = queueMicrotask) {
    this._deliver = deliver
    this._defer = defer
  }

  // Enqueue one frame's handoff attempt onto its lane chain. Returns a token the caller resolves later
  // via `await`, so acceptance can return before the attempt runs (no reentrant delivery inside commit).
  enqueue(inc: string, laneKey: string, targets: RouteTarget[], frame: Uint8Array, info: DeliveryInfo): string {
    let lanes = this._chains.get(inc)
    if (lanes === undefined) {
      lanes = new Map<string, Promise<void>>()
      this._chains.set(inc, lanes)
    }
    // Acceptance owns immutable delivery inputs. A caller may reuse or mutate its buffer as soon as
    // commit returns; no deferred attempt can observe those later writes.
    const acceptedFrame = new Uint8Array(frame)
    const acceptedTargets = targets.map((target) => ({ ...target }))
    let fence = this._incarnationFences.get(inc)
    if (fence === undefined) {
      fence = { active: true }
      this._incarnationFences.set(inc, fence)
    }
    const previous = lanes.get(laneKey) ?? Promise.resolve()
    const attempt = previous
      .then(() => new Promise<void>((resolve) => this._defer(resolve)))
      .then(() => {
        if (!fence.active) throw new Error('Cloudflare Room delivery cancelled before handoff')
        return this._fanout(acceptedTargets, acceptedFrame, info)
      })
    // Settlement gate: the next frame starts after this one settles, success OR failure.
    lanes.set(laneKey, attempt.then(noop, noop))
    const token = `d-${++this._tokenSeq}`
    this._attempts.set(token, attempt)
    return token
  }

  // The caller's `delivery` promise: rejects only on this frame's own handoff failure.
  async await(token: string): Promise<void> {
    const attempt = this._attempts.get(token)
    if (attempt === undefined) throw new Error('Cloudflare Room delivery has an unknown delivery token')
    try {
      await attempt
    } finally {
      this._attempts.delete(token)
    }
  }

  // Incarnation-scoped teardown: a dropped/closed generation's chains never continue into a recreation.
  clearIncarnation(inc: string): void {
    const fence = this._incarnationFences.get(inc)
    if (fence !== undefined) fence.active = false
    this._incarnationFences.delete(inc)
    this._chains.delete(inc)
  }

  private async _fanout(targets: RouteTarget[], frame: Uint8Array, info: DeliveryInfo): Promise<void> {
    const outcomes = await Promise.allSettled(targets.map((target) => this._deliver(target, frame, info)))
    const failures = outcomes.filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected')
    if (failures.length === 1) throw failures[0]!.reason
    if (failures.length > 1) {
      throw new AggregateError(
        failures.map((failure) => failure.reason),
        'Cloudflare Room fanout failed',
      )
    }
  }
}
