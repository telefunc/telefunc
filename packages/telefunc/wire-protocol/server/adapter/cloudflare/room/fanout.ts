// The ordered at-most-once invariant (I5/I8) — the exact non-poisoning chain of readiness-ordering.md §3.
// One ephemeral promise chain per (incarnation, laneKey): frame N+1's attempt begins only after frame N's
// attempt SETTLED (success or failure), so a failed frame never poisons the lane and each frame's promise
// rejects only on its own failure. The chains live in the backend's ephemeral delivery host and are
// discarded on close/eviction — no state survives into a new incarnation. W2c hosts them in its facade
// because Miniflare serializes a stalled service-binding relay across lanes; W3-C owns final DO wiring.
//
// `deliver` is the handoff seam: in production the room DO RPCs each target subscriber DO
// (telefuncBroadcastDeliver); the parity fixture points it at a Node callback so the same chain drives
// real receivers. Either way the attempt is the backend's ONE handoff — never retried, never rolled back.

export type DeliveryInfo = { seq: number; timestamp: number }
export type DeliverFn = (subscriber: string, frame: Uint8Array, info: DeliveryInfo) => Promise<void>

const noop = (): void => {}

export class Fanout {
  readonly #deliver: DeliverFn
  // inc -> laneKey -> the lane's current chain tail. Nested so an incarnation's chains drop as a unit and
  // no key separator is needed.
  readonly #chains = new Map<string, Map<string, Promise<void>>>()
  readonly #attempts = new Map<string, Promise<void>>()
  #tokenSeq = 0

  constructor(deliver: DeliverFn) {
    this.#deliver = deliver
  }

  // Enqueue one frame's handoff attempt onto its lane chain. Returns a token the caller resolves later
  // via `await`, so acceptance can return before the attempt runs (no reentrant delivery inside commit).
  enqueue(inc: string, laneKey: string, targets: string[], frame: Uint8Array, info: DeliveryInfo): string {
    let lanes = this.#chains.get(inc)
    if (lanes === undefined) {
      lanes = new Map<string, Promise<void>>()
      this.#chains.set(inc, lanes)
    }
    const previous = lanes.get(laneKey) ?? Promise.resolve()
    const attempt = previous.then(() => this.#fanout(targets, frame, info))
    // Settlement gate: the next frame starts after this one settles, success OR failure.
    lanes.set(laneKey, attempt.then(noop, noop))
    const token = `d-${++this.#tokenSeq}`
    this.#attempts.set(token, attempt)
    return token
  }

  // The caller's `delivery` promise: rejects only on this frame's own handoff failure.
  async await(token: string): Promise<void> {
    const attempt = this.#attempts.get(token)
    if (attempt === undefined) return
    try {
      await attempt
    } finally {
      this.#attempts.delete(token)
    }
  }

  // Incarnation-scoped teardown: a dropped/closed generation's chains never continue into a recreation.
  clearIncarnation(inc: string): void {
    this.#chains.delete(inc)
  }

  async #fanout(targets: string[], frame: Uint8Array, info: DeliveryInfo): Promise<void> {
    await Promise.all(targets.map((subscriber) => this.#deliver(subscriber, frame, info)))
  }
}
