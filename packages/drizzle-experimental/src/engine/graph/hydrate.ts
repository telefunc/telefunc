// Synchronous-cut hydration. The registry BLOCKS acquire on the seed, so a
// stateful graph is PRECISE from its first live tick — there is no warming tier and no
// coarse-during-seed window. Activation registers the graph with the router BEFORE the scan
// (activate-before-read), so any change routed during the async scan is buffered by the live graph
// and replayed exactly once — as a PK-keyed upsert against the seeded shadow — inside the
// synchronous cut. Seeding: scan each input's σ-scoped, column-pruned baseline into its shadow,
// mark it complete, then hand the shadows to the live graph, which reconciles its one-shot buffer
// and seeds the engine in ONE muted flush (no await). A seed that errors or overflows the per-input
// state bound demotes to coarse (bounded, sound). A generation aborted (retire / destroy / demote)
// before it lands is inert — a late scan completion never seeds a dead or superseded graph.

export { type HydrationExecutor, type Seed, createSeed }

import type { SeedDescriptor } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import { type ShadowIndex, createShadow, matchesResidual, pkOf, pruneRow } from './shadow.js'

/** Injected in the binding layer from the σ residual's opaque `src` handles; `graph/` imports only
 *  this interface (zero drizzle here). */
type HydrationExecutor = {
  scan(descriptor: SeedDescriptor): Promise<Row[]>
}

type SeedHooks = {
  /** The synchronous cut: completed shadows handed over; the implementer replays its one-shot
   *  buffer and seeds the engine WITHOUT awaiting. */
  onComplete(shadows: Map<string, ShadowIndex>): void
  /** State overflow or scan error → demote to coarse (bounded, sound). Never after abort. */
  onDemote(reason: string): void
}

type Seed = {
  start(): void
  /** Supersede this generation — a later scan completion becomes inert. */
  abort(): void
}

function createSeed(params: {
  descriptors: SeedDescriptor[]
  executor: HydrationExecutor
  maxStateRows: number
  hooks: SeedHooks
}): Seed {
  const { descriptors, executor, maxStateRows, hooks } = params
  let aborted = false

  async function run(): Promise<void> {
    try {
      const shadows = new Map<string, ShadowIndex>()
      for (const descriptor of descriptors) {
        const shadow = createShadow()
        for (const raw of await executor.scan(descriptor)) admit(descriptor, shadow, raw)
        if (aborted) return
        shadow.complete = true
        shadows.set(descriptor.inputId, shadow)
        if (shadow.size > maxStateRows) return hooks.onDemote('state-row-limit')
      }
      if (aborted) return
      hooks.onComplete(shadows) // the synchronous cut lives in the live graph — no await past here
    } catch {
      if (!aborted) hooks.onDemote('hydration-error')
    }
  }

  function admit(descriptor: SeedDescriptor, shadow: ShadowIndex, raw: Row): void {
    if (!matchesResidual(descriptor, raw)) return
    const pk = pkOf(descriptor, raw)
    if (pk !== undefined) shadow.put(pk, pruneRow(descriptor, raw))
  }

  return {
    start() {
      void run()
    },
    abort() {
      aborted = true
    },
  }
}
