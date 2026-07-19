// Shared test doubles for the registry specs (acquire / notify).
//
// These two files were one file and drive ONE registry through the SAME fakes. The builders below are
// deterministic by construction — pending promises drive every async path, never timers — and everything is
// observed THROUGH THE INTERFACE (`graph.state()` and notify), never an inspect() getter. Two copies of that
// would drift, and a drifting fake is a spec that has quietly stopped describing the same registry.

import type { FireResult, GraphPlan, SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import type { HydrationExecutor } from './hydrate.js'
import { type AcquireRequest, createRegistry } from './registry.js'

// ── deterministic async plumbing ────────────────────────────────────

export type Deferred<T> = { promise: Promise<T>; resolve: (value: T) => void; reject: (error: unknown) => void }
export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

export const NO_FIRE: FireResult = { invalidated: false }

export function fakeExecutor(over: Partial<HydrationExecutor> = {}): HydrationExecutor {
  // acquire BLOCKS on the seed, so the scan must resolve for the graph to reach live.
  return { scan: over.scan ?? (() => Promise.resolve<Row[]>([])) }
}

export function seed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, relationId: table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' } }
}

export function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
  return {
    seeds,
    seedInput() {},
    flushSeed() {},
    feedInput() {},
    runBatch: () => NO_FIRE,
    apply: () => NO_FIRE,
  }
}

export function coarsePlan(tables: string[]): GraphPlan {
  return {
    tables,
    stateless: true,
    coarse: true,
    instantiate: () => ({ apply: () => NO_FIRE }),
  }
}
export function statelessPlan(tables: string[]): GraphPlan {
  return {
    tables,
    stateless: true,
    coarse: false,
    instantiate: () => ({ apply: () => NO_FIRE }),
  }
}
export function statefulPlan(tables: string[], seeds: SeedDescriptor[]): GraphPlan {
  return { tables, stateless: false, coarse: false, instantiate: () => statefulFake(seeds) }
}

export function req(over: Partial<AcquireRequest> & { compilePlan: AcquireRequest['compilePlan'] }): AcquireRequest {
  return {
    instanceKey: over.instanceKey ?? 'inst-1',
    tables: over.tables ?? ['users'],
    rlsEnabled: over.rlsEnabled ?? false,
    compilePlan: over.compilePlan,
    executor: over.executor ?? fakeExecutor(),
    notify: over.notify ?? (() => {}),
  }
}

export const registryOf = (over: Partial<Parameters<typeof createRegistry>[0]> = {}) =>
  createRegistry({ maxStateRowsPerInput: over.maxStateRowsPerInput ?? 100 })
