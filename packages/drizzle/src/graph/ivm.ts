// The one and only module that imports @tanstack/db-ivm. Every compile stage reaches
// the engine through this facade, so a single grep proves the seam: the
// operator set, the MultiSet/D2 primitives, and the reader types the stages need are
// re-exported here and nowhere else. `assertIvmContract` exercises the documented
// engine behaviours we depend on — join retractions, retraction-correct reduce, the
// distinct multiplicity edge, concat, topK{limit,offset}, filterBy/antiJoin — and
// throws loudly if a future engine bump changes a shape out from under us.

export {
  D2,
  MultiSet,
  map,
  filter,
  negate,
  concat,
  consolidate,
  output,
  join,
  innerJoin,
  leftJoin,
  rightJoin,
  fullJoin,
  antiJoin,
  reduce,
  distinct,
  keyBy,
  topK,
  filterBy,
  assertIvmContract,
  IVM_ENGINE_VERSION,
}
export type { IStreamBuilder, MultiSetArray, KeyValue, PipedOperator, RootStreamBuilder, KeyedStream }

import {
  D2,
  MultiSet,
  antiJoin,
  concat,
  consolidate,
  distinct,
  filter,
  filterBy,
  fullJoin,
  innerJoin,
  join,
  keyBy,
  leftJoin,
  map,
  negate,
  output,
  reduce,
  rightJoin,
  topK,
} from '@tanstack/db-ivm'
import type { IStreamBuilder, KeyValue, MultiSetArray, PipedOperator, RootStreamBuilder } from '@tanstack/db-ivm'
import { assertUsage } from '../utils/assert.js'

/** The engine golden. db-ivm is pre-1.0, so the pin is exact and this name is asserted
 *  by the contract spec against the installed package version. */
const IVM_ENGINE_VERSION = '0.1.18'

/** A keyed difference stream — the shape every join/aggregate/window stage keys into. */
type KeyedStream<K, V> = IStreamBuilder<KeyValue<K, V>>

// ── Engine contract ─────────────────────────────────────────────────
// Small graphs that pin the exact behaviours the compiler relies on. Each runs a
// self-contained D2 graph and compares the netted output against the expected
// difference; a mismatch is a broken contract with the pinned engine, surfaced loudly.

function assertIvmContract(): void {
  assertJoinRetractions()
  assertReduceRetractions()
  assertDistinctMultiplicityEdge()
  assertConcat()
  assertTopKBoundaryAndOffset()
  assertSemiAndAntiJoin()
}

/** A terminal that nets the output emitted *since the last drain* into a stable
 *  `serialized → multiplicity` map (zeros dropped) and clears its buffer — so each
 *  `run()` step is checked against just that step's difference, not a running total. */
function sink<T>(): { op: PipedOperator<T, T>; drain: () => Map<string, number> } {
  let acc: MultiSetArray<T> = []
  return {
    op: output<T>((data) => {
      for (const entry of data.getInner()) acc.push(entry)
    }),
    drain: () => {
      const out = new Map<string, number>()
      for (const [value, multiplicity] of acc) {
        const key = JSON.stringify(value)
        const sum = (out.get(key) ?? 0) + multiplicity
        if (sum === 0) out.delete(key)
        else out.set(key, sum)
      }
      acc = []
      return out
    },
  }
}

function has(result: Map<string, number>, value: unknown, multiplicity: number): boolean {
  return result.get(JSON.stringify(value)) === multiplicity
}

function assertJoinRetractions(): void {
  for (const mode of ['inner', 'left', 'right', 'full'] as const) {
    const graph = new D2()
    const left = graph.newInput<KeyValue<number, string>>()
    const right = graph.newInput<KeyValue<number, string>>()
    const out = sink<KeyValue<number, [string | null, string | null]>>()
    left.pipe(join<number, string, string, KeyValue<number, string>>(right, mode), out.op)
    graph.finalize()

    left.sendData(new MultiSet([[[1, 'L1'], 1]]))
    right.sendData(new MultiSet([[[1, 'R1'], 1]]))
    graph.run()
    assertUsage(
      has(out.drain(), [1, ['L1', 'R1']], 1),
      `db-ivm ${IVM_ENGINE_VERSION} ${mode} join did not emit the matched pair`,
    )

    // Retracting the right row must retract the matched pair; outer modes then reveal
    // the null-extended left/right row instead of silently dropping it.
    right.sendData(new MultiSet([[[1, 'R1'], -1]]))
    graph.run()
    const after = out.drain()
    assertUsage(
      has(after, [1, ['L1', 'R1']], -1),
      `db-ivm ${IVM_ENGINE_VERSION} ${mode} join did not retract on the driving row removal`,
    )
    if (mode === 'left' || mode === 'full') {
      assertUsage(
        has(after, [1, ['L1', null]], 1),
        `db-ivm ${IVM_ENGINE_VERSION} ${mode} join did not null-extend the unmatched left row`,
      )
    }
  }
}

function assertReduceRetractions(): void {
  // A per-key MIN that must reveal the next value when the current extremum is retracted.
  const graph = new D2()
  const input = graph.newInput<KeyValue<string, number>>()
  const out = sink<KeyValue<string, number>>()
  input.pipe(
    reduce<string, number, number, KeyValue<string, number>>((values) => {
      let min = Number.POSITIVE_INFINITY
      for (const [value, multiplicity] of values) if (multiplicity > 0) min = Math.min(min, value)
      return min === Number.POSITIVE_INFINITY ? [] : [[min, 1]]
    }),
    out.op,
  )
  graph.finalize()

  input.sendData(
    new MultiSet([
      [['g', 3], 1],
      [['g', 5], 1],
    ]),
  )
  graph.run()
  assertUsage(has(out.drain(), ['g', 3], 1), `db-ivm ${IVM_ENGINE_VERSION} reduce did not emit the initial min`)

  input.sendData(new MultiSet([[['g', 3], -1]]))
  graph.run()
  const after = out.drain()
  assertUsage(
    has(after, ['g', 3], -1) && has(after, ['g', 5], 1),
    `db-ivm ${IVM_ENGINE_VERSION} reduce did not reveal the next extremum after removal`,
  )
}

function assertDistinctMultiplicityEdge(): void {
  const graph = new D2()
  const input = graph.newInput<KeyValue<string, string>>()
  const out = sink<KeyValue<string, string>>()
  input.pipe(distinct(), out.op)
  graph.finalize()

  input.sendData(new MultiSet([[['k', 'v'], 1]]))
  graph.run()
  assertUsage(out.drain().size === 1, `db-ivm ${IVM_ENGINE_VERSION} distinct did not emit the first occurrence`)

  // A second identical occurrence (multiplicity 1→2) is still present — distinct must
  // emit nothing. This is the precision win the cancellation blockers rely on.
  input.sendData(new MultiSet([[['k', 'v'], 1]]))
  graph.run()
  assertUsage(
    out.drain().size === 0,
    `db-ivm ${IVM_ENGINE_VERSION} distinct emitted on a 1→2 multiplicity edge (must be silent)`,
  )
}

function assertConcat(): void {
  const graph = new D2()
  const a = graph.newInput<string>()
  const b = graph.newInput<string>()
  const out = sink<string>()
  a.pipe(concat(b), out.op)
  graph.finalize()
  a.sendData(new MultiSet([['a', 1]]))
  b.sendData(new MultiSet([['b', 1]]))
  graph.run()
  const result = out.drain()
  assertUsage(
    has(result, 'a', 1) && has(result, 'b', 1),
    `db-ivm ${IVM_ENGINE_VERSION} concat did not carry both branches`,
  )
}

function assertTopKBoundaryAndOffset(): void {
  const comparator = (x: number, y: number) => x - y
  const graph = new D2()
  const input = graph.newInput<KeyValue<null, number>>()
  const out = sink<KeyValue<null, number>>()
  input.pipe(topK(comparator, { limit: 2, offset: 1 }), out.op)
  graph.finalize()

  input.sendData(
    new MultiSet([
      [[null, 10], 1],
      [[null, 20], 1],
      [[null, 30], 1],
      [[null, 40], 1],
    ]),
  )
  graph.run()
  const window = out.drain()
  // offset 1, limit 2 over [10,20,30,40] → {20,30}
  assertUsage(
    has(window, [null, 20], 1) &&
      has(window, [null, 30], 1) &&
      !has(window, [null, 10], 1) &&
      !has(window, [null, 40], 1),
    `db-ivm ${IVM_ENGINE_VERSION} topK{limit,offset} selected the wrong window`,
  )

  // Removing an in-window element promotes the next below-window element into it.
  input.sendData(new MultiSet([[[null, 20], -1]]))
  graph.run()
  const promoted = out.drain()
  assertUsage(
    has(promoted, [null, 40], 1) && !has(promoted, [null, 20], 1),
    `db-ivm ${IVM_ENGINE_VERSION} topK did not promote the next element after an in-window removal`,
  )
}

function assertSemiAndAntiJoin(): void {
  const graph = new D2()
  const outer = graph.newInput<KeyValue<number, string>>()
  const inner = graph.newInput<KeyValue<number, null>>()
  const semi = sink<KeyValue<number, string>>()
  const anti = sink<KeyValue<number, [string, null]>>()
  outer.pipe(filterBy<number, string, KeyValue<number, string>>(inner), semi.op)
  outer.pipe(antiJoin<number, string, null, KeyValue<number, string>>(inner), anti.op)
  graph.finalize()

  outer.sendData(
    new MultiSet([
      [[1, 'a'], 1],
      [[2, 'b'], 1],
    ]),
  )
  inner.sendData(new MultiSet([[[1, null], 1]]))
  graph.run()
  assertUsage(
    has(semi.drain(), [1, 'a'], 1),
    `db-ivm ${IVM_ENGINE_VERSION} filterBy did not admit the correlated outer row`,
  )
  assertUsage(
    has(anti.drain(), [2, ['b', null]], 1),
    `db-ivm ${IVM_ENGINE_VERSION} antiJoin did not emit the uncorrelated outer row`,
  )
}
