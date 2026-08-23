import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join as joinPath } from 'node:path'
import { antiJoin } from '@tanstack/db-ivm'
import { describe, expect, it } from 'vitest'
import {
  D2,
  IVM_ENGINE_VERSION,
  type KeyValue,
  MultiSet,
  type MultiSetArray,
  concat,
  distinct,
  filterBy,
  join,
  output,
  reduce,
  topK,
} from './ivm.js'

const requireFrom = createRequire(import.meta.url)

/** The installed @tanstack/db-ivm version, read from its package.json (the engine golden). */
function ivmVersion(): string {
  const entry = requireFrom.resolve('@tanstack/db-ivm')
  let dir = dirname(entry)
  for (let i = 0; i < 8; i++) {
    try {
      const pkg = JSON.parse(readFileSync(joinPath(dir, 'package.json'), 'utf8'))
      if (pkg.name === '@tanstack/db-ivm') return pkg.version
    } catch {}
    dir = dirname(dir)
  }
  throw new Error('@tanstack/db-ivm package.json not found')
}

/** Run one difference-stream fixture: build a graph, feed a script of sends, netting the
 *  terminal output into a `serialized → multiplicity` map after every `run()` step. */
function net(entries: MultiSetArray<unknown>): Map<string, number> {
  const out = new Map<string, number>()
  for (const [value, multiplicity] of entries) {
    const key = JSON.stringify(value)
    const sum = (out.get(key) ?? 0) + multiplicity
    if (sum === 0) out.delete(key)
    else out.set(key, sum)
  }
  return out
}

describe('graph/ivm — engine pin', () => {
  it('db-ivm is installed at the exact pinned engine golden 0.1.18', () => {
    expect(IVM_ENGINE_VERSION).toBe('0.1.18')
    expect(ivmVersion()).toBe(IVM_ENGINE_VERSION)
  })
})

describe('graph/ivm — engine contract tests', () => {
  it('inner join emits the matched pair and retracts it on the driving removal', () => {
    const graph = new D2()
    const left = graph.newInput<KeyValue<number, string>>()
    const right = graph.newInput<KeyValue<number, string>>()
    const collected: MultiSetArray<unknown> = []
    left.pipe(
      join<number, string, string, KeyValue<number, string>>(right, 'inner'),
      output((data) => {
        for (const e of data.getInner()) collected.push(e)
      }),
    )
    graph.finalize()
    left.sendData(new MultiSet([[[1, 'L'], 1]]))
    right.sendData(new MultiSet([[[1, 'R'], 1]]))
    graph.run()
    expect(net(collected).get(JSON.stringify([1, ['L', 'R']]))).toBe(1)

    left.sendData(new MultiSet([[[1, 'L'], -1]]))
    graph.run()
    expect(net(collected).get(JSON.stringify([1, ['L', 'R']]))).toBeUndefined()
  })

  it('reduce MIN reveals the next extremum when the current minimum is removed', () => {
    const graph = new D2()
    const input = graph.newInput<KeyValue<string, number>>()
    const collected: MultiSetArray<unknown> = []
    input.pipe(
      reduce<string, number, number, KeyValue<string, number>>((values) => {
        let min = Number.POSITIVE_INFINITY
        for (const [value, multiplicity] of values) if (multiplicity > 0) min = Math.min(min, value)
        return min === Number.POSITIVE_INFINITY ? [] : [[min, 1]]
      }),
      output((data) => {
        for (const e of data.getInner()) collected.push(e)
      }),
    )
    graph.finalize()
    input.sendData(
      new MultiSet([
        [['g', 3], 1],
        [['g', 7], 1],
      ]),
    )
    graph.run()
    expect(net(collected).get(JSON.stringify(['g', 3]))).toBe(1)
    input.sendData(new MultiSet([[['g', 3], -1]]))
    graph.run()
    const after = net(collected)
    expect(after.get(JSON.stringify(['g', 3]))).toBeUndefined()
    expect(after.get(JSON.stringify(['g', 7]))).toBe(1)
  })

  it('distinct is silent on a 1→2 multiplicity edge but fires on 1→0', () => {
    const graph = new D2()
    const input = graph.newInput<KeyValue<string, string>>()
    const collected: MultiSetArray<unknown> = []
    input.pipe(
      distinct(),
      output((data) => {
        for (const e of data.getInner()) collected.push(e)
      }),
    )
    graph.finalize()
    input.sendData(new MultiSet([[['k', 'v'], 1]]))
    graph.run()
    expect(net(collected).size).toBe(1)

    collected.length = 0
    input.sendData(new MultiSet([[['k', 'v'], 1]]))
    graph.run()
    expect(net(collected).size).toBe(0)

    collected.length = 0
    input.sendData(new MultiSet([[['k', 'v'], -2]]))
    graph.run()
    expect(net(collected).size).toBe(1)
  })

  it('topK selects the {limit,offset} window and promotes on in-window removal', () => {
    const graph = new D2()
    const input = graph.newInput<KeyValue<null, number>>()
    const collected: MultiSetArray<unknown> = []
    input.pipe(
      topK<null, number, KeyValue<null, number>>((x, y) => x - y, { limit: 2, offset: 1 }),
      output((data) => {
        for (const e of data.getInner()) collected.push(e)
      }),
    )
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
    const window = net(collected)
    expect(window.get(JSON.stringify([null, 20]))).toBe(1)
    expect(window.get(JSON.stringify([null, 30]))).toBe(1)
    expect(window.get(JSON.stringify([null, 10]))).toBeUndefined()

    input.sendData(new MultiSet([[[null, 20], -1]]))
    graph.run()
    const promoted = net(collected)
    expect(promoted.get(JSON.stringify([null, 40]))).toBe(1)
    expect(promoted.get(JSON.stringify([null, 20]))).toBeUndefined()
  })

  it('concat carries both branches', () => {
    const graph = new D2()
    const a = graph.newInput<string>()
    const b = graph.newInput<string>()
    const collected: MultiSetArray<unknown> = []
    a.pipe(
      concat(b),
      output((data) => {
        for (const e of data.getInner()) collected.push(e)
      }),
    )
    graph.finalize()
    a.sendData(new MultiSet([['a', 1]]))
    b.sendData(new MultiSet([['b', 1]]))
    graph.run()
    const result = net(collected)
    expect(result.get(JSON.stringify('a'))).toBe(1)
    expect(result.get(JSON.stringify('b'))).toBe(1)
  })

  it('filterBy admits the correlated outer row; antiJoin emits the uncorrelated one', () => {
    const graph = new D2()
    const outer = graph.newInput<KeyValue<number, string>>()
    const inner = graph.newInput<KeyValue<number, null>>()
    const semi: MultiSetArray<unknown> = []
    const anti: MultiSetArray<unknown> = []
    outer.pipe(
      filterBy<number, string, KeyValue<number, string>>(inner),
      output((data) => {
        for (const e of data.getInner()) semi.push(e)
      }),
    )
    outer.pipe(
      antiJoin<number, string, null, KeyValue<number, string>>(inner),
      output((data) => {
        for (const e of data.getInner()) anti.push(e)
      }),
    )
    graph.finalize()
    outer.sendData(
      new MultiSet([
        [[1, 'a'], 1],
        [[2, 'b'], 1],
      ]),
    )
    inner.sendData(new MultiSet([[[1, null], 1]]))
    graph.run()
    expect(net(semi).get(JSON.stringify([1, 'a']))).toBe(1) // outer key 1 correlated → kept
    expect(net(anti).get(JSON.stringify([2, ['b', null]]))).toBe(1) // outer key 2 uncorrelated → anti-emitted
  })
})
