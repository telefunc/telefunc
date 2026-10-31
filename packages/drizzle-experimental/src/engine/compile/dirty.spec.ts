import { describe, expect, it } from 'vitest'
import { D2, MultiSet, consolidate, distinct, keyBy, map, output } from '../graph/ivm.js'
import type { Predicate } from '../../ir/types.js'
import { containsUnknown, createDirtySink, dirtyFrontier } from './dirty.js'

const compare = (col: string, value: number): Predicate => ({
  kind: 'compare',
  op: '=',
  left: { kind: 'col', ref: { table: 'users', column: col } },
  right: { kind: 'lit', value },
})
const unknown: Predicate = { kind: 'unknown', tables: ['users'] }

describe('dirty — tap frontier', () => {
  it('A AND unknown → active, gate is A (tap after A)', () => {
    const frontier = dirtyFrontier({ kind: 'and', parts: [compare('team_id', 5), unknown] })
    expect(frontier.active).toBe(true)
    expect(frontier.gate).toEqual(compare('team_id', 5)) // the exact conjunct is the gate
  })

  it('A OR unknown → active, gate is TRUE (tap before)', () => {
    const frontier = dirtyFrontier({ kind: 'or', parts: [compare('team_id', 5), unknown] })
    expect(frontier.active).toBe(true)
    expect(frontier.gate).toEqual({ kind: 'true' }) // no exact top-level conjunct survives
  })

  it('NOT unknown → active, gate is TRUE (tap before)', () => {
    const frontier = dirtyFrontier({ kind: 'not', operand: unknown })
    expect(frontier.active).toBe(true)
    expect(frontier.gate).toEqual({ kind: 'true' })
  })

  it('a fully exact predicate is not active (no dirty witness)', () => {
    const frontier = dirtyFrontier({ kind: 'and', parts: [compare('team_id', 5), compare('id', 1)] })
    expect(frontier.active).toBe(false)
  })

  it('containsUnknown detects opaque and exists leaves', () => {
    expect(containsUnknown(unknown)).toBe(true)
    expect(containsUnknown({ kind: 'exists', negated: false, tables: ['x'] })).toBe(true)
    expect(containsUnknown(compare('team_id', 5))).toBe(false)
  })
})

describe('dirty — sink signal/union', () => {
  it('signal fires and reset clears', () => {
    const sink = createDirtySink()
    expect(sink.fired()).toBe(false)
    sink.signal()
    expect(sink.fired()).toBe(true)
    sink.reset()
    expect(sink.fired()).toBe(false)
  })
})

describe('dirty — bypass guarantee', () => {
  it('a witnessed retract+insert that CANCELS in the data path still fires dirty', () => {
    // Model the D1 cancellation shape: the same projected tuple is retracted and inserted in
    // one commit, so distinct/consolidate net to zero on the data path — but the dirty tap,
    // which is never filtered, still fires.
    const graph = new D2()
    const dataIn = graph.newInput<string>()
    const dirtyIn = graph.newInput<{ marker: number }>()
    const dirty = createDirtySink()

    let dataFired = false
    dataIn
      .pipe(
        keyBy((value: string) => value),
        distinct(),
        map(([, value]) => value),
        consolidate(),
      )
      .pipe(
        output((data) => {
          if (data.getInner().length > 0) dataFired = true
        }),
      )
    dirty.tap(dirtyIn)
    dirty.buildTerminal()
    graph.finalize()

    dataIn.sendData(
      new MultiSet([
        ['tuple', 1],
        ['tuple', -1],
      ]),
    ) // nets to zero
    dirtyIn.sendData(new MultiSet([[{ marker: 1 }, 1]]))
    graph.run()

    expect(dataFired).toBe(false) // data cancelled
    expect(dirty.fired()).toBe(true) // the witness bypassed the cancellation
  })
})
