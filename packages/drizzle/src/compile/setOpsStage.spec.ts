import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'
import { isDirtySetOp } from './setOpsStage.js'

const members = pg.pgTable('members', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const admins = pg.pgTable('admins', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const guests = pg.pgTable('guests', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const insM = (row: Record<string, unknown>): Change => ({ table: 'members', kind: 'insert', new: row })
const insA = (row: Record<string, unknown>): Change => ({ table: 'admins', kind: 'insert', new: row })
const delM = (row: Record<string, unknown>): Change => ({ table: 'members', kind: 'delete', old: row })
const insG = (row: Record<string, unknown>): Change => ({ table: 'guests', kind: 'insert', new: row })

const memberArm = () => qb.select({ id: members.id }).from(members)
const adminArm = () => qb.select({ id: admins.id }).from(admins)
const guestArm = () => qb.select({ id: guests.id }).from(guests)

describe('setOpsStage — UNION ALL / UNION', () => {
  it('UNION ALL fires from a change in EITHER branch', () => {
    const graph = run(pg.unionAll(memberArm(), adminArm()))
    expect(graph.apply([insM({ id: 1, name: 'm' })]).invalidated).toBe(true)
    expect(graph.apply([insA({ id: 2, name: 'a' })]).invalidated).toBe(true)
  })

  it('UNION (distinct) fires when a new value appears and stays silent for a cross-branch duplicate', () => {
    const graph = run(pg.union(memberArm(), adminArm()))
    expect(graph.apply([insM({ id: 1, name: 'm' })]).invalidated).toBe(true) // value 1 appears
    // admin with the same id 1: UNION already contains 1 (multiplicity 1→2) → distinct is silent
    expect(graph.apply([insA({ id: 1, name: 'a' })]).invalidated).toBe(false)
    expect(graph.apply([insA({ id: 9, name: 'b' })]).invalidated).toBe(true) // new value 9
  })
})

describe('setOpsStage — mixed UNION / UNION ALL global distinct is wrong SQL semantics (BLOCKER #2)', () => {
  it('(members UNION admins) UNION ALL members: deleting a row still present via UNION ALL misses the bag change', () => {
    // SQL set ops are LEFT-ASSOCIATIVE: (members UNION admins) UNION ALL guests = distinct(members ∪
    // admins) then + ALL guests. Value 1 appears in members AND guests (not admins), so the RESULT
    // holds it at multiplicity 2 (once from the distinct, once from the UNION ALL arm); deleting the
    // members row drops the bag to 1 → the result changes → must fire. The stage concatenates every
    // arm and applies ONE global distinct (because a UNION arm exists), collapsing the value to
    // multiplicity 1, so the delete produces no delta → a false negative.
    const graph = run(pg.unionAll(pg.union(memberArm(), adminArm()), guestArm()))
    graph.apply([insM({ id: 1, name: 'm' })])
    graph.apply([insG({ id: 1, name: 'g' })]) // value 1 now in distinct(members ∪ admins) AND the UNION ALL arm → bag 2
    const fired = graph.apply([delM({ id: 1, name: 'm' })]).invalidated
    expect(fired).toBe(true) // FAILS today: global distinct still shows {1} (from guests) → no delta → MISS
  })
})

describe('setOpsStage — INTERSECT / EXCEPT dirty from either branch', () => {
  it('INTERSECT taps dirty from a change in either branch', () => {
    const graph = run(pg.intersect(memberArm(), adminArm()))
    expect(graph.apply([insM({ id: 1, name: 'm' })]).invalidated).toBe(true)
    expect(graph.apply([insA({ id: 2, name: 'a' })]).invalidated).toBe(true)
  })

  it('EXCEPT taps dirty from either branch', () => {
    const graph = run(pg.except(memberArm(), adminArm()))
    expect(graph.apply([insM({ id: 1, name: 'm' })]).invalidated).toBe(true)
    expect(graph.apply([insA({ id: 2, name: 'a' })]).invalidated).toBe(true)
  })

  it('classifies the set-op kinds', () => {
    expect(isDirtySetOp('union')).toBe(false)
    expect(isDirtySetOp('unionAll')).toBe(false)
    expect(isDirtySetOp('intersect')).toBe(true)
    expect(isDirtySetOp('exceptAll')).toBe(true)
  })
})

// REGRESSION — a MISSED invalidation (not a safe over-fire), on a plan that is otherwise precise.
//
// When the main branch degrades internally it taps its own streams and returns `undefined`. `buildSetOps`
// then returned early, so every successfully-built ARM was left untapped: its inputs were registered and
// fed, but nothing consumed the resulting stream — the caller only pipes a defined stream, and no dirty tap
// was installed. A change to an arm-only table therefore reported `invalidated: false`.
//
// Reachable from ordinary Drizzle: a cross join has no equi key, so the main branch degrades, and a shape
// carrying set-ops is stateful-planned without consulting join exactness.
describe('setOpsStage — a degraded MAIN branch must not silence the arms', () => {
  // members ⨯ admins (no equi key → the main branch degrades) UNION guests
  const crossJoinedMain = () => qb.select({ id: members.id }).from(members).crossJoin(admins).union(guestArm())

  it('a change to an ARM-ONLY table still invalidates', () => {
    const graph = run(crossJoinedMain())
    expect(graph.apply([insG({ id: 1, name: 'g' })]).invalidated).toBe(true)
  })

  it('the degraded main branch still invalidates from its own tables', () => {
    const graph = run(crossJoinedMain())
    expect(graph.apply([insM({ id: 2, name: 'm' })]).invalidated).toBe(true)
    expect(graph.apply([insA({ id: 3, name: 'a' })]).invalidated).toBe(true)
  })
})
