import { eq, exists } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'
import { isDirtySetOp } from './setOpsStage.js'

const members = pg.pgTable('members', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const admins = pg.pgTable('admins', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const guests = pg.pgTable('guests', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const visitors = pg.pgTable('visitors', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
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

describe('setOpsStage — a MIXED UNION / UNION ALL nesting never misses', () => {
  it('deleting a row still present via the UNION ALL arm invalidates', () => {
    // SQL set ops are LEFT-ASSOCIATIVE: (members UNION admins) UNION ALL guests = distinct(members ∪
    // admins) then + ALL guests. Value 1 appears in members AND guests (not admins), so the RESULT holds
    // it at multiplicity 2; deleting the members row drops the bag to 1 → the result changes → must fire.
    //
    // It fires by DIRTY (over-fire), not by computing the bag delta: the stage concatenates every arm and
    // applies ONE global distinct when a UNION arm exists, which cannot represent that bag. Mixed
    // nestings therefore degrade to dirty — sound (never-miss), deliberately imprecise. Verified by probe:
    // this graph also fires for an insert+delete of the same row in one commit, which a precise plan would
    // fold to a no-op.
    const graph = run(pg.unionAll(pg.union(memberArm(), adminArm()), guestArm()))
    graph.apply([insM({ id: 1, name: 'm' })])
    graph.apply([insG({ id: 1, name: 'g' })]) // value 1 now in distinct(members ∪ admins) AND the UNION ALL arm → bag 2
    expect(graph.apply([delM({ id: 1, name: 'm' })]).invalidated).toBe(true)
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

  it('an EXISTS INNER of the degraded main branch also still invalidates', () => {
    // The SECOND instance of the same stranded-input class. `buildBranch` registers the EXISTS inner
    // stream BEFORE `applyJoins`; on join degradation it tapped only its own base streams and returned
    // before `applyExists`, so the EXISTS inner was left built, registered and fed — observed by nothing.
    // A write to a table that appears ONLY inside the EXISTS therefore did not invalidate.
    const graph = run(
      qb
        .select({ id: members.id })
        .from(members)
        .crossJoin(admins) // no equi key → the main branch degrades
        .where(exists(qb.select({ id: guests.id }).from(guests).where(eq(guests.id, members.id))))
        .union(qb.select({ id: visitors.id }).from(visitors)),
    )
    expect(graph.apply([insG({ id: 1, name: 'g' })]).invalidated).toBe(true)
  })
})
