import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'
import { isDirtySetOp } from './setOpsStage.js'

const members = pg.pgTable('members', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const admins = pg.pgTable('admins', { id: pg.integer('id').primaryKey(), name: pg.text('name') })
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const insM = (row: Record<string, unknown>): Change => ({ table: 'members', kind: 'insert', new: row })
const insA = (row: Record<string, unknown>): Change => ({ table: 'admins', kind: 'insert', new: row })

const memberArm = () => qb.select({ id: members.id }).from(members)
const adminArm = () => qb.select({ id: admins.id }).from(admins)

describe('setOpsStage — UNION ALL / UNION (T4.B7)', () => {
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

describe('setOpsStage — INTERSECT / EXCEPT dirty from either branch (T4.B7)', () => {
  it('INTERSECT taps dirty from a change in either branch', () => {
    const graph = run(pg.intersect(memberArm(), adminArm()))
    expect(graph.apply([insM({ id: 1, name: 'm' })]).dirty).toBe(true)
    expect(graph.apply([insA({ id: 2, name: 'a' })]).dirty).toBe(true)
  })

  it('EXCEPT taps dirty from either branch', () => {
    const graph = run(pg.except(memberArm(), adminArm()))
    expect(graph.apply([insM({ id: 1, name: 'm' })]).dirty).toBe(true)
    expect(graph.apply([insA({ id: 2, name: 'a' })]).dirty).toBe(true)
  })

  it('classifies the set-op kinds', () => {
    expect(isDirtySetOp('union')).toBe(false)
    expect(isDirtySetOp('unionAll')).toBe(false)
    expect(isDirtySetOp('intersect')).toBe(true)
    expect(isDirtySetOp('exceptAll')).toBe(true)
  })
})
