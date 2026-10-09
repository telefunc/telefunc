// WHICH PLAN A SHAPE COMPILES TO — the public compiler's classification, as distinct from what the
// dataflow builder does once a shape is known to need state (statefulCompiler.ts) and from what each
// operator stage computes.
//
// Every case here closes a gap found by mutating the compiler after R5's split, and every one was
// confirmed PRE-EXISTING by applying the same mutation to the pre-split tree. They survived because the
// suite tests compiled BEHAVIOUR thoroughly and the classification DECISION barely at all: a shape that
// takes the wrong path still produces plausible answers for the queries anyone happened to write down.

import { sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import type { SelectShape } from '../../ir/types.js'
import { extractQueryShape } from '../../drizzle/extract/queryShape.js'
import { coarsePlan, compileQuery } from './compile.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
})
const qb = new pg.QueryBuilder()

const shapeOf = (build: () => unknown) => extractQueryShape(build() as never, { dialect: 'pg' }) as SelectShape

describe('a set-op arm the extractor could not read degrades the WHOLE plan to coarse', () => {
  it('a non-select right side compiles coarse, not stateful', () => {
    // The dataflow builder assumes every set-op branch is a select it can construct a stream from. A
    // coarse arm carries no such structure, so compiling it statefully would build a graph over a branch
    // nobody can feed — the invalidations for that arm would simply never arrive.
    const base = shapeOf(() => qb.select().from(users))
    const withUnreadableArm: SelectShape = {
      ...base,
      setOps: [{ type: 'union', right: { kind: 'coarse', tables: ['audit'], reason: 'unreadable arm' }, orderBy: [] }],
      tables: [...base.tables, 'audit'],
    }

    const plan = compileQuery(withUnreadableArm)

    expect(plan.coarse).toBe(true)
    expect(plan.tables).toContain('audit') // …and it still watches the arm it could not read
  })

  it('a set-op whose arms ARE all selects stays precise, so the check above is not blanket coarsening', () => {
    const base = shapeOf(() => qb.select().from(users))
    const allSelects: SelectShape = {
      ...base,
      setOps: [{ type: 'union', right: base, orderBy: [] }],
    }
    expect(compileQuery(allSelects).coarse).toBe(false)
  })
})

describe('an OPAQUE group-by counts as an aggregate', () => {
  it('a GROUP BY the parser could not reduce to a column is never compiled STATELESS', () => {
    // `groupByOpaque` means the grouping key is an expression nobody modelled. Row-space evaluation
    // decides membership per row, which cannot answer a grouped query at all — so treating such a shape
    // as stateless would have the evaluator confidently reporting on groups it never computed.
    // The projection is deliberately NON-aggregate here: with `count()` in it the shape would classify as
    // an aggregate through the projection instead, and the group-by clause would never be consulted.
    const opaque = shapeOf(() => qb.select().from(users).groupBy(sql`lower(${users.teamId}::text)`))
    expect(opaque.groupByOpaque).toBe(true) // the fixture reaches the state under test…

    expect(compileQuery(opaque).stateless).toBe(false)
  })

  it('the same shape WITHOUT a group-by is stateless, so the above is attributable to the clause', () => {
    const plain = shapeOf(() => qb.select().from(users))
    expect(plain.groupByOpaque).toBe(false)
    expect(compileQuery(plain).stateless).toBe(true)
  })
})

describe('a coarse plan invalidates only for the tables it WATCHES', () => {
  it('a change on an unwatched table does not fire it', () => {
    // Sound-but-noisy rather than unsound: over-firing, not a wrong row. Pinned because "every write
    // anywhere invalidates every coarse query" is a performance cliff no behavioural test would report.
    const graph = coarsePlan(['users']).instantiate()

    expect(graph.apply([{ table: 'orders', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(false)
    // …and the positive half, so the negative above cannot be passing vacuously.
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(true)
  })
})
