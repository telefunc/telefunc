// Two D1 databases with the same tables, on this process's broadcast: a write to one refetches the other's reads
// unless each has a `name` of its own.

import { AsyncLocalStorage } from 'node:async_hooks'
import { DatabaseSync, type StatementSync } from 'node:sqlite'
import { REQUEST_CONTEXT } from 'telefunc'
import { describe, expect, it } from 'vitest'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'

const telefunc = (globalThis as any)._telefunc
const calls: AsyncLocalStorage<object> = (telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

/** A stand-in for a Worker's D1 binding (`env.DB`), on a node:sqlite database in memory: the parts `@telefunc/live`
 *  calls. */
function d1Database() {
  const db = new DatabaseSync(':memory:')
  db.exec("create table t (id integer primary key, x text); insert into t values (1, 'a')")
  const statements = new WeakMap<object, (arrays: boolean) => { names: string[]; rows: unknown[] }>()
  const statementOf = (text: string, values: unknown[]) => {
    const rowsOf = (arrays: boolean) => {
      const prepared: StatementSync = db.prepare(text)
      prepared.setReturnArrays(arrays)
      return { names: prepared.columns().map((c) => c.name), rows: prepared.all(...(values as [])) }
    }
    const statement = {
      bind: (...bound: unknown[]) => statementOf(text, bound),
      all: async () => ({ results: rowsOf(false).rows }),
      run: async () => ({ results: rowsOf(false).rows }),
      first: async () => rowsOf(false).rows[0] ?? null,
      raw: async (options?: { columnNames?: boolean }) => {
        const { names, rows } = rowsOf(true)
        return options?.columnNames ? [names, ...rows] : rows
      },
    }
    statements.set(statement, rowsOf)
    return statement
  }
  return {
    prepare: (text: string) => statementOf(text, []),
    batch: async (batched: object[]) => {
      db.exec('BEGIN')
      try {
        const results = batched.map((statement) => ({ results: statements.get(statement)!(false).rows }))
        db.exec('COMMIT')
        return results
      } catch (err) {
        db.exec('ROLLBACK')
        throw err
      }
    },
    exec: async (text: string) => db.exec(text),
  }
}

describe('two D1 databases with the same tables', () => {
  it.each([
    { label: 'without names', one: undefined, other: undefined, signals: 1 },
    { label: 'named alike', one: 'tenant', other: 'tenant', signals: 1 },
    { label: 'named apart', one: 'tenant-a', other: 'tenant-b', signals: 0 },
  ])("$label: a write to one gives the other's read $signals stale signal(s)", async ({ one, other, signals }) => {
    const [a, b] = [d1Database(), d1Database()]
    liveDatabaseOf(a, { name: one })
    liveDatabaseOf(b, { name: other })
    const cell = await calls.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(async () => (await b.prepare('select * from t where id = ?').bind(1).all()).results, 'the spec'),
    )
    let stale = 0
    cell.onStale(() => stale++)
    cell.send()
    await a.prepare("update t set x = 'b' where id = ?").bind(1).run()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(stale).toBe(signals)
  })
})
