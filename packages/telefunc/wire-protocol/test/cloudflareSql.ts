/// <reference types="@cloudflare/workers-types" />
export { sqliteStorage }

import { DatabaseSync, type SQLInputValue } from 'node:sqlite'

/** Cloudflare's SQLite storage API over node:sqlite, as far as the adapter uses it, recording the plan of each read. */
function sqliteStorage(): { storage: DurableObjectStorage; plans: string[] } {
  const db = new DatabaseSync(':memory:')
  const plans: string[] = []
  const sql = {
    exec(query: string, ...bindings: SQLInputValue[]) {
      if (/^\s*SELECT/i.test(query)) {
        const plan = db.prepare(`EXPLAIN QUERY PLAN ${query}`).all(...bindings) as Array<{ detail: string }>
        for (const step of plan) plans.push(step.detail)
        return cursor(db.prepare(query).all(...bindings), 0)
      }
      if (/\bRETURNING\b/i.test(query)) {
        const rows = db.prepare(query).all(...bindings)
        return cursor(rows, rows.length)
      }
      if (bindings.length === 0 && !/^\s*(INSERT|UPDATE|DELETE)/i.test(query)) {
        db.exec(query)
        return cursor([], 0)
      }
      return cursor([], Number(db.prepare(query).run(...bindings).changes))
    },
  }
  const storage = {
    sql,
    transactionSync<T>(fn: () => T): T {
      db.exec('BEGIN')
      try {
        const result = fn()
        db.exec('COMMIT')
        return result
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
    },
  }
  return { storage: storage as unknown as DurableObjectStorage, plans }
}

/** As Cloudflare's cursor: `one()` throws unless there is exactly one row; `rowsWritten` counts the rows changed. */
function cursor(rows: unknown[], rowsWritten: number) {
  return {
    toArray: () => rows,
    one: () => {
      if (rows.length !== 1) throw new Error(`Expected exactly one row from the query, got ${rows.length}`)
      return rows[0]
    },
    rowsWritten,
  }
}
