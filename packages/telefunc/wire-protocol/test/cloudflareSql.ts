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
        const rows = db.prepare(query).all(...bindings)
        return { toArray: () => rows }
      }
      if (bindings.length === 0) db.exec(query)
      else db.prepare(query).run(...bindings)
      return { toArray: () => [] }
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
