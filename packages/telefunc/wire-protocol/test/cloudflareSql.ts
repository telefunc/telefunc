export { sqliteStorage }

import { DatabaseSync, type SQLInputValue, type SQLOutputValue } from 'node:sqlite'
import type { DurableStorage, SqlBinding, SqlCursor, SqlRow, SqlValue } from '../adapter/cloudflare/sqlStorage.js'

/** A Durable Object's SQLite storage over node:sqlite, as far as Telefunc uses it: a blob is bound and read as
 *  Cloudflare's storage does, as an ArrayBuffer. Records the plan of each read. */
function sqliteStorage(): { storage: DurableStorage & { getAlarm(): Promise<number | null> }; plans: string[] } {
  const db = new DatabaseSync(':memory:')
  const plans: string[] = []
  const sql = {
    exec(query: string, ...values: SqlBinding[]): SqlCursor {
      const bindings = values.map(binding)
      if (/^\s*SELECT/i.test(query)) {
        for (const step of db.prepare(`EXPLAIN QUERY PLAN ${query}`).all(...bindings)) plans.push(planDetail(step))
        return cursor(
          db
            .prepare(query)
            .all(...bindings)
            .map(row),
          0,
        )
      }
      if (/\bRETURNING\b/i.test(query)) {
        const rows = db
          .prepare(query)
          .all(...bindings)
          .map(row)
        return cursor(rows, rows.length)
      }
      if (bindings.length === 0 && !/^\s*(INSERT|UPDATE|DELETE)/i.test(query)) {
        db.exec(query)
        return cursor([], 0)
      }
      return cursor([], Number(db.prepare(query).run(...bindings).changes))
    },
  }
  let alarm: number | null = null
  const storage = {
    sql,
    getAlarm: async () => alarm,
    setAlarm: async (at: number) => void (alarm = at),
    deleteAlarm: async () => void (alarm = null),
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
  return { storage, plans }
}

/** As Cloudflare's cursor: `one()` throws unless there is exactly one row; `rowsWritten` counts the rows changed. */
function cursor(rows: SqlRow[], rowsWritten: number): SqlCursor {
  return {
    toArray: () => rows,
    one: () => {
      const [only] = rows
      if (only === undefined || rows.length !== 1) {
        throw new Error(`Expected exactly one row from the query, got ${rows.length}`)
      }
      return only
    },
    rowsWritten,
  }
}

function binding(value: SqlBinding): SQLInputValue {
  if (value instanceof ArrayBuffer) return blob(new Uint8Array(value))
  return value instanceof Uint8Array ? blob(value) : value
}

/** `bytes` as node:sqlite binds them as a blob: Node 22's binds an empty view of an empty ArrayBuffer as NULL. */
function blob(bytes: Uint8Array): Uint8Array {
  return bytes.byteLength === 0 ? new Uint8Array(0) : bytes
}

/** A node:sqlite row as Cloudflare's storage gives it: a blob as an ArrayBuffer of its own. */
function row(read: Record<string, SQLOutputValue>): SqlRow {
  const values: SqlRow = {}
  for (const [column, value] of Object.entries(read)) values[column] = sqlValue(column, value)
  return values
}

function sqlValue(column: string, value: SQLOutputValue): SqlValue {
  if (value instanceof Uint8Array) return new Uint8Array(value).buffer
  if (typeof value === 'bigint')
    throw new Error(`The column '${column}' read a bigint, which Cloudflare's storage never does`)
  return value
}

function planDetail(step: Record<string, SQLOutputValue>): string {
  const { detail } = step
  if (typeof detail !== 'string') throw new Error('A query plan step has no detail')
  return detail
}
