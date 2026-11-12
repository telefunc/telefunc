// node:sqlite through the capture every synchronous SQLite driver shares (better-sqlite3 and bun:sqlite too): a live
// read goes stale when a committed write changes what it read, and only then.

import { AsyncLocalStorage } from 'node:async_hooks'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { REQUEST_CONTEXT } from 'telefunc'
import { afterAll, describe, expect, it } from 'vitest'
import { liveDatabaseOfFile } from '../database.js'
import { readLive } from '../liveQuery.js'

const folder = mkdtempSync(join(tmpdir(), 'live-sqlite-'))
afterAll(() => rmSync(folder, { recursive: true, force: true }))
const telefunc = (globalThis as any)._telefunc
const calls: AsyncLocalStorage<object> = (telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

let files = 0
/** A connection on a new file, which `setup` creates; wrapped before it, unless `wrap` is `'later'`. */
function open(setup: string, wrap: 'now' | 'later' = 'now'): { db: DatabaseSync; path: string } {
  const path = join(folder, `${++files}.sqlite`)
  if (wrap === 'now') liveDatabaseOfFile(path, {})
  const db = new DatabaseSync(path)
  db.exec(setup)
  return { db, path }
}

/** Reads `text` live, as a telefunction does, and counts the stale signals its result gets. */
async function watch(db: DatabaseSync, text: string): Promise<() => Promise<number>> {
  const cell = await calls.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
    readLive(async () => db.prepare(text).all(), 'liveDatabaseOfFile()'),
  )
  let stale = 0
  cell.onStale(() => stale++)
  cell.send()
  return async () => {
    await new Promise((resolve) => setTimeout(resolve, 50))
    return stale
  }
}

const TWO_ROWS = "create table t (id integer primary key, x text unique); insert into t values (1, 'a'), (2, 'b')"

describe('SQLite capture', () => {
  it('announces a savepoint outside a transaction once released, and nothing it rolled back', async () => {
    const { db } = open(TWO_ROWS)
    const stale = await watch(db, 'select * from t')
    db.exec('savepoint app')
    db.prepare("update t set x = 'c' where id = 1").run()
    expect(await stale()).toBe(0)
    db.exec('rollback to app')
    db.exec('release app')
    expect(await stale()).toBe(0)
    db.exec('savepoint app')
    db.prepare("update t set x = 'c' where id = 1").run()
    db.exec('release app')
    expect(await stale()).toBe(1)
  })
})
