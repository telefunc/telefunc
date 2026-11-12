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
  it('announces what a text kept before its statement that failed', async () => {
    const { db } = open(TWO_ROWS)
    const stale = await watch(db, 'select * from t')
    expect(() => db.exec("update t set x = 'c' where id = 1; insert into t values (2, 'd')")).toThrow()
    expect(await stale()).toBe(1)
  })

  it('announces the rows an OR FAIL statement changed before its conflict', async () => {
    const { db } = open(TWO_ROWS)
    const stale = await watch(db, 'select * from t')
    expect(() => db.prepare("update or fail t set x = 'c'").run()).toThrow()
    expect(db.prepare('select x from t where id = 1').get()).toEqual({ x: 'c' })
    expect(await stale()).toBe(1)
  })

  it('announces the row a REPLACE in a text deleted', async () => {
    const { db } = open("create table t (id integer primary key, x text unique); insert into t values (1, 'a')")
    const stale = await watch(db, 'select * from t where id = 1')
    db.exec("replace into t (x) values ('a'); select 1")
    expect(db.prepare('select * from t where id = 1').all()).toEqual([])
    expect(await stale()).toBe(1)
  })

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

  it('announces nothing of a transaction SQLite rolled back by itself, and the writes after it', async () => {
    const { db } = open(TWO_ROWS)
    const second = await watch(db, 'select * from t where id = 2')
    const all = await watch(db, 'select * from t')
    db.exec('begin')
    db.prepare("update t set x = 'c' where id = 2").run()
    expect(() => db.prepare("insert or rollback into t values (1, 'duplicate')").run()).toThrow()
    expect(db.isTransaction).toBe(false)
    db.prepare("update t set x = 'd' where id = 1").run()
    expect([await second(), await all()]).toEqual([0, 1])
  })

  it('announces a transaction committed after its first COMMIT failed', async () => {
    const { db } = open(`pragma foreign_keys = on; create table p (id integer primary key);
      create table c (id integer primary key, p int references p (id) deferrable initially deferred)`)
    const stale = await watch(db, 'select * from c')
    db.exec('begin')
    db.prepare('insert into c values (1, 5)').run()
    expect(() => db.exec('commit')).toThrow()
    expect(db.isTransaction).toBe(true)
    db.prepare('insert into p values (5)').run()
    db.exec('commit')
    expect(await stale()).toBe(1)
  })

  it("follows the transaction state SQLite tells after a text the parser can't read", async () => {
    const { db } = open(TWO_ROWS)
    db.exec('begin; reindex')
    const rolledBack = await watch(db, 'select * from t')
    db.prepare("update t set x = 'c' where id = 1").run()
    db.exec('rollback')
    expect(await rolledBack()).toBe(0)
    db.exec('begin')
    db.exec('commit; reindex')
    const committed = await watch(db, 'select * from t')
    db.prepare("update t set x = 'c' where id = 1").run()
    expect(await committed()).toBe(1)
  })

  it("leaves changes() to the app's statements", () => {
    const { db } = open(
      `${TWO_ROWS}; create table audit (id integer);
      create trigger audited after insert on t begin insert into audit values (1); insert into audit values (2); end`,
    )
    db.prepare("insert into t values (3, 'c')").run()
    expect(db.prepare('select changes() as changes').get()).toEqual({ changes: 1 })
  })

  it('announces no change twice once a transaction rolled back', async () => {
    const { db } = open(TWO_ROWS)
    db.prepare("update t set x = 'c' where id = 1").run()
    const first = await watch(db, 'select * from t where id = 1')
    db.exec('begin')
    db.prepare("update t set x = 'd' where id = 2").run()
    expect(() => db.prepare("insert or rollback into t values (2, 'e')").run()).toThrow()
    expect(await first()).toBe(0)
  })

  it('captures a connection that ran statements before its file was wrapped', async () => {
    // Another file wrapped first: every connection's statements pass through the capture.
    open(TWO_ROWS)
    const { db, path } = open(TWO_ROWS, 'later')
    liveDatabaseOfFile(path, {})
    const stale = await watch(db, 'select * from t')
    db.prepare("update t set x = 'c' where id = 1").run()
    expect(await stale()).toBe(1)
  })

  it('announces a value of a STRICT ANY column, which compares by its own type', async () => {
    const { db } = open('create table t (id integer primary key, x any) strict')
    const stale = await watch(db, "select * from t where x < '3'")
    db.prepare('insert into t values (1, 10)').run()
    expect(db.prepare("select id from t where x < '3'").all()).toEqual([{ id: 1 }])
    expect(await stale()).toBe(1)
  })

  it("announces a value stored as another type than its column's, which compares by type first", async () => {
    const { db } = open('create table t (id integer primary key, x text, n integer)')
    const text = await watch(db, "select id from t where x <> 'a'")
    const number = await watch(db, 'select id from t where n <> 0')
    db.prepare("insert into t values (1, x'61', '')").run()
    expect(db.prepare("select typeof(x) as x, typeof(n) as n from t where x <> 'a' and n <> 0").all()).toEqual([
      { x: 'blob', n: 'text' },
    ])
    expect([await text(), await number()]).toEqual([1, 1])
  })

  it("announces a row sorting before a page's last row, stored as another type than its column's", async () => {
    const { db } = open("create table t (id integer primary key, n integer); insert into t values (1, '')")
    const stale = await watch(db, 'select id, n from t order by n limit 1')
    db.prepare('insert into t values (2, 5)').run()
    expect(db.prepare('select id from t order by n limit 1').all()).toEqual([{ id: 2 }])
    expect(await stale()).toBe(1)
  })
})
