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

/** The texts node:sqlite refused to prepare, ours among them: the capture prepares through `DatabaseSync.prototype`,
 *  as it was before the first file was wrapped. */
const refused: string[] = []
const { prepare } = DatabaseSync.prototype
DatabaseSync.prototype.prepare = function (this: DatabaseSync, text: string, ...options: unknown[]) {
  try {
    return (prepare as (text: string, ...options: unknown[]) => ReturnType<typeof prepare>).call(this, text, ...options)
  } catch (err) {
    refused.push(text)
    throw err
  }
} as typeof prepare

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
    db.prepare("update t set x = 'd' where id = 3 and changes() = 1").run()
    expect(db.prepare('select x from t where id = 3').get()).toEqual({ x: 'd' })
  })

  it('announces a write of up to 1,000 rows row by row, and a larger one as changing its tables', async () => {
    const { db } = open(`create table t (id integer primary key, x int);
      with recursive n (i) as (select 1 union all select i + 1 from n where i < 1002) insert into t select i, 0 from n`)
    const last = await watch(db, 'select * from t where id = 1002')
    db.prepare('update t set x = 1 where id <= 1000').run()
    expect(await last()).toBe(0)
    db.prepare('update t set x = 2 where id <= 1001').run()
    expect(await last()).toBe(1)
  })

  it('announces a write whose first rows the change log no longer holds', async () => {
    // Each row also changes four rows of a temporary table, which isn't logged: the log keeps the last of its rows only.
    const { db } = open(`create table t (id integer primary key, x int); create temp table scratch (x int);
      create temp trigger copied after update on t begin insert into scratch values (1), (2), (3), (4); end;
      with recursive n (i) as (select 1 union all select i + 1 from n where i < 900) insert into t select i, 0 from n`)
    const first = await watch(db, 'select * from t where id = 1')
    db.prepare('update t set x = 1').run()
    expect(await first()).toBe(1)
  })

  it('announces nothing a ROLLBACK TO undid, and the writes after it', async () => {
    const { db } = open(TWO_ROWS)
    const first = await watch(db, 'select * from t where id = 1')
    const second = await watch(db, 'select * from t where id = 2')
    db.exec('begin; savepoint s')
    db.prepare("update t set x = 'c' where id = 1").run()
    db.exec('rollback to s')
    db.prepare("update t set x = 'd' where id = 2").run()
    db.exec('commit')
    expect([await first(), await second()]).toEqual([0, 1])
  })

  it.each([9_999, 10_000, 10_001])(
    'announces nothing a ROLLBACK TO undid after %i updates, and the write before its savepoint',
    async (count) => {
      const { db } = open(TWO_ROWS)
      const first = await watch(db, 'select * from t where id = 1')
      const second = await watch(db, 'select * from t where id = 2')
      db.exec('begin')
      db.prepare("update t set x = 'c' where id = 2").run()
      db.exec('savepoint s')
      const update = db.prepare('update t set x = ? where id = 1')
      for (let i = 0; i < count; i++) update.run(`${i}`)
      db.exec('rollback to s; release s; commit')
      expect(db.prepare('select x from t where id = 1').get()).toEqual({ x: 'a' })
      expect([await first(), await second()]).toEqual([0, 1])
    },
  )

  it('announces the 10,001 updates of a savepoint its transaction kept', async () => {
    const { db } = open(TWO_ROWS)
    const stale = await watch(db, 'select * from t where id = 1')
    db.exec('begin; savepoint s')
    const update = db.prepare('update t set x = ? where id = 1')
    for (let i = 0; i < 10_001; i++) update.run(`${i}`)
    db.exec('release s; commit')
    expect(await stale()).toBe(1)
  })

  it("announces a write after a ROLLBACK TO the parser can't read", async () => {
    const { db } = open(TWO_ROWS)
    const first = await watch(db, 'select * from t where id = 1')
    const second = await watch(db, 'select * from t where id = 2')
    db.exec('begin; savepoint s')
    db.prepare("update t set x = 'c' where id = 1").run()
    // Read as a write of any table, until the next ROLLBACK TO undoes it too.
    db.exec('rollback to s; select 1 from t not indexed')
    db.exec('rollback to s')
    db.prepare("update t set x = 'd' where id = 2").run()
    db.exec('commit')
    expect([await first(), await second()]).toEqual([0, 1])
  })

  it("runs an iterator's statement as its rows are read, and closes it", () => {
    const { db } = open(TWO_ROWS)
    let evaluated = 0
    db.function('observed', (id) => {
      evaluated++
      return id
    })
    const rows = db.prepare('select observed(id) as id from t').iterate()
    expect(evaluated).toBe(0)
    expect(rows.next()).toEqual({ done: false, value: { id: 1 } })
    expect(evaluated).toBe(1)
    expect(rows.return!().done).toBe(true)
    expect(evaluated).toBe(1)
  })

  // The page's last row, a hidden column, tells that row 2 sorts after it.
  it('reads a live query through an iterator row by row, without its hidden columns', async () => {
    const { db } = open(TWO_ROWS)
    const cell = await calls.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(async () => [...db.prepare('select * from t order by id limit 1').iterate()], 'liveDatabaseOfFile()'),
    )
    let stale = 0
    cell.onStale(() => stale++)
    expect(cell.send()).toEqual([{ id: 1, x: 'a' }])
    db.prepare("update t set x = 'c' where id = 2").run()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(stale).toBe(0)
    db.prepare("update t set x = 'd' where id = 1").run()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(stale).toBe(1)
  })

  it.each(['read to its end', 'closed'])('announces a write whose iterator was %s', async (end) => {
    const { db } = open(TWO_ROWS)
    const first = await watch(db, 'select * from t where id = 1')
    const second = await watch(db, 'select * from t where id = 2')
    const rows = db.prepare("update t set x = 'c' where id = 1 returning id").iterate()
    expect(rows.next()).toEqual({ done: false, value: { id: 1 } })
    if (end === 'closed') rows.return!()
    else expect(rows.next().done).toBe(true)
    expect([await first(), await second()]).toEqual([1, 0])
  })

  it.each([
    ["begin; update t set x = 'c' where id = 2; savepoint s", 'rollback to s; release s; commit'],
    ["update t set x = 'c' where id = 2; begin", 'rollback'],
  ])('announces a write in a text with transaction statements: %s, then %s', async (text, end) => {
    const { db } = open(TWO_ROWS)
    const stale = await watch(db, 'select * from t where id = 2')
    db.exec(text)
    db.exec(end)
    expect(db.prepare('select x from t where id = 2').get()).toEqual({ x: 'c' })
    expect(await stale()).toBe(1)
  })

  it('runs a text with transaction statements up to its first failing statement, as node:sqlite does', () => {
    const text =
      "begin; update t set x = 'c' where id = 1; insert into t values (2, 'z'); update t set x = 'd' where id = 2"
    const [captured, native] = [open(TWO_ROWS), open(TWO_ROWS, 'later')].map(({ db }) => {
      let error: unknown
      try {
        db.exec(text)
      } catch (err) {
        error = (err as Error).message
      }
      return { error, transaction: db.isTransaction, rows: db.prepare('select * from t order by id').all() }
    })
    expect(captured).toEqual(native)
    expect(captured).toEqual({
      error: 'UNIQUE constraint failed: t.id',
      transaction: true,
      rows: [
        { id: 1, x: 'c' },
        { id: 2, x: 'b' },
      ],
    })
  })

  it('announces a write to a grouped read with HAVING only when its group may cross the bound', async () => {
    const { db } = open(
      'create table t (id integer primary key, g integer); insert into t (g) values (1), (1), (2), (2), (2), (2), (2)',
    )
    const stale = await watch(db, 'select g, count(*) as n from t group by g having count(*) > 3')
    db.prepare('insert into t (g) values (1)').run()
    expect(await stale()).toBe(0)
    db.prepare('insert into t (g) values (1)').run()
    expect(await stale()).toBe(1)
  })

  it("loads the catalog again after CREATE TABLE and ALTER TABLE, and asks nothing SQLite can't run", async () => {
    const { db } = open(TWO_ROWS)
    refused.length = 0
    db.exec('create table u (id integer primary key)')
    db.exec('alter table u add column x text')
    db.prepare("insert into u values (1, 'a')").run()
    const stale = await watch(db, 'select * from u where id = 1')
    db.prepare("update u set x = 'b' where id = 1").run()
    expect(await stale()).toBe(1)
    expect(refused).toEqual([])
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
