import { AsyncLocalStorage } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import { sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/pglite'
import { REQUEST_CONTEXT } from 'telefunc'
import { describe, expect, it } from 'vitest'
import { liveDrizzle } from '@telefunc/live/drizzle'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'

describe('PGlite capture', () => {
  // The tenant sees only row 1, so a write captured with a widened `RETURNING` would update row 1 only.
  it('plans a write after the role change queued before it', async () => {
    const pglite = new PGlite()
    await pglite.exec(`create table items (id int primary key, value text);
      insert into items values (1, 'before'), (2, 'before');
      create role tenant;
      alter table items enable row level security;
      create policy see on items for select using (id = 1);
      create policy change on items for update using (true) with check (true);
      grant select, update on items to tenant;`)
    const db = liveDrizzle(drizzle(pglite))
    const updated = await db.transaction(async (tx) => {
      const [, update] = await Promise.all([
        tx.execute(sql`set local role tenant`),
        tx.execute(sql`update items set value = 'after'`),
      ])
      return update.affectedRows
    })
    expect(updated).toBe(2)
    expect((await pglite.query('select id, value from items order by id')).rows).toEqual([
      { id: 1, value: 'after' },
      { id: 2, value: 'after' },
    ])
    await pglite.close()
  })

  it('announces nothing a ROLLBACK TO undid, past 10,000 changes', async () => {
    const pglite = new PGlite()
    await pglite.exec(
      'create table t (id int primary key, x int); insert into t select i, 0 from generate_series(1, 10002) i',
    )
    liveDatabaseOf(pglite, {})
    const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
    const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(() => pglite.query('select * from t where id = 1'), 'the spec'),
    )
    let stale = 0
    cell.onStale(() => stale++)
    cell.send()
    // Two statements: one changing more than 10,000 rows changes its table whole on its own.
    const updates = ['update t set x = 1 where id between 2 and 5002', 'update t set x = 1 where id > 5002']
    for (const text of ['begin', 'savepoint s', ...updates, 'rollback to s', 'commit']) await pglite.query(text)
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(stale).toBe(0)
    await pglite.close()
  })
})

describe('two in-memory PGlite databases', () => {
  it("announce a write to one to the other's reads", async () => {
    const [one, other] = [new PGlite(), new PGlite()]
    for (const pglite of [one, other]) {
      await pglite.exec("create table t (id int primary key, x text); insert into t values (1, 'a')")
      liveDatabaseOf(pglite, {})
    }
    const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
    const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(() => one.query('select * from t where id = 1'), 'the spec'),
    )
    let stale = 0
    cell.onStale(() => stale++)
    cell.send()
    const settled = () => new Promise((resolve) => setTimeout(resolve, 50))
    await other.query("update t set x = 'b' where id = 1")
    await settled()
    expect(stale).toBe(0)
    await one.query("update t set x = 'b' where id = 1")
    await settled()
    expect(stale).toBe(1)
    await Promise.all([one.close(), other.close()])
  })
})

describe('identical live reads on one PGlite', () => {
  const READ = 'select id, done from todos order by id'

  /** A wrapped PGlite counting the reads of `todos` that reach it; `hold()` delays the results of those after it. */
  async function counting() {
    const pglite = new PGlite()
    await pglite.exec('create table todos (id int primary key, done bool); insert into todos values (1, false)')
    let reads = 0
    let held: Promise<void> | undefined
    const query = pglite.query.bind(pglite)
    pglite.query = (async (...args: Parameters<typeof query>) => {
      const reading = /from todos/.test(args[0])
      if (reading) reads++
      const until = reading ? held : undefined
      const result = await query(...args)
      await until
      return result
    }) as typeof query
    liveDatabaseOf(pglite, {})
    // A telefunction call, as telefunc's server runs one: the response sends the `Live`.
    const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
    const live = () =>
      store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => readLive(() => pglite.query(READ), 'the spec'))
    return { pglite, live, reads: () => reads, hold: (until: Promise<void> | undefined) => (held = until) }
  }

  it('share one read', async () => {
    const { pglite, live, reads } = await counting()
    const lives = await Promise.all(Array.from({ length: 10 }, live))
    expect(reads()).toBe(1)
    for (const each of lives) expect(each.send().rows).toEqual([{ id: 1, done: false }])
    await pglite.close()
  })

  it("don't share across a write between them", async () => {
    const { pglite, live, reads, hold } = await counting()
    let release!: () => void
    hold(new Promise((resolve) => (release = resolve)))
    const first = live()
    while (reads() === 0) await new Promise((resolve) => setTimeout(resolve, 1))
    hold(undefined)
    await pglite.query('update todos set done = true where id = 1')
    const next = live()
    // Joined, it would wait for the first one's result.
    setTimeout(release, 50)
    expect((await next).send().rows).toEqual([{ id: 1, done: true }])
    expect(reads()).toBe(2)
    expect((await first).send().rows).toEqual([{ id: 1, done: false }])
    await pglite.close()
  })
})

describe('a delete cascading two levels on PGlite', () => {
  const SCHEMA = `create table users (id int primary key);
    create table todos (id int primary key, user_id int references users on delete cascade);
    create table tags (id int primary key, todo_id int references todos on delete cascade);
    insert into users values (1), (2);
    insert into todos values (10, 1), (11, 1), (20, 2);
    insert into tags values (100, 10), (101, 11), (200, 20);`

  /** A live PGlite with `SCHEMA`, the texts sent to it, and how many stale signals each of `reads` gets. */
  async function cascading(reads: string[]) {
    const pglite = new PGlite()
    await pglite.exec(SCHEMA)
    const sent: string[] = []
    const query = pglite.query.bind(pglite)
    pglite.query = ((text: string, ...rest: []) => {
      sent.push(text)
      return query(text, ...rest)
    }) as typeof query
    liveDatabaseOf(pglite, {})
    const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
    const counts = await Promise.all(
      reads.map(async (read) => {
        const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
          readLive(() => pglite.query(read), 'the spec'),
        )
        let stale = 0
        cell.onStale(() => stale++)
        cell.send()
        return () => stale
      }),
    )
    return { pglite, sent, stale: () => counts.map((count) => count()) }
  }

  it('stales only the reads of the rows it deletes', async () => {
    const { pglite, stale } = await cascading([
      'select * from tags where id = 100',
      'select * from tags where id = 200',
    ])
    await pglite.query('delete from users where id = $1', [1])
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(stale()).toEqual([1, 0])
    await pglite.close()
  })

  it('is sent as without cascades while no live read reads their tables', async () => {
    const { pglite, sent } = await cascading(['select * from users where id = 2'])
    await pglite.query('delete from users where id = $1', [1])
    expect(sent.filter((text) => text.startsWith('delete'))).toEqual([
      'delete from users where id = $1 RETURNING to_json(ARRAY["users"."id"::text])::text AS "__live_0"',
    ])
    await pglite.close()
  })
})
