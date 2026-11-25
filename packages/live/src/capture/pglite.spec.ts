import { AsyncLocalStorage } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import { sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/pglite'
import { REQUEST_CONTEXT } from 'telefunc'
import { describe, expect, it } from 'vitest'
import { liveDrizzle } from '@telefunc/live/drizzle'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'
import { emptyPGlite } from '../test/pglite.js'

const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

/** A live read of `text`, as a telefunction runs one: its rows, and how many stale signals it got since. */
async function watched(pglite: PGlite, text: string) {
  const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
    readLive(() => pglite.query(text), 'the spec'),
  )
  let stale = 0
  cell.onStale(() => stale++)
  return { rows: cell.send().rows as unknown[], stale: () => stale }
}

/** Runs `text`, then waits for the stale signals it sends. A read goes stale once: count its signals after each write
 *  that may make it stale. */
async function written(pglite: PGlite, text: string): Promise<void> {
  await pglite.query(text)
  await new Promise((resolve) => setTimeout(resolve, 50))
}

describe('PGlite capture', () => {
  // The tenant sees only row 1, so a write captured with a widened `RETURNING` would update row 1 only.
  it('plans a write after the role change queued before it', async () => {
    const pglite = await emptyPGlite()
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
    const pglite = await emptyPGlite()
    await pglite.exec(
      'create table t (id int primary key, x int); insert into t select i, 0 from generate_series(1, 10002) i',
    )
    liveDatabaseOf(pglite, {})
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
    const [one, other] = await Promise.all([emptyPGlite(), emptyPGlite()])
    for (const pglite of [one, other]) {
      await pglite.exec("create table t (id int primary key, x text); insert into t values (1, 'a')")
      liveDatabaseOf(pglite, {})
    }
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
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table todos (id int primary key, done bool);
      insert into todos values (1, false);
      create view every_todo as select * from todos;`)
    let reads = 0
    let held: Promise<void> | undefined
    const query = pglite.query.bind(pglite)
    pglite.query = (async (...args: Parameters<typeof query>) => {
      const reading = /from (todos|every_todo)/.test(args[0])
      if (reading) reads++
      const until = reading ? held : undefined
      const result = await query(...args)
      await until
      return result
    }) as typeof query
    liveDatabaseOf(pglite, {})
    // A telefunction call, as telefunc's server runs one: the response sends the `Live`.
    const live = (text = READ) =>
      store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => readLive(() => pglite.query(text), 'the spec'))
    return { pglite, live, reads: () => reads, hold: (until: Promise<void> | undefined) => (held = until) }
  }

  it.each([
    ['a table', READ],
    ['a view', 'select id, done from every_todo order by id'],
  ])('share one read of %s', async (_, text) => {
    const { pglite, live, reads } = await counting()
    const lives = await Promise.all(Array.from({ length: 10 }, () => live(text)))
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
    const pglite = await emptyPGlite()
    await pglite.exec(SCHEMA)
    const sent: string[] = []
    const query = pglite.query.bind(pglite)
    pglite.query = ((text: string, ...rest: []) => {
      sent.push(text)
      return query(text, ...rest)
    }) as typeof query
    liveDatabaseOf(pglite, {})
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

describe('a live read calling a SQL function of the app', () => {
  it.each([
    ['its AS text', (table: string) => `as 'select count(*)::int from ${table}'`],
    ['its SQL-standard body', (table: string) => `return (select count(*)::int from ${table})`],
  ])('watches the tables %s reads, as defined now', async (_, body) => {
    const define = (table: string) => `create or replace function n() returns int language sql stable ${body(table)}`
    const pglite = await emptyPGlite()
    await pglite.exec(`create table a (id int); create table b (id int); create table c (id int); ${define('a')}`)
    liveDatabaseOf(pglite, {})
    const before = await watched(pglite, 'select n()')
    await written(pglite, 'insert into c values (1)')
    expect(before.stale()).toBe(0)
    await written(pglite, 'insert into a values (1)')
    expect(before.stale()).toBe(1)
    await written(pglite, define('b'))
    const after = await watched(pglite, 'select n()')
    await written(pglite, 'insert into a values (2)')
    expect(after.stale()).toBe(0)
    await written(pglite, 'insert into b values (1)')
    expect(after.stale()).toBe(1)
    await pglite.close()
  })
})

describe("a live read of a view or a table whose policy calls the app's SQL function", () => {
  it('watches the tables a view calls it to read', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table source (id int);
      create table c (id int);
      insert into source values (1);
      create function n() returns int language sql stable as 'select count(*)::int from source';
      create view v as select n() as n;`)
    liveDatabaseOf(pglite, {})
    const read = await watched(pglite, 'select * from v')
    expect(read.rows).toEqual([{ n: 1 }])
    await written(pglite, 'insert into c values (1)')
    expect(read.stale()).toBe(0)
    await written(pglite, 'insert into source values (2)')
    expect(read.stale()).toBe(1)
    expect((await pglite.query('select * from v')).rows).toEqual([{ n: 2 }])
    await pglite.close()
  })

  it('watches the tables a row-level security policy calls a security definer function to read', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table items (id int);
      create table flags (ok bool);
      create table c (id int);
      insert into items values (1);
      insert into flags values (true);
      create function allowed() returns bool language sql stable security definer as 'select ok from flags';
      alter table items enable row level security;
      create policy tenant_reads on items for select using (allowed());
      create role tenant;
      grant select on items to tenant;`)
    liveDatabaseOf(pglite, {})
    await pglite.query('set role tenant')
    const read = await watched(pglite, 'select * from items')
    expect(read.rows).toEqual([{ id: 1 }])
    await pglite.query('reset role')
    await written(pglite, 'insert into c values (1)')
    expect(read.stale()).toBe(0)
    await written(pglite, 'update flags set ok = false')
    expect(read.stale()).toBe(1)
    await pglite.query('set role tenant')
    expect((await pglite.query('select * from items')).rows).toEqual([])
    await pglite.close()
  })
})

describe('identical live reads reaching a volatile function', () => {
  /** A PGlite holding `schema`, connected as `role` when given: the app's role from the start, as a `SET ROLE` would
   *  keep identical reads from sharing anyway. */
  async function database(schema: string, role: string | undefined): Promise<PGlite> {
    const owner = await emptyPGlite()
    await owner.exec(schema)
    if (!role) return owner
    const loadDataDir = await owner.dumpDataDir('none')
    await owner.close()
    const pglite = new PGlite({ loadDataDir, username: role })
    await pglite.waitReady
    return pglite
  }

  it.each([
    ['through a view', "create view tick as select nextval('seq')::int as n", 'select * from tick', undefined],
    [
      "through an aggregate's transition function",
      `create function step(int, int) returns int language sql volatile as 'select $1 + nextval(''seq'')::int';
       create aggregate total(int) (sfunc = step, stype = int, initcond = '0');
       create table one (id int);
       insert into one values (1);`,
      'select total(id) from one',
      undefined,
    ],
    [
      'through a row-level security policy',
      `create table items (id int);
       insert into items values (1);
       alter table items enable row level security;
       create policy counted on items for select using (nextval('seq') > 0);
       create role tenant;
       grant select on items to tenant;
       grant usage, select on sequence seq to tenant;`,
      'select * from items',
      // Postgres applies a policy to a role that isn't the table's owner.
      'tenant',
    ],
  ])('run each, %s', async (_, schema, text, role) => {
    const pglite = await database(`create sequence seq; ${schema}`, role)
    liveDatabaseOf(pglite, {})
    await Promise.all([watched(pglite, text), watched(pglite, text)])
    expect((await pglite.query('select last_value::int as n from seq')).rows).toEqual([{ n: 2 }])
    await pglite.close()
  })
})

describe('a read calling a SQL function of the app that writes', () => {
  it.each([
    ['in the statement', 'select bump()'],
    ['through a view', 'select * from bumped'],
    ['through an aggregate', 'select total(id) from one'],
  ])('announces what the function writes, called %s', async (_, text) => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table counters (id int primary key, n int);
      insert into counters values (1, 0);
      create function bump() returns int language sql volatile as
        'update counters set n = n + 1 where id = 1 returning n';
      create view bumped as select bump() as n;
      create function step(int, int) returns int language sql volatile as
        'update counters set n = n + 1 where id = 1 returning $1 + $2';
      create aggregate total(int) (sfunc = step, stype = int, initcond = '0');
      create table one (id int);
      insert into one values (1);`)
    liveDatabaseOf(pglite, {})
    const read = await watched(pglite, 'select * from counters')
    await written(pglite, text)
    expect((await pglite.query('select n from counters')).rows).toEqual([{ n: 1 }])
    expect(read.stale()).toBe(1)
    await pglite.close()
  })
})

describe('a read calling a STABLE function of the app', () => {
  it('announces what the functions it calls write, and only that', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table settled (id int primary key, n int);
      insert into settled values (1, 0);
      create table c (id int);
      create function settle() returns void language sql as $f$ update settled set n = n + 1 $f$;
      create function settle_stably() returns void language sql stable as $f$ select settle() $f$;`)
    liveDatabaseOf(pglite, {})
    const reads = await Promise.all([watched(pglite, 'select n from settled'), watched(pglite, 'select * from c')])
    await written(pglite, 'select settle_stably()')
    expect((await pglite.query('select n from settled')).rows).toEqual([{ n: 1 }])
    expect(reads.map((read) => read.stale())).toEqual([1, 0])
    await pglite.close()
  })

  it('stays a read when the functions it calls write nothing', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create function shout(t text) returns text language sql stable as $f$ select upper(t) $f$;
      create function greet(t text) returns text language sql stable as $f$ select shout(t) || '!' $f$;`)
    liveDatabaseOf(pglite, {})
    const read = await watched(pglite, "select greet('hi') as g")
    expect(read.rows).toEqual([{ g: 'HI!' }])
    await pglite.close()
  })

  it('announces what functions calling each other write', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table counters (id int primary key, n int);
      insert into counters values (1, 0);
      create table c (id int);
      create function ping(depth int) returns int language plpgsql stable as $f$
        begin if depth > 0 then perform pong(depth - 1); end if; return depth; end $f$;
      create function pong(depth int) returns int language plpgsql as $f$
        begin
          update counters set n = n + 1 where id = 1;
          if depth > 0 then perform ping(depth - 1); end if;
          return depth;
        end $f$;`)
    liveDatabaseOf(pglite, {})
    const reads = await Promise.all([watched(pglite, 'select n from counters'), watched(pglite, 'select * from c')])
    await written(pglite, 'select ping(3)')
    expect((await pglite.query('select n from counters')).rows).toEqual([{ n: 2 }])
    expect(reads.map((read) => read.stale())).toEqual([1, 0])
    await pglite.close()
  })

  // A view calling a writer reaches its tables' readers; a read of helpers reading no table reaches no view.
  it.each([
    ['SQL', `create function shout(t text) returns text language sql stable as $f$ select upper(t) $f$`],
    [
      'PL/pgSQL',
      `create function shout(t text) returns text language plpgsql stable as $f$ begin return upper(t); end $f$`,
    ],
  ])('stays a read through %s helpers while a view of the app writes', async (_, shout) => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table a (n int);
      insert into a values (0);
      create function writer() returns int language sql as $f$ update a set n = n + 1 returning n $f$;
      create view written as select writer() as n;
      ${shout};
      create function greet(t text) returns text language sql stable as $f$ select shout(t) || '!' $f$;`)
    liveDatabaseOf(pglite, {})
    expect((await watched(pglite, "select greet('hi') as g")).rows).toEqual([{ g: 'HI!' }])
    await pglite.close()
  })
})

describe('a read calling a function of the app that writes a table with side effects', () => {
  const bodies = [
    ['a BEGIN ATOMIC body', 'begin atomic update a set n = n + 1 where id = 1; select 1; end'],
    ['an AS text', 'as $f$ update a set n = n + 1 where id = 1; select 1 $f$'],
  ]
  const calls = [
    ['in the statement', 'select bump()'],
    ['through a STABLE function', 'select bump_stably()'],
    ['through a view', 'select * from bumped'],
  ]
  it.each(bodies.flatMap(([bodyName, body]) => calls.map(([call, text]) => [bodyName, call, body, text])))(
    "announces what the table's trigger writes, with %s, called %s",
    async (_, __, body, text) => {
      const pglite = await emptyPGlite()
      await pglite.exec(`
        create table a (id int primary key, n int);
        insert into a values (1, 0);
        create table b (id int primary key, n int);
        insert into b values (1, 0);
        create table c (id int);
        create function log_a() returns trigger language plpgsql as $f$
          begin update b set n = n + 1; return new; end $f$;
        create trigger a_logged after update on a for each row execute function log_a();
        create function bump() returns int language sql ${body};
        create function bump_stably() returns int language sql stable as $f$ select bump() $f$;
        create view bumped as select bump();`)
      liveDatabaseOf(pglite, {})
      const reads = await Promise.all(['a', 'b', 'c'].map((table) => watched(pglite, `select * from ${table}`)))
      await written(pglite, text)
      expect((await pglite.query('select n from b')).rows).toEqual([{ n: 1 }])
      expect(reads.map((read) => read.stale())).toEqual([1, 1, 0])
      await pglite.close()
    },
  )

  it('announces the rows its delete cascades to', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table a (id int primary key);
      insert into a values (1);
      create table b (id int references a (id) on delete cascade);
      insert into b values (1);
      create table c (id int);
      create function empty_a() returns void language sql begin atomic delete from a; end;`)
    liveDatabaseOf(pglite, {})
    const reads = await Promise.all(['a', 'b', 'c'].map((table) => watched(pglite, `select * from ${table}`)))
    await written(pglite, 'select empty_a()')
    expect((await pglite.query('select * from b')).rows).toEqual([])
    expect(reads.map((read) => read.stale())).toEqual([1, 1, 0])
    await pglite.close()
  })
})

describe('a SQL function of the app with a SQL-standard body', () => {
  // Its tables named, a call runs as sent: in no transaction of ours that reads its locks to learn them.
  it('announces what its BEGIN ATOMIC body writes, as the catalog names it', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table a (id int primary key, n int);
      insert into a values (1, 0);
      create table c (id int);
      create function bump() returns void language sql begin atomic update a set n = n + 1 where id = 1; end;`)
    let transactions = 0
    const transaction = pglite.transaction.bind(pglite)
    pglite.transaction = ((...args: Parameters<typeof transaction>) => {
      transactions++
      return transaction(...args)
    }) as typeof transaction
    liveDatabaseOf(pglite, {})
    const reads = await Promise.all([watched(pglite, 'select n from a'), watched(pglite, 'select * from c')])
    await written(pglite, 'select bump()')
    expect(reads.map((read) => read.stale())).toEqual([1, 0])
    expect(transactions).toBe(0)
    await pglite.close()
  })

  it('stays a read when its RETURN body writes nothing', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table a (id int);
      insert into a values (1);
      create function n() returns int language sql stable return (select count(*)::int from a);`)
    liveDatabaseOf(pglite, {})
    expect((await watched(pglite, 'select n()')).rows).toEqual([{ n: 1 }])
    await pglite.close()
  })
})

describe('a live read reaching a PL/pgSQL function of the app that only reads', () => {
  it.each([
    ['called in the statement', 'select read_n() as n'],
    ['through a view', 'select * from readable'],
  ])('watches what it reads, %s', async (_, text) => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table counters (id int primary key, n int);
      insert into counters values (1, 0);
      create function read_n() returns int language plpgsql as
        'begin return (select n from counters where id = 1); end';
      create view readable as select read_n() as n;`)
    liveDatabaseOf(pglite, {})
    const read = await watched(pglite, text)
    expect(read.rows).toEqual([{ n: 0 }])
    await written(pglite, 'update counters set n = 5')
    expect(read.stale()).toBe(1)
    await pglite.close()
  })

  it('watches what it reads, through a row-level security policy calling a security definer one', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table members (uid int);
      insert into members values (1);
      create table items (id int, owner int);
      insert into items values (7, 1);
      create function is_member(o int) returns bool language plpgsql security definer as
        'begin return exists (select 1 from members where uid = o); end';
      alter table items enable row level security;
      create policy members_only on items for select using (is_member(owner));
      create role tenant;
      grant select on items to tenant;`)
    liveDatabaseOf(pglite, {})
    await pglite.query('set role tenant')
    const read = await watched(pglite, 'select id from items')
    await pglite.query('reset role')
    expect(read.rows).toEqual([{ id: 7 }])
    await written(pglite, 'delete from members')
    expect(read.stale()).toBe(1)
    await pglite.close()
  })
  // The cursor's query reads `a`, which the function's body doesn't name.
  it('watches every table when it fetches from a cursor passed to it', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table a (n int);
      insert into a values (10);
      create function read_cursor(c refcursor) returns int language plpgsql stable as $f$
        declare n int;
        begin fetch next from c into n; return n; end $f$;`)
    liveDatabaseOf(pglite, {})
    await pglite.exec('declare data cursor with hold for select n from a')
    const read = await watched(pglite, "select read_cursor('data') as n")
    await pglite.exec('close data')
    expect(read.rows).toEqual([{ n: 10 }])
    await written(pglite, 'update a set n = 20')
    expect(read.stale()).toBe(1)
    await pglite.close()
  })
})

describe('a live read reaching a PL/pgSQL function of the app returning rows or columns', () => {
  it.each([
    [
      'rows through RETURN QUERY',
      'returns setof items',
      'begin return query select * from items where active; end',
      [{ id: 1, active: true }],
    ],
    [
      'rows through RETURN NEXT',
      'returns setof items',
      'declare r items; begin for r in select * from items where active loop return next r; end loop; end',
      [{ id: 1, active: true }],
    ],
    [
      'a column of its RETURNS TABLE',
      'returns table (item int)',
      'begin item := (select max(id) from items where active); return next; end',
      [{ item: 1 }],
    ],
    ['an OUT parameter', '', 'begin item := (select max(id) from items where active); end', [{ item: 1 }]],
  ])(
    'watches what it reads, returning %s, through a view and called in the statement',
    async (_, result, body, rows) => {
      const pglite = await emptyPGlite()
      const parameters = result ? '()' : '(out item int)'
      await pglite.exec(`
      create table items (id int primary key, active bool);
      insert into items values (1, true), (2, false);
      create function active_items${parameters} ${result} language plpgsql as $f$ ${body} $f$;
      create view active as select * from active_items();`)
      liveDatabaseOf(pglite, {})
      const reads = await Promise.all([
        watched(pglite, 'select * from active'),
        watched(pglite, 'select * from active_items()'),
      ])
      expect(reads.map((read) => read.rows)).toEqual([rows, rows])
      await written(pglite, 'update items set active = true where id = 2')
      expect(reads.map((read) => read.stale())).toEqual([1, 1])
      await pglite.close()
    },
  )
})

describe('a live read calling an aggregate of the app', () => {
  it.each([
    ["Postgres's own", 'numeric_mul'],
    ['a SQL function reading no table', 'times'],
  ])('watches the table it aggregates, through %s as its transition function', async (_, transition) => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table a (n numeric);
      create table c (id int);
      create function times(numeric, numeric) returns numeric language sql immutable as 'select $1 * $2';
      create aggregate product(numeric) (sfunc = ${transition}, stype = numeric, initcond = '1');`)
    liveDatabaseOf(pglite, {})
    const read = await watched(pglite, 'select product(n) from a')
    await written(pglite, 'insert into c values (1)')
    expect(read.stale()).toBe(0)
    await written(pglite, 'insert into a values (2)')
    expect(read.stale()).toBe(1)
    await pglite.close()
  })
})

describe('a transaction writing through a rule, a default and a trigger of the app', () => {
  it('stales the reads of what they and a trigger calling one write, not every read', async () => {
    const pglite = await emptyPGlite()
    await pglite.exec(`
      create table t (id int primary key, x text);
      insert into t values (1, 'a'), (2, 'a');
      create table audit (id serial primary key, what text);
      create table counters (id int primary key, n int);
      insert into counters values (1, 0);
      create function next_n() returns int language sql as 'update counters set n = n + 1 where id = 1 returning n';
      create table events (id serial primary key, kind text, seq int default next_n());
      create rule audited as on insert to events do also insert into audit (what) values ('event ' || new.kind);
      create table settled (n int);
      insert into settled values (0);
      create function settle() returns void language sql as 'update settled set n = n + 1';
      create function settled_event() returns trigger language plpgsql as 'begin perform settle(); return null; end';
      create trigger settles after insert on events for each row execute function settled_event();`)
    liveDatabaseOf(pglite, {})
    const live = async (text: string) => {
      const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
        readLive(() => pglite.query(text), 'the spec'),
      )
      let stale = 0
      cell.onStale(() => stale++)
      cell.send()
      return () => stale
    }
    const reads = await Promise.all([
      live('select x from t where id = 1'),
      live('select x from t where id = 2'),
      // The app inserts into audit too: its row doesn't stand for the rule's.
      live(`select what from audit where what like 'event%'`),
      live('select n from counters'),
      live('select n from settled'),
    ])
    await pglite.transaction(async (tx) => {
      await tx.query(`insert into events (kind) values ('signup')`)
      await tx.query(`update t set x = 'b' where id = 1`)
      await tx.query(`insert into audit (what) values ('app')`)
    })
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(reads.map((stale) => stale())).toEqual([1, 0, 1, 1, 1])
    await pglite.close()
  })
})
