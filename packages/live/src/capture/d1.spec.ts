// D1 through a stand-in for its binding: what a write tells the live reads of its database, and of another one with
// the same tables.

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
function d1Database(schema = "create table t (id integer primary key, x text); insert into t values (1, 'a')") {
  const db = new DatabaseSync(':memory:')
  db.exec(schema)
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

/** How many stale signals `read`, run in `.live()`, gets from now on. */
async function staleSignals(read: () => Promise<unknown>): Promise<() => number> {
  const cell = await calls.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => readLive(read, 'the spec'))
  let stale = 0
  cell.onStale(() => stale++)
  cell.send()
  return () => stale
}

const settled = () => new Promise((resolve) => setTimeout(resolve, 50))

describe('two D1 databases with the same tables', () => {
  it.each([
    { label: 'without names', one: undefined, other: undefined, signals: 1 },
    { label: 'named alike', one: 'tenant', other: 'tenant', signals: 1 },
    { label: 'named apart', one: 'tenant-a', other: 'tenant-b', signals: 0 },
  ])("$label: a write to one gives the other's read $signals stale signal(s)", async ({ one, other, signals }) => {
    const [a, b] = [d1Database(), d1Database()]
    liveDatabaseOf(a, { name: one })
    liveDatabaseOf(b, { name: other })
    const stale = await staleSignals(
      async () => (await b.prepare('select * from t where id = ?').bind(1).all()).results,
    )
    await a.prepare("update t set x = 'b' where id = ?").bind(1).run()
    await settled()
    expect(stale()).toBe(signals)
  })
})

describe('a write the database refused', () => {
  const refuse = "create trigger refuse before insert on t when new.x = 'z' begin select raise(fail, 'refused'); end;"
  it.each([
    {
      label: 'under the default ABORT',
      clause: '',
      trigger: '',
      write: "insert into t values (2, 'b'), (3, 'a')",
      kept: false,
    },
    {
      label: 'under OR FAIL',
      clause: '',
      trigger: '',
      write: "insert or fail into t values (2, 'b'), (3, 'a')",
      kept: true,
    },
    {
      label: 'under ON CONFLICT FAIL',
      clause: ' on conflict fail',
      trigger: '',
      write: "insert into t values (2, 'b'), (3, 'a')",
      kept: true,
    },
    {
      label: 'by RAISE(FAIL)',
      clause: '',
      trigger: refuse,
      write: "insert into t values (2, 'b'), (3, 'z')",
      kept: true,
    },
    {
      label: 'after a statement in its text',
      clause: '',
      trigger: '',
      write: "insert into t values (2, 'b'); insert into t values (3, 'a')",
      kept: true,
    },
  ])('$label: the reads go stale when it kept a row ($kept)', async ({ label, clause, trigger, write, kept }) => {
    const d1 = d1Database(
      `create table t (id integer primary key, x text unique${clause}); insert into t values (1, 'a'); ${trigger}`,
    )
    liveDatabaseOf(d1, { name: label })
    const read = async () => (await d1.prepare('select id from t order by id').all()).results
    const stale = await staleSignals(read)
    await expect(write.includes(';') ? d1.exec(write) : d1.prepare(write).run()).rejects.toThrow()
    await settled()
    expect(await read()).toEqual(kept ? [{ id: 1 }, { id: 2 }] : [{ id: 1 }])
    expect(stale()).toBe(kept ? 1 : 0)
  })
})

describe('a write that changed no row', () => {
  const schema = `
    create table users (id integer primary key);
    create table todos (id integer primary key, user_id int not null references users (id) on delete cascade);
    create table tags (id integer primary key, todo_id int not null references todos (id) on delete cascade);
    create table deleted_todos (id integer primary key, todo_id int not null);
    create trigger todos_deleted after delete on todos begin insert into deleted_todos (todo_id) values (old.id); end;
    insert into users values (1); insert into todos values (1, 1); insert into tags values (1, 1);`

  it.each([
    { label: "a user who doesn't exist", user: 9, signals: [0, 0, 0] },
    { label: 'a user with a tagged todo', user: 1, signals: [1, 1, 1] },
  ])(
    'deleting $label stales the reads of what its cascades and triggers write: $signals',
    async ({ label, user, signals }) => {
      const d1 = d1Database(schema)
      liveDatabaseOf(d1, { name: label })
      const reads = ['select id from todos', 'select id from tags', 'select todo_id from deleted_todos']
      const stale = await Promise.all(
        reads.map((text) => staleSignals(async () => (await d1.prepare(text).all()).results)),
      )
      await d1.prepare('delete from users where id = ?').bind(user).run()
      await settled()
      expect(stale.map((count) => count())).toEqual(signals)
    },
  )
})
