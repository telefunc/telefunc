// A text of several statements on each Postgres driver: Postgres runs it in one transaction, so it runs a statement at
// a time in one, each captured as on its own, and gives the driver's own result.

import { AsyncLocalStorage } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import pg from 'pg'
import postgres from 'postgres'
import { REQUEST_CONTEXT } from 'telefunc'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'
import { emptyPGlite } from '../test/pglite.js'

type Driver = {
  /** Runs a text of several statements: the driver's result. */
  text(text: string): Promise<unknown>
  /** The rows of `t` with `id`. */
  read(id: number): Promise<unknown[]>
  /** The database itself, past the driver. */
  pglite: PGlite
  close(): Promise<void>
}

/** A PGlite holding `t`, through `driver`; `live`: wrapped by `liveDatabaseOf()`. */
async function database(driver: string, live: boolean): Promise<Driver> {
  const pglite = await emptyPGlite()
  await pglite.exec('create table t (id int primary key, x text)')
  const wrapped = <T extends object>(client: T) => (live && liveDatabaseOf(client as never, {}), client)
  if (driver === 'PGlite') {
    const db = wrapped(pglite)
    const read = async (id: number) => (await db.query('select * from t where id = $1', [id])).rows
    return { text: (text) => db.exec(text), read, pglite, close: () => pglite.close() }
  }
  const server = new PGLiteSocketServer({ db: pglite, port: 0 })
  await server.start()
  const url = `postgres://postgres@${server.getServerConn()}/postgres`
  const close = async (client: { end(): Promise<unknown> }) => {
    await client.end()
    await server.stop()
    await pglite.close()
  }
  if (driver === 'pg') {
    const client = wrapped(new pg.Client(url))
    await client.connect()
    const read = async (id: number) => (await client.query('select * from t where id = $1', [id])).rows
    return { text: (text) => client.query(text), read, pglite, close: () => close(client) }
  }
  // One connection: the server runs every connection on PGlite's one session, which a pool's would share.
  const sql = wrapped(postgres(url, { max: 1, onnotice: () => {} }))
  const read = async (id: number) => [...(await sql`select * from t where id = ${id}`)]
  return { text: (text) => sql.unsafe(text), read, pglite, close: () => close(sql) }
}

/** What a driver's result for a text holds: each statement's command, count and rows, as it groups them. */
function shapeOf(result: unknown): unknown {
  if (Array.isArray(result) && !('command' in result)) return result.map(shapeOf)
  const r = result as { command?: string; rowCount?: number; affectedRows?: number; count?: number; rows?: unknown[] }
  return {
    command: r.command,
    count: r.rowCount ?? r.affectedRows ?? r.count,
    rows: r.rows ?? [...(result as unknown[])],
  }
}

const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

/** How many stale signals a live read of `t`'s row `id` gets from now on. */
async function signals(db: Driver, id: number): Promise<() => number> {
  const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => readLive(() => db.read(id), 'the spec'))
  let stale = 0
  cell.onStale(() => stale++)
  cell.send()
  return () => stale
}

describe.each(['PGlite', 'pg', 'postgres.js'])('a text of several statements on %s', (driver) => {
  let captured: Driver
  let native: Driver
  beforeAll(async () => {
    ;[captured, native] = await Promise.all([database(driver, true), database(driver, false)])
  })
  afterAll(() => Promise.all([captured.close(), native.close()]))
  beforeEach(async () => {
    const rows = "delete from t; insert into t values (1, 'a'), (2, 'a')"
    await Promise.all([captured.pglite.exec(rows), native.pglite.exec(rows)])
  })

  it.each([
    "update t set x = 'b' where id = 1; select 1",
    "set application_name to 'live'; update t set x = 'b' where id = 1",
  ])('stales only the reads of the rows it writes, and gives the result the driver gives: %s', async (text) => {
    const stale = await Promise.all([1, 2].map((id) => signals(captured, id)))
    const [result, expected] = [await captured.text(text), await native.text(text)]
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(stale.map((count) => count())).toEqual([1, 0])
    expect(shapeOf(result)).toEqual(shapeOf(expected))
    expect(await captured.read(1)).toMatchObject([{ id: 1, x: 'b' }])
  })

  it('undoes its first statement when its second fails, as Postgres does', async () => {
    await expect(captured.text("update t set x = 'b' where id = 1; insert into t values (2, 'z')")).rejects.toThrow()
    expect(await captured.read(1)).toMatchObject([{ id: 1, x: 'a' }])
  })
})
