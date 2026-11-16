// Identical live reads of postgres.js clients share a result only when each caller gets the same rows from it as from its
// own read.

import { AsyncLocalStorage } from 'node:async_hooks'
import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import postgres from 'postgres'
import { REQUEST_CONTEXT } from 'telefunc'
import { expect, it } from 'vitest'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'
import { emptyPGlite } from '../test/pglite.js'

const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

it('gives identical live reads of two postgres.js clients each its own rows, and shares those of one client', async () => {
  const pglite = await emptyPGlite()
  await pglite.exec("create table t (id int primary key, full_name text); insert into t values (1, 'Alice')")
  const server = new PGLiteSocketServer({ db: pglite, port: 0, maxConnections: 2 })
  await server.start()
  const url = `postgres://postgres@${server.getServerConn()}/postgres`
  /** The reads of `t` each client sends. */
  const sent: string[] = []
  const clients = () => {
    const options = { max: 1, onnotice: () => {}, debug: (_: unknown, text: string) => void sent.push(text) }
    return [postgres(url, options), postgres(url, { ...options, transform: postgres.camel })] as const
  }
  const read = (sql: postgres.Sql) => sql`select full_name from t where id = ${1}`
  const native = clients()
  const expected = [[...(await read(native[0]))], [...(await read(native[1]))]]
  await Promise.all(native.map((sql) => sql.end()))
  expect(expected).toEqual([[{ full_name: 'Alice' }], [{ fullName: 'Alice' }]])

  const [plain, camel] = clients()
  for (const sql of [plain, camel]) liveDatabaseOf(sql, {})
  const watched = (sql: postgres.Sql) =>
    store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => readLive(() => read(sql), 'the spec'))
  const rowsOf = async (sqls: postgres.Sql[]) => {
    sent.length = 0
    const cells = await Promise.all(sqls.map(watched))
    return cells.map((cell) => [...(cell.send() as unknown[])])
  }
  expect(await rowsOf([plain, camel])).toEqual(expected)
  expect(sent.filter((text) => text.includes('from t'))).toHaveLength(2)
  expect(await rowsOf([camel, camel])).toEqual([expected[1], expected[1]])
  expect(sent.filter((text) => text.includes('from t'))).toHaveLength(1)
  await Promise.all([plain.end(), camel.end()])
  await server.stop()
  await pglite.close()
})

it("gives identical live reads of one postgres.js client the values of its custom type's class", async () => {
  class Amount {
    constructor(readonly text: string) {}
  }
  const pglite = await emptyPGlite()
  await pglite.exec('create table t (id int primary key, amount numeric); insert into t values (1, 12.5)')
  const server = new PGLiteSocketServer({ db: pglite, port: 0, maxConnections: 1 })
  await server.start()
  const amount = { to: 1700, from: [1700], serialize: String, parse: (text: string) => new Amount(text) }
  const sql = postgres(`postgres://postgres@${server.getServerConn()}/postgres`, {
    max: 1,
    onnotice: () => {},
    types: { amount },
  })
  liveDatabaseOf(sql, {})
  const watched = () =>
    store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(() => sql`select amount from t where id = ${1}`, 'the spec'),
    )
  const amounts = (await Promise.all([watched(), watched()])).map(
    (cell) => (cell.send() as { amount: unknown }[])[0]!.amount,
  )
  expect(amounts).toStrictEqual([new Amount('12.5'), new Amount('12.5')])
  expect(amounts[0]).not.toBe(amounts[1])
  await sql.end()
  await server.stop()
  await pglite.close()
})
