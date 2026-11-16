// Two pg pools of one database, parsing values their own ways: their identical live reads share no result.

import { AsyncLocalStorage } from 'node:async_hooks'
import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import pg from 'pg'
import { REQUEST_CONTEXT } from 'telefunc'
import { expect, it } from 'vitest'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'
import { emptyPGlite } from '../test/pglite.js'

const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

/** The `amount` that the live read of each of `pools`, made for one database, gets when they run at once. */
async function amountsOf(pools: (connectionString: string) => pg.Pool[]): Promise<unknown[]> {
  const pglite = await emptyPGlite()
  await pglite.exec('create table t (id int primary key, amount numeric); insert into t values (1, 12.5)')
  const server = new PGLiteSocketServer({ db: pglite, port: 0, maxConnections: 2 })
  await server.start()
  const made = pools(`postgres://postgres@${server.getServerConn()}/postgres`)
  for (const pool of made) liveDatabaseOf(pool, {})
  const watched = (pool: pg.Pool) =>
    store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(() => pool.query('select amount from t where id = $1', [1]), 'the spec'),
    )
  const cells = await Promise.all(made.map(watched))
  await Promise.all(made.map((pool) => pool.end()))
  await server.stop()
  await pglite.close()
  return cells.map((cell) => (cell.send() as pg.QueryResult).rows[0].amount)
}

it('gives identical live reads of two pg pools each the values its own types parse', async () => {
  class Amount {
    constructor(readonly text: string) {}
  }
  const types = {
    getTypeParser: (oid: number, format?: string) =>
      oid === 1700 ? (text: string) => new Amount(text) : pg.types.getTypeParser(oid, format as 'text'),
  }
  const amounts = await amountsOf((connectionString) => [
    new pg.Pool({ connectionString, max: 1, types }),
    new pg.Pool({ connectionString, max: 1 }),
  ])
  expect(amounts).toStrictEqual([new Amount('12.5'), '12.5'])
})

it("gives identical live reads of two pg pools each the values its clients' setTypeParser() parse", async () => {
  const amounts = await amountsOf((connectionString) => {
    const numbers = new pg.Pool({ connectionString, max: 1 })
    numbers.on('connect', (client) => client.setTypeParser(1700, Number))
    return [numbers, new pg.Pool({ connectionString, max: 1 })]
  })
  expect(amounts).toStrictEqual([12.5, '12.5'])
})
