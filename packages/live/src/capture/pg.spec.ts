// Two pg pools of one database: identical live reads share a result only when the pools parse its values alike.

import { AsyncLocalStorage } from 'node:async_hooks'
import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import pg from 'pg'
import { REQUEST_CONTEXT } from 'telefunc'
import { expect, it } from 'vitest'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'
import { emptyPGlite } from '../test/pglite.js'

const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

it('gives identical live reads of two pg pools each the values its own types parse', async () => {
  class Amount {
    constructor(readonly text: string) {}
  }
  const pglite = await emptyPGlite()
  await pglite.exec('create table t (id int primary key, amount numeric); insert into t values (1, 12.5)')
  const server = new PGLiteSocketServer({ db: pglite, port: 0, maxConnections: 2 })
  await server.start()
  const connectionString = `postgres://postgres@${server.getServerConn()}/postgres`
  const types = {
    getTypeParser: (oid: number, format?: string) =>
      oid === 1700 ? (text: string) => new Amount(text) : pg.types.getTypeParser(oid, format as 'text'),
  }
  const custom = new pg.Pool({ connectionString, max: 1, types })
  const plain = new pg.Pool({ connectionString, max: 1 })
  for (const pool of [custom, plain]) liveDatabaseOf(pool, {})
  const watched = (pool: pg.Pool) =>
    store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
      readLive(() => pool.query('select amount from t where id = $1', [1]), 'the spec'),
    )
  const amounts = (await Promise.all([watched(custom), watched(plain)])).map(
    (cell) => (cell.send() as pg.QueryResult).rows[0].amount,
  )
  expect(amounts).toStrictEqual([new Amount('12.5'), '12.5'])
  await Promise.all([custom.end(), plain.end()])
  await server.stop()
  await pglite.close()
})
