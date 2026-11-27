/// <reference types="@cloudflare/workers-types" />
import { describe, expect, expectTypeOf, it } from 'vitest'
import { hasNodeSqlite, sqliteStorage } from '../../test/cloudflareSql.js'
import {
  blobColumn,
  integerColumn,
  nullableIntegerColumn,
  textColumn,
  type DurableStorage,
  type SqlStore,
} from './sqlStorage.js'

describe("a Durable Object's SQLite storage", () => {
  it("is Cloudflare's own, as it is", () => {
    expectTypeOf<DurableObjectState['storage']>().toExtend<DurableStorage>()
    expectTypeOf<DurableObjectState['storage']['sql']>().toExtend<SqlStore>()
  })

  it.runIf(hasNodeSqlite)('reads a column as the type it holds, a blob as an ArrayBuffer', () => {
    const { sql } = sqliteStorage().storage
    sql.exec('CREATE TABLE t (n INTEGER, s TEXT, b BLOB, z INTEGER)')
    sql.exec('INSERT INTO t VALUES (?, ?, ?, ?)', 7, 'seven', new Uint8Array([7]), null)
    const row = sql.exec('SELECT n, s, b, z FROM t').one()
    expect({
      n: integerColumn(row, 'n'),
      s: textColumn(row, 's'),
      b: [...new Uint8Array(blobColumn(row, 'b'))],
      z: nullableIntegerColumn(row, 'z'),
    }).toEqual({ n: 7, s: 'seven', b: [7], z: null })
  })

  it.runIf(hasNodeSqlite)('refuses a column that holds another type, or nothing, naming the column', () => {
    const { sql } = sqliteStorage().storage
    sql.exec('CREATE TABLE t (n INTEGER, s TEXT)')
    sql.exec('INSERT INTO t VALUES (?, ?)', null, '7')
    const row = sql.exec('SELECT n, s FROM t').one()
    expect(() => integerColumn(row, 's')).toThrow("The SQL column 's' holds string, not an integer")
    expect(() => integerColumn(row, 'n')).toThrow("The SQL column 'n' holds null, not an integer")
    expect(() => textColumn(row, 'missing')).toThrow("The SQL column 'missing' holds nothing, not text")
  })
})
