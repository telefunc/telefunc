// libSQL on a local file: what a delete cascading through foreign keys tells the live reads.

import { AsyncLocalStorage } from 'node:async_hooks'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createClient } from '@libsql/client'
import { REQUEST_CONTEXT } from 'telefunc'
import { afterEach, describe, expect, it } from 'vitest'
import { liveDatabaseOf } from '../database.js'
import { readLive } from '../liveQuery.js'

const folders: string[] = []
afterEach(() => {
  for (const folder of folders.splice(0)) rmSync(folder, { recursive: true, force: true })
})

describe('a libSQL delete cascading two levels', () => {
  const SCHEMA = `create table users (id integer primary key);
    create table todos (id integer primary key, user_id integer references users on delete cascade);
    create table tags (id integer primary key, todo_id integer references todos on delete cascade);
    insert into users values (1), (2);
    insert into todos values (10, 1), (11, 1), (20, 2);
    insert into tags values (100, 10), (101, 11), (200, 20);`

  /** A live libSQL client on a new file with `SCHEMA`, the texts it executes, and a live read's stale signals. */
  async function cascading() {
    const folder = mkdtempSync(join(tmpdir(), 'live-libsql-'))
    folders.push(folder)
    const client = createClient({ url: `file:${join(folder, 'db')}` })
    await client.executeMultiple(SCHEMA)
    const sent: string[] = []
    const execute = client.execute.bind(client)
    client.execute = ((statement: Parameters<typeof execute>[0], args?: never) => {
      sent.push(typeof statement === 'string' ? statement : statement.sql)
      return execute(statement, args)
    }) as typeof execute
    const transaction = client.transaction.bind(client)
    client.transaction = async (mode) => {
      const tx = await transaction(mode)
      const txExecute = tx.execute.bind(tx)
      tx.execute = (statement) => {
        sent.push(typeof statement === 'string' ? statement : statement.sql)
        return txExecute(statement)
      }
      return tx
    }
    liveDatabaseOf(client, {})
    const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
    const read = async (text: string, id: number) => {
      const cell = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () =>
        readLive(async () => (await client.execute({ sql: text, args: [id] })).rows, 'the spec'),
      )
      let stale = 0
      cell.onStale(() => stale++)
      cell.send()
      return () => stale
    }
    return { client, sent, read }
  }

  it('stales only the reads of the rows it deletes', async () => {
    const { client, read } = await cascading()
    const [deleted, kept] = [
      await read('select * from tags where id = ?', 100),
      await read('select * from tags where id = ?', 200),
    ]
    await client.execute({ sql: 'delete from users where id = ?', args: [1] })
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect((await client.execute('select id from tags')).rows.map((row) => row.id)).toEqual([200])
    expect([deleted(), kept()]).toEqual([1, 0])
    client.close()
  })

  it('is sent as without cascades while no live read reads their tables', async () => {
    const { client, sent, read } = await cascading()
    await read('select * from users where id = ?', 2)
    sent.length = 0
    await client.execute({ sql: 'delete from users where id = ?', args: [1] })
    // As libSQL sends a write without a RETURNING of its own: then what it changed.
    expect(sent).toEqual([
      'delete from users where id = ? RETURNING json_array(CASE WHEN typeof("id") IN (\'null\', \'integer\', \'real\') THEN CAST("id" AS TEXT) ELSE 0 END) AS "__live_0"',
      'SELECT CAST(changes() AS TEXT), CAST(last_insert_rowid() AS TEXT)',
    ])
    client.close()
  })
})
