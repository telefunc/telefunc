export { cursorRead }

import type pg from 'pg'
import Cursor from 'pg-cursor'

/** Runs `text` through a `pg-cursor` on a client of `pool`, reading it with the promise `read()` returns when given no
 *  callback. */
async function cursorRead(pool: pg.Pool, text: string, values: unknown[]): Promise<unknown[]> {
  // `pool.query()` doesn't release its client after a `Submittable`.
  const client = await pool.connect()
  const cursor = client.query(new Cursor(text, values))
  try {
    return await cursor.read(1000)
  } finally {
    await cursor.close()
    client.release()
  }
}
