export { EVENT_TAG, eventQuery }

import pg from 'pg'

/** A write the `pg` lanes send as an event-based `pg.Query`. */
const EVENT_TAG = 'insert into tags (todo_id, name) values ($1, $2)'

/** Runs `text` as an event-based `pg.Query` (rows come as events) on a client of `pool`. */
async function eventQuery(pool: pg.Pool, text: string, values: unknown[]): Promise<unknown[]> {
  // `pool.query()` doesn't release its client after a `Submittable`.
  const client = await pool.connect()
  try {
    return await new Promise((resolve, reject) => {
      const query = new pg.Query(text, values)
      const rows: unknown[] = []
      query.on('row', (row) => rows.push(row))
      query.on('end', () => resolve(rows))
      query.on('error', reject)
      client.query(query)
    })
  } finally {
    client.release()
  }
}
