import { PGlite } from '@electric-sql/pglite'
import { sql } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/pglite'
import { describe, expect, it } from 'vitest'
import { liveDrizzle } from '@telefunc/live/drizzle'

describe('PGlite capture', () => {
  // The tenant sees only row 1, so a write captured with a widened `RETURNING` would update row 1 only.
  it('plans a write after the role change queued before it', async () => {
    const pglite = new PGlite()
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
})
