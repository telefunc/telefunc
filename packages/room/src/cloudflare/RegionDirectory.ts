/// <reference types="@cloudflare/workers-types" />
export { RegionDirectory }

import { DIRECTORY_PAGE_SIZE, type DirectoryPage } from '../backend/contract.js'
import { keyRange } from './storage.js'

/** How long an unlisted room's version stays, refusing a call its authority made before and that arrives later. */
const ROOM_DIRECTORY_TOMBSTONE_MS = 24 * 60 * 60 * 1000
/** The tombstones past their time each unlisting deletes, so they go at least as fast as unlistings come. */
const TOMBSTONES_DELETED_PER_UNLIST = 2

/** The rooms whose first listing was written from one region, which `Room.list()` reads a page of per region. Each row
 *  is its room's listing at the version its authority last sent: an incarnation, or none (a tombstone). */
class RegionDirectory {
  private readonly sql: SqlStorage

  constructor(ctx: DurableObjectState) {
    this.sql = ctx.storage.sql
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS room
        (room_id TEXT PRIMARY KEY, inc_tag TEXT, version INTEGER NOT NULL, unlisted_at INTEGER);
      CREATE INDEX IF NOT EXISTS room_listed ON room (room_id) WHERE inc_tag IS NOT NULL;
      CREATE INDEX IF NOT EXISTS room_unlisted ON room (unlisted_at) WHERE inc_tag IS NULL;
    `)
  }

  async list(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const range = keyRange('room_id', prefix, cursor)
    const matching = this.sql
      .exec<{
        room_id: string
        inc_tag: string
      }>(
        `SELECT room_id, inc_tag FROM room WHERE ${range.where} AND inc_tag IS NOT NULL ORDER BY room_id LIMIT ?`,
        ...range.args,
        DIRECTORY_PAGE_SIZE + 1,
      )
      .toArray()
    const page = matching.slice(0, DIRECTORY_PAGE_SIZE)
    const entries = page.map((row) => ({ roomId: row.room_id, incTag: row.inc_tag }))
    const last = page[page.length - 1]
    const more = last !== undefined && matching.length > DIRECTORY_PAGE_SIZE
    return more ? { entries, cursor: last.room_id } : { entries }
  }

  /** Lists `roomId` with `incTag`, or unlists it (null), unless it has a newer `version` of the room's listing. */
  async show(roomId: string, incTag: string | null, version: number): Promise<void> {
    const now = Date.now()
    this.sql.exec(
      'INSERT INTO room (room_id, inc_tag, version, unlisted_at) VALUES (?, ?, ?, ?) ON CONFLICT (room_id) DO UPDATE ' +
        'SET inc_tag = excluded.inc_tag, version = excluded.version, unlisted_at = excluded.unlisted_at ' +
        'WHERE excluded.version > room.version',
      roomId,
      incTag,
      version,
      incTag === null ? now : null,
    )
    if (incTag !== null) return
    this.sql.exec(
      'DELETE FROM room WHERE room_id IN (SELECT room_id FROM room WHERE inc_tag IS NULL AND unlisted_at < ? ' +
        'ORDER BY unlisted_at LIMIT ?)',
      now - ROOM_DIRECTORY_TOMBSTONE_MS,
      TOMBSTONES_DELETED_PER_UNLIST,
    )
  }
}
