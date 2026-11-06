/// <reference types="@cloudflare/workers-types" />
export { RegionDirectory }

import { DIRECTORY_PAGE_SIZE, type DirectoryPage } from '../backend/contract.js'
import { keyRange } from './storage.js'

/** The rooms whose first listing was written from one region, which `Room.list()` reads a page of per region. */
class RegionDirectory {
  private readonly sql: SqlStorage

  constructor(ctx: DurableObjectState) {
    this.sql = ctx.storage.sql
    this.sql.exec('CREATE TABLE IF NOT EXISTS room (room_id TEXT PRIMARY KEY, inc_tag TEXT NOT NULL)')
  }

  async list(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const range = keyRange('room_id', prefix, cursor)
    const matching = this.sql
      .exec<{
        room_id: string
        inc_tag: string
      }>(
        `SELECT room_id, inc_tag FROM room WHERE ${range.where} ORDER BY room_id LIMIT ?`,
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

  async add(roomId: string, incTag: string): Promise<void> {
    this.sql.exec('INSERT OR REPLACE INTO room (room_id, inc_tag) VALUES (?, ?)', roomId, incTag)
  }

  /** A stale tag removes nothing: the room was listed again meanwhile. */
  async remove(roomId: string, incTag: string): Promise<void> {
    this.sql.exec('DELETE FROM room WHERE room_id = ? AND inc_tag = ?', roomId, incTag)
  }
}
