export { RegionDirectory }

import { DIRECTORY_PAGE_SIZE, type DirectoryPage } from '../backend/contract.js'
import {
  integerColumn,
  nullableIntegerColumn,
  textColumn,
  type DurableStorage,
  type SqlStore,
} from 'telefunc/__internal'
import { keyRange } from './storage.js'

/** How long an unlisted room's version stays, refusing a call its authority made before and that arrives later. */
const ROOM_DIRECTORY_TOMBSTONE_MS = 24 * 60 * 60 * 1000
/** How often at most the directory wakes to delete the tombstones past their time, while it has any. */
const TOMBSTONE_SWEEP_EVERY_MS = 60 * 60 * 1000
/** The most tombstones one wake deletes; one that leaves more past their time wakes again at once. */
const TOMBSTONES_PER_SWEEP = 1_000

/** The rooms whose first listing was written from one region, which `Room.list()` reads a page of per region. Each row
 *  is its room's listing at the version its authority last sent: an incarnation, or none (a tombstone). */
class RegionDirectory {
  private readonly storage: DurableStorage
  private readonly sql: SqlStore
  /** Whether this run of the object set the alarm that is stored: an unlisting sets it where it didn't. */
  private alarmSet = false

  constructor(ctx: { storage: DurableStorage }) {
    this.storage = ctx.storage
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
      .exec(
        `SELECT room_id, inc_tag, version FROM room WHERE ${range.where} AND inc_tag IS NOT NULL ORDER BY room_id LIMIT ?`,
        ...range.args,
        DIRECTORY_PAGE_SIZE + 1,
      )
      .toArray()
    const entries = matching.slice(0, DIRECTORY_PAGE_SIZE).map((row) => ({
      roomId: textColumn(row, 'room_id'),
      incTag: textColumn(row, 'inc_tag'),
      version: String(integerColumn(row, 'version')),
    }))
    const last = entries[entries.length - 1]
    const more = last !== undefined && matching.length > DIRECTORY_PAGE_SIZE
    return more ? { entries, cursor: last.roomId } : { entries }
  }

  /** Lists `roomId` with `incTag`, or unlists it (null), unless it has a newer `version` of the room's listing. An
   *  unlisting keeps its version as a tombstone, which the alarm deletes once its time passed. */
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
    // Tombstones age in order, so an alarm this run set comes for an older one first.
    if (incTag !== null || this.alarmSet) return
    const due = this.nextTombstoneDue()
    // In the turn of its write, with no `await` between them, so both are stored or neither.
    if (due !== null) await this.setAlarm(due)
  }

  /** Deletes the tombstones past their time, a batch at a time, and wakes again for the next. */
  async alarm(): Promise<void> {
    // Cloudflare unsets the alarm that runs this.
    this.alarmSet = false
    const now = Date.now()
    this.sql.exec(
      'DELETE FROM room WHERE room_id IN (SELECT room_id FROM room WHERE inc_tag IS NULL AND unlisted_at <= ? ' +
        'ORDER BY unlisted_at LIMIT ?)',
      now - ROOM_DIRECTORY_TOMBSTONE_MS,
      TOMBSTONES_PER_SWEEP,
    )
    const due = this.nextTombstoneDue()
    if (due === null) return
    await this.setAlarm(due <= now ? now : Math.max(due, now + TOMBSTONE_SWEEP_EVERY_MS))
  }

  /** When the oldest tombstone's time passes, null where there is none. */
  private nextTombstoneDue(): number | null {
    const oldest = nullableIntegerColumn(
      this.sql.exec('SELECT MIN(unlisted_at) AS oldest FROM room WHERE inc_tag IS NULL').one(),
      'oldest',
    )
    return oldest === null ? null : oldest + ROOM_DIRECTORY_TOMBSTONE_MS
  }

  private setAlarm(at: number): Promise<void> {
    this.alarmSet = true
    return this.storage.setAlarm(at)
  }
}
