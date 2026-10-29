export { RoomSession, decodeSessionRecord, sessionRecordBytes }
export type { SessionRecord }

// A server's lease in one room incarnation, on the store's clock: the members it joined and the pages' views it holds
// name it, and once it lapses one other server claims it and removes them (see `membership.ts`).

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import type { CellMutation } from '../../backend/room/contract.js'
import { ROOM_SESSION_TTL_MS } from './constants.js'
import { sessionCellKey } from './cells.js'
import { decodeRoomText } from './lanes.js'
assertIsNotBrowser()

const sessionTextEncoder = new TextEncoder()

/** Lapses at `until`, authority time; `cleaner` names the server that claimed it to remove what it hosted. */
type SessionRecord = { until: number; cleaner?: string }

function decodeSessionRecord(raw: Uint8Array): SessionRecord {
  return JSON.parse(decodeRoomText(raw)) as SessionRecord
}

/** A lease from `now`, the authority time its exchange read. */
function sessionRecordBytes(now: number, cleaner?: string): Uint8Array {
  const record: SessionRecord = { until: now + ROOM_SESSION_TTL_MS, cleaner }
  return sessionTextEncoder.encode(JSON.stringify(record))
}

/** One per replica. Its id changes once its cell is lost: claimed, or removed after a claim. */
class RoomSession {
  private _id = crypto.randomUUID()
  /** Whether an exchange hosting something under `_id` committed, so its cell was written. */
  private _created = false
  /** The exchange writing the cell, which the writes that would host something await. */
  creating: Promise<void> | null = null

  get id(): string {
    return this._id
  }

  get created(): boolean {
    return this._created
  }

  /** What an exchange hosting something under `id` adds, given the cells it read with the session's: nothing while the
   *  cell is live, a new lease while it is absent or lapsed unclaimed, `null` once the session is lost. */
  hosting(id: string, cells: ReadonlyMap<string, Uint8Array>, now: number): CellMutation[] | null {
    if (id !== this._id) return null
    const key = sessionCellKey(id)
    const raw = cells.get(key)
    if (raw === undefined) return this._created ? null : [{ key, bytes: sessionRecordBytes(now) }]
    const record = decodeSessionRecord(raw)
    if (record.cleaner !== undefined) return null
    return record.until < now ? [{ key, bytes: sessionRecordBytes(now) }] : []
  }

  hosted(id: string): void {
    if (id === this._id) this._created = true
  }

  /** The session `id` was found lost; what it hosted is its cleaner's. */
  lost(id: string): void {
    if (id !== this._id) return
    this._id = crypto.randomUUID()
    this._created = false
  }
}
