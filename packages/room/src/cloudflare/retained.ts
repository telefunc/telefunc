export { installRetained, readRetained, retainedOwner, listRetained, deleteRetained }

import type { LaneId, RetainedFrame, RetainedOf } from '../backend/contract.js'
import { binaryLaneKeyPrefix, decodeLaneKey, encodeLaneKey } from '../backend/laneKey.js'
import {
  blobColumn,
  integerColumn,
  nullableTextColumn,
  textColumn,
  type OrderingInfo,
  type SqlCursor,
  type SqlStore,
} from 'telefunc/__internal'
import { keyRange } from './storage.js'

/** Retained payloads are internally chunked to 1.5 MB rows below workerd's 2 MB cap. */
const MAX_RETAINED_CHUNK_BYTES = 1_500_000

/** Install retained state inside the acceptance `transactionSync`; partial chunk replacement rolls back. `owner` is the
 *  member the frame is from. */
function installRetained(
  sql: SqlStore,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  mark: OrderingInfo,
  owner: string | undefined,
): void {
  const key = encodeLaneKey(lane)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
  const chunkCount = Math.max(1, Math.ceil(payload.byteLength / MAX_RETAINED_CHUNK_BYTES))
  for (let i = 0; i < chunkCount; i++) {
    const slice = payload.subarray(i * MAX_RETAINED_CHUNK_BYTES, (i + 1) * MAX_RETAINED_CHUNK_BYTES)
    sql.exec('INSERT INTO rt_chunk (inc, lane_key, i, bytes) VALUES (?, ?, ?, ?)', inc, key, i, slice)
  }
  sql.exec(
    'INSERT OR REPLACE INTO rt_manifest (inc, lane_key, track, size, seq, ts, owner) VALUES (?, ?, ?, ?, ?, ?, ?)',
    inc,
    key,
    lane.kind === 'binary' ? lane.track : null,
    payload.byteLength,
    mark.seq,
    mark.timestamp,
    owner ?? null,
  )
}

function readRetained(sql: SqlStore, inc: string, lane: LaneId): RetainedFrame | null {
  const key = encodeLaneKey(lane)
  const manifest = sql
    .exec('SELECT size, seq, ts FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
    .toArray()[0]
  if (manifest === undefined) return null
  const chunks = sql.exec('SELECT bytes FROM rt_chunk WHERE inc = ? AND lane_key = ? ORDER BY i', inc, key).toArray()
  const payload = new Uint8Array(integerColumn(manifest, 'size'))
  let offset = 0
  for (const chunk of chunks) {
    const bytes = new Uint8Array(blobColumn(chunk, 'bytes'))
    payload.set(bytes, offset)
    offset += bytes.byteLength
  }
  return { payload, seq: integerColumn(manifest, 'seq'), timestamp: integerColumn(manifest, 'ts') }
}

/** The member `lane`'s retained frame is from, null for none or a frame no member sent. */
function retainedOwner(sql: SqlStore, inc: string, lane: LaneId): string | null {
  const query = 'SELECT owner FROM rt_manifest WHERE inc = ? AND lane_key = ?'
  const row = sql.exec(query, inc, encodeLaneKey(lane)).toArray()[0]
  return row === undefined ? null : nullableTextColumn(row, 'owner')
}

/** A member's lanes are a primary-key range: their keys share its prefix. A track's are an rt_manifest_track range. */
function listRetained(sql: SqlStore, inc: string, of?: RetainedOf): LaneId[] {
  const query = 'SELECT lane_key FROM rt_manifest WHERE inc = ?'
  let rows: SqlCursor
  if (of === undefined) rows = sql.exec(query, inc)
  else if ('track' in of) rows = sql.exec(`${query} AND track = ?`, inc, of.track)
  else {
    const range = keyRange('lane_key', binaryLaneKeyPrefix(of.member))
    rows = sql.exec(`${query} AND ${range.where}`, inc, ...range.args)
  }
  return rows.toArray().map((row) => decodeLaneKey(textColumn(row, 'lane_key')))
}

function deleteRetained(sql: SqlStore, inc: string, lane: LaneId): void {
  const key = encodeLaneKey(lane)
  sql.exec('DELETE FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
}
