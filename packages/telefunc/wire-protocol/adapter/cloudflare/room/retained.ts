/// <reference types="@cloudflare/workers-types" />
export { installRetained, readRetained, listRetained, deleteRetained }

import type { LaneId, RetainedFrame, RetainedOf } from '../../../backend/room/contract.js'
import { binaryLaneKeyPrefix, decodeLaneKey, encodeLaneKey } from '../../../backend/room/laneKey.js'
import type { OrderingInfo } from '../../../wire/orderingFrame.js'
import { keyRange } from './storage.js'

/** Retained payloads are internally chunked to 1.5 MB rows below workerd's 2 MB cap. */
const MAX_RETAINED_CHUNK_BYTES = 1_500_000

type ManifestRow = { lane_key: string; size: number; seq: number; ts: number }

/** Install retained state inside the acceptance `transactionSync`; partial chunk replacement rolls back. */
function installRetained(sql: SqlStorage, inc: string, lane: LaneId, payload: Uint8Array, mark: OrderingInfo): void {
  const key = encodeLaneKey(lane)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
  const chunkCount = Math.max(1, Math.ceil(payload.byteLength / MAX_RETAINED_CHUNK_BYTES))
  for (let i = 0; i < chunkCount; i++) {
    const slice = payload.subarray(i * MAX_RETAINED_CHUNK_BYTES, (i + 1) * MAX_RETAINED_CHUNK_BYTES)
    sql.exec('INSERT INTO rt_chunk (inc, lane_key, i, bytes) VALUES (?, ?, ?, ?)', inc, key, i, slice)
  }
  sql.exec(
    'INSERT OR REPLACE INTO rt_manifest (inc, lane_key, track, size, seq, ts) VALUES (?, ?, ?, ?, ?, ?)',
    inc,
    key,
    lane.kind === 'binary' ? lane.track : null,
    payload.byteLength,
    mark.seq,
    mark.timestamp,
  )
}

function readRetained(sql: SqlStorage, inc: string, lane: LaneId): RetainedFrame | null {
  const key = encodeLaneKey(lane)
  const manifest = sql
    .exec<ManifestRow>('SELECT * FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
    .toArray()[0]
  if (manifest === undefined) return null
  const chunks = sql
    .exec<{ bytes: ArrayBuffer }>('SELECT bytes FROM rt_chunk WHERE inc = ? AND lane_key = ? ORDER BY i', inc, key)
    .toArray()
  const payload = new Uint8Array(manifest.size)
  let offset = 0
  for (const chunk of chunks) {
    const bytes = new Uint8Array(chunk.bytes)
    payload.set(bytes, offset)
    offset += bytes.byteLength
  }
  return { payload, seq: manifest.seq, timestamp: manifest.ts }
}

/** A member's lanes are a primary-key range: their keys share its prefix. A track's are an rt_manifest_track range. */
function listRetained(sql: SqlStorage, inc: string, of?: RetainedOf): LaneId[] {
  const query = 'SELECT lane_key FROM rt_manifest WHERE inc = ?'
  let rows: SqlStorageCursor<Pick<ManifestRow, 'lane_key'>>
  if (of === undefined) rows = sql.exec(query, inc)
  else if ('track' in of) rows = sql.exec(`${query} AND track = ?`, inc, of.track)
  else {
    const range = keyRange('lane_key', binaryLaneKeyPrefix(of.member))
    rows = sql.exec(`${query} AND ${range.where}`, inc, ...range.args)
  }
  return rows.toArray().map((row) => decodeLaneKey(row.lane_key))
}

function deleteRetained(sql: SqlStorage, inc: string, lane: LaneId, opts?: { ifSeq?: number }): void {
  const key = encodeLaneKey(lane)
  if (opts?.ifSeq !== undefined) {
    const manifest = sql
      .exec<Pick<ManifestRow, 'seq'>>('SELECT seq FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
      .toArray()[0]
    if (manifest === undefined || manifest.seq !== opts.ifSeq) return
  }
  sql.exec('DELETE FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
}
