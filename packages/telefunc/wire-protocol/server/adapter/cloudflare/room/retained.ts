/// <reference types="@cloudflare/workers-types" />
// Retained payloads are internally chunked to 1.5 MB rows below workerd's 2 MB cap.

import type { LaneId, RetainedFrame } from '../../../../backend/room/contract.js'
import { decodeLaneKey, encodeLaneKey } from '../../../../backend/room/lane-key.js'
import type { OrderingInfo } from '../../../../ordering-frame.js'

const MAX_RETAINED_CHUNK_BYTES = 1_500_000

type ManifestRow = { lane_key: string; size: number; seq: number; ts: number }

// Install retained state inside the acceptance `transactionSync`; partial chunk replacement rolls back.
export function installRetained(
  sql: SqlStorage,
  inc: string,
  key: string,
  payload: Uint8Array,
  mark: OrderingInfo,
): void {
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
  const chunkCount = Math.max(1, Math.ceil(payload.byteLength / MAX_RETAINED_CHUNK_BYTES))
  for (let i = 0; i < chunkCount; i++) {
    const slice = payload.subarray(i * MAX_RETAINED_CHUNK_BYTES, (i + 1) * MAX_RETAINED_CHUNK_BYTES)
    // Copy out of the subarray view so SQLite stores exactly the chunk bytes.
    sql.exec('INSERT INTO rt_chunk (inc, lane_key, i, bytes) VALUES (?, ?, ?, ?)', inc, key, i, new Uint8Array(slice))
  }
  sql.exec(
    'INSERT OR REPLACE INTO rt_manifest (inc, lane_key, size, seq, ts) VALUES (?, ?, ?, ?, ?)',
    inc,
    key,
    payload.byteLength,
    mark.seq,
    mark.timestamp,
  )
}

export function readRetained(sql: SqlStorage, inc: string, lane: LaneId): RetainedFrame | null {
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

export function listRetained(sql: SqlStorage, inc: string): LaneId[] {
  return sql
    .exec<Pick<ManifestRow, 'lane_key'>>('SELECT lane_key FROM rt_manifest WHERE inc = ?', inc)
    .toArray()
    .map((row) => decodeLaneKey(row.lane_key))
}

export function deleteRetained(sql: SqlStorage, inc: string, lane: LaneId, opts?: { ifSeq?: number }): void {
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
