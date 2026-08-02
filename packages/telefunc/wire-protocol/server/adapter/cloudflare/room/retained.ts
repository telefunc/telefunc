/// <reference types="@cloudflare/workers-types" />
// Retained payloads are internally chunked to 1.5 MB rows below workerd's 2 MB cap.

import type { LaneId } from '../../../../backend/room/contract.js'
import { encodeLaneKey, type LaneParts, laneToParts, partsToLane } from './codec.js'
import { toBytes, type OrderMark } from './storage.js'

const MAX_RETAINED_CHUNK_BYTES = 1_500_000

type ManifestRow = {
  lane_key: string
  size: number
  seq: number
  ts: number
  lane_kind: string
  lane_member: string | null
  lane_track: string | null
}

// Install retained state inside the acceptance `transactionSync`; partial chunk replacement rolls back.
export function installRetained(
  sql: SqlStorage,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  mark: OrderMark,
): void {
  const key = encodeLaneKey(lane)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
  const chunkCount = Math.max(1, Math.ceil(payload.byteLength / MAX_RETAINED_CHUNK_BYTES))
  for (let i = 0; i < chunkCount; i++) {
    const slice = payload.subarray(i * MAX_RETAINED_CHUNK_BYTES, (i + 1) * MAX_RETAINED_CHUNK_BYTES)
    // Copy out of the subarray view so SQLite stores exactly the chunk bytes.
    sql.exec('INSERT INTO rt_chunk (inc, lane_key, i, bytes) VALUES (?, ?, ?, ?)', inc, key, i, new Uint8Array(slice))
  }
  const parts = laneToParts(lane)
  sql.exec(
    'INSERT OR REPLACE INTO rt_manifest (inc, lane_key, size, seq, ts, lane_kind, lane_member, lane_track) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    inc,
    key,
    payload.byteLength,
    mark.seq,
    mark.timestamp,
    parts.kind,
    parts.member,
    parts.track,
  )
}

export function readRetained(
  sql: SqlStorage,
  inc: string,
  lane: LaneId,
): { payload: Uint8Array; seq: number; timestamp: number } | null {
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
    const bytes = toBytes(chunk.bytes)
    payload.set(bytes, offset)
    offset += bytes.byteLength
  }
  if (
    !Number.isSafeInteger(manifest.seq) ||
    manifest.seq <= 0 ||
    !Number.isSafeInteger(manifest.ts) ||
    manifest.ts < 0
  ) {
    throw new Error('readRetained: invalid Room ordering position')
  }
  return { payload, seq: manifest.seq, timestamp: manifest.ts }
}

export function listRetained(sql: SqlStorage, inc: string): LaneId[] {
  const rows = sql
    .exec<Pick<ManifestRow, 'lane_kind' | 'lane_member' | 'lane_track'>>(
      'SELECT lane_kind, lane_member, lane_track FROM rt_manifest WHERE inc = ?',
      inc,
    )
    .toArray()
  return rows.map((row) => {
    const parts: LaneParts = { kind: row.lane_kind as LaneId['kind'], member: row.lane_member, track: row.lane_track }
    return partsToLane(parts)
  })
}

export function deleteRetained(sql: SqlStorage, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): void {
  if (lane === undefined && opts?.ifSeq !== undefined) throw new Error('deleteRetained: ifSeq requires a lane')
  if (opts?.ifSeq !== undefined && (!Number.isSafeInteger(opts.ifSeq) || opts.ifSeq <= 0)) {
    throw new Error('deleteRetained: ifSeq must be a positive safe integer')
  }
  if (lane === undefined) {
    sql.exec('DELETE FROM rt_manifest WHERE inc = ?', inc)
    sql.exec('DELETE FROM rt_chunk WHERE inc = ?', inc)
    return
  }
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
