/// <reference types="@cloudflare/workers-types" />
// The retained-generation invariant (I6), with chunking kept entirely backend-internal. A retained
// payload is split into rows ≤1.5 MB (workerd's row cap is 2 MB); the manifest row is the atomic pointer,
// swapped inside the acceptance transaction, and the superseded generation's chunk rows are deleted after
// the swap. The aggregate cap (16 MiB) is checked BEFORE anything is mutated so an over-cap retain throws
// without advancing the order domain.

import type { LaneId } from '../../../../backend/spi.js'
import { laneKey, type LaneParts, laneToParts, partsToLane } from './codec.js'
import type { OrderMark } from './storage.js'

export const MAX_RETAINED_CHUNK_BYTES = 1_500_000

type ManifestRow = {
  lane_key: string
  gen: number
  size: number
  chunks: number
  seq: number
  ts: number
  lane_kind: string
  lane_member: string | null
  lane_track: string | null
}

function toBytes(value: ArrayBuffer | Uint8Array): Uint8Array {
  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

// The aggregate is per generation across its OTHER retained lanes plus this new payload, so replacing one
// lane's retained never counts twice.
export function assertRetainedCapacity(sql: SqlStorage, inc: string, key: string, size: number, cap: number): void {
  const row = sql
    .exec<{ total: number }>(
      'SELECT COALESCE(SUM(size), 0) AS total FROM rt_manifest WHERE inc = ? AND lane_key != ?',
      inc,
      key,
    )
    .toArray()[0]
  const total = (row?.total ?? 0) + size
  if (total > cap) throw new Error(`commitLane: retained aggregate ${total} bytes exceeds the ${cap} byte cap`)
}

// Install a retained generation, meant to run inside the acceptance `transactionSync`. The manifest gen
// counter increments so the pointer swap is unambiguous; older chunk rows for the lane are pruned.
export function installRetained(
  sql: SqlStorage,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  mark: OrderMark,
): void {
  const key = laneKey(lane)
  const previous = sql
    .exec<{ gen: number }>('SELECT gen FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
    .toArray()[0]
  const gen = (previous?.gen ?? 0) + 1
  const chunkCount = Math.max(1, Math.ceil(payload.byteLength / MAX_RETAINED_CHUNK_BYTES))
  for (let i = 0; i < chunkCount; i++) {
    const slice = payload.subarray(i * MAX_RETAINED_CHUNK_BYTES, (i + 1) * MAX_RETAINED_CHUNK_BYTES)
    // Copy out of the subarray view so SQLite stores exactly the chunk bytes.
    sql.exec(
      'INSERT OR REPLACE INTO rt_chunk (inc, lane_key, gen, i, bytes) VALUES (?, ?, ?, ?, ?)',
      inc,
      key,
      gen,
      i,
      new Uint8Array(slice),
    )
  }
  const parts = laneToParts(lane)
  sql.exec(
    'INSERT OR REPLACE INTO rt_manifest (inc, lane_key, gen, size, chunks, seq, ts, lane_kind, lane_member, lane_track) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    inc,
    key,
    gen,
    payload.byteLength,
    chunkCount,
    mark.seq,
    mark.timestamp,
    parts.kind,
    parts.member,
    parts.track,
  )
  // Superseded generation's chunks are now unreachable through the manifest pointer — drop them.
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ? AND gen != ?', inc, key, gen)
}

export function readRetained(
  sql: SqlStorage,
  inc: string,
  lane: LaneId,
): { payload: Uint8Array; seq: number; timestamp: number } | null {
  const key = laneKey(lane)
  const manifest = sql
    .exec<ManifestRow>('SELECT * FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
    .toArray()[0]
  if (manifest === undefined) return null
  const chunks = sql
    .exec<{ bytes: ArrayBuffer }>(
      'SELECT bytes FROM rt_chunk WHERE inc = ? AND lane_key = ? AND gen = ? ORDER BY i',
      inc,
      key,
      manifest.gen,
    )
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

export function deleteRetained(sql: SqlStorage, inc: string, lane?: LaneId): void {
  if (lane === undefined) {
    sql.exec('DELETE FROM rt_manifest WHERE inc = ?', inc)
    sql.exec('DELETE FROM rt_chunk WHERE inc = ?', inc)
    return
  }
  const key = laneKey(lane)
  sql.exec('DELETE FROM rt_manifest WHERE inc = ? AND lane_key = ?', inc, key)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ? AND lane_key = ?', inc, key)
}
