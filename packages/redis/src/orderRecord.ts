export { NO_ID, drawCommitId, decodeLaneOrder, decodeBroadcastOrder, laneWhere, historyOf }
export type { LaneStart, StartRead, Starts }

import { randomFillSync } from 'node:crypto'
import type { BroadcastKind } from 'telefunc/__internal'
import { assert } from './assert.js'
import { orderRecordRefusal } from './redisFailure.js'

/** Where a lane stands: its last commit's place and id, `NO_ID` before its first. */
type LaneStart = { place: number; id: bigint }

/** What a start script read, with the history it read it from: the node's replication id, and on a Cluster its config
 *  epoch. Another history than a route's last start can lack what the old one accepted. */
type StartRead<T> = { authority: string; read: T }

/** The history an authority names: its node's replication id, without a Cluster's config epoch. */
function historyOf(authority: string): string {
  const [history] = authority.split(':')
  assert(history !== undefined)
  return history
}

/** Where each lane of a route starts, by its lane key, and the history that says so. */
type Starts = { authority: string; starts: Map<string, LaneStart> }

/** The id of no commit, which a lane's first commit names as before it. */
const NO_ID = BigInt(0)

const ID_BYTES = 8
const POOL_BYTES = 4096
let pool = Buffer.alloc(0)
let drawn = 0

/** The id of one publish or commit attempt, which its frame carries and its lane's next commit names as before it.
 *  A pool is never refilled: a command ioredis holds still points into it. All zero means none, so it is never drawn. */
function drawCommitId(): Buffer {
  for (;;) {
    if (drawn === pool.byteLength) {
      pool = randomFillSync(Buffer.allocUnsafe(POOL_BYTES))
      drawn = 0
    }
    const id = pool.subarray(drawn, (drawn += ID_BYTES))
    if (id.readBigUInt64BE(0) !== NO_ID) return id
  }
}

/** A count commands.ts's scripts write: a whole number, a safe integer. Only its next past 2^53 - 1 is refused. */
function isCount(n: number): boolean {
  return Number.isSafeInteger(n) && n >= 0
}

/** A Room lane's record, as a refusal names it. */
function laneWhere(roomId: string, laneKey: string): string {
  return `lane ${laneKey} of the room '${roomId}'`
}

/** A Room lane's record, `<dc8>` as commands.ts's `ORDER_RECORD_LUA` packs it. */
function decodeLaneOrder(record: Buffer, where: string): LaneStart {
  if (record.byteLength === 16) {
    const start = { place: record.readDoubleLE(0), id: record.readBigUInt64BE(8) }
    if (isCount(start.place) && (start.place === 0) === (start.id === NO_ID)) return start
  }
  throw orderRecordRefusal(where)
}

/** Where a Broadcast key's `kind` stands, from its `<dddc8c8>` record: the kinds share the seq, and each counts its own
 *  places. */
function decodeBroadcastOrder(record: Buffer, kind: BroadcastKind, where: string): LaneStart {
  if (record.byteLength === 40) {
    const seq = record.readDoubleLE(0)
    const text = { place: record.readDoubleLE(8), id: record.readBigUInt64BE(24) }
    const binary = { place: record.readDoubleLE(16), id: record.readBigUInt64BE(32) }
    const valid = (start: LaneStart) => isCount(start.place) && (start.place === 0) === (start.id === NO_ID)
    if (isCount(seq) && seq >= text.place + binary.place && valid(text) && valid(binary)) {
      return kind === 'text' ? text : binary
    }
  }
  throw orderRecordRefusal(where)
}
