export {
  assertRoomId,
  evictMember,
  mutateCells,
  presenceCount,
  readCell,
  readLiveMember,
  readMembers,
  requireRoom,
  resolveIdentityMembers,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertUsage } from '../../../utils/assert.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation } from '../../backend/room/contract.js'
import { ROOM_MEMBER_TTL_MS } from '../constants.js'
import { uuidToBytes } from '../binary.js'
import { RoomError } from '../errors.js'
import {
  roomIdentityKvPrefix,
  roomIdentityMemberKvKey,
  roomMemberCleanupKvKey,
  roomMemberCleanupKvPrefix,
  roomMemberKvKey,
  roomMemberKvPrefix,
} from '../keys.js'
import { leaveCauseToWire } from '../model.js'
import {
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomDataEnvelope,
  type RoomMemberRecord,
} from '../protocol.js'
import type { LeaveCause } from '../types.js'
import { SEMANTIC_LANE, configFromHead, decodeRoomText, encodeRoomText, publishCtrl } from './lanes.js'

// Room owns conflict recovery: 16 attempts with 1→64 ms jitter, then a contention RoomError.
const ROOM_CX_ATTEMPTS = 16
type CellSelector = { keys: string[] } | { prefix: string }
type CellPlan<T> = { value: T; mutations: CellMutation[] }
type PendingMemberCleanup = { cause: ReturnType<typeof leaveCauseToWire> }

async function readCellSet(
  roomId: string,
  inc: string,
  selector: CellSelector,
): Promise<{ revision: string; cells: Map<string, Uint8Array> }> {
  const result = await getRoomBackend().readCells(roomId, inc, selector)
  if ('staleInc' in result) throw new RoomError(`Room is closed: ${roomId}`)
  return result
}

async function readCell(roomId: string, inc: string, key: string): Promise<Uint8Array | null> {
  const { cells } = await readCellSet(roomId, inc, { keys: [key] })
  return cells.get(key) ?? null
}

async function readLiveMember(roomId: string, inc: string, id: string): Promise<RoomMemberRecord | null> {
  const raw = await readCell(roomId, inc, roomMemberKvKey(roomId, id))
  if (raw === null) return null
  const record = parse(decodeRoomText(raw)) as RoomMemberRecord
  if (Date.now() - record.seenAt <= ROOM_MEMBER_TTL_MS) return record
  return await reapExpiredMember({ roomId, inc, id, record })
}

async function mutateCells<T>(
  roomId: string,
  inc: string,
  selector: CellSelector,
  plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T>,
): Promise<T> {
  const backend = getRoomBackend()
  for (let attempt = 0; attempt < ROOM_CX_ATTEMPTS; attempt++) {
    const read = await backend.readCells(roomId, inc, selector)
    if ('staleInc' in read) throw new RoomError(`Room is closed: ${roomId}`)
    const next = plan(read.cells)
    if (next.mutations.length === 0) return next.value
    const result = await backend.compareExchangeCells(roomId, inc, read.revision, next.mutations)
    if (result === 'committed') return next.value
    if (result === 'stale-inc') throw new RoomError(`Room is closed: ${roomId}`)
    const ceiling = Math.min(64, 2 ** attempt)
    await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * ceiling) + 1))
  }
  throw new RoomError(`Room update contention: ${roomId}`)
}

async function requireRoom(id: string): Promise<{ config: RoomConfigRecord }> {
  assertRoomId(id)
  const current = await getRoomBackend().readHead(id)
  if (current === null || current.head.state !== 'open' || current.head.currentInc === null) {
    throw new RoomError(`Room not found: ${id}`)
  }
  return { config: configFromHead(current.head) }
}

async function readMembers(roomId: string, inc: string, ids?: string[]): Promise<MemberSnapshot[]> {
  if (ids === undefined) await completePendingMemberCleanups(roomId, inc)
  // Authority reads keep replica lag from reaping a heartbeat that already renewed.
  const memberKeys =
    ids === undefined ? await listMemberKeys(roomId, inc) : ids.map((id) => ({ key: roomMemberKvKey(roomId, id), id }))
  const { cells } = await readCellSet(roomId, inc, { keys: memberKeys.map(({ key }) => key) })
  const members: MemberSnapshot[] = []
  for (const { key, id } of memberKeys) {
    const raw = cells.get(key)
    if (raw === undefined) continue
    const candidate = parse(decodeRoomText(raw)) as RoomMemberRecord
    const record =
      Date.now() - candidate.seenAt > ROOM_MEMBER_TTL_MS
        ? await reapExpiredMember({ roomId, inc, id, record: candidate })
        : candidate
    if (record !== null) members.push(memberSnapshot(id, record))
  }
  return members
}

async function reapExpiredMember(input: {
  roomId: string
  inc: string
  id: string
  record: RoomMemberRecord
}): Promise<RoomMemberRecord | null> {
  const { roomId, inc, id, record } = input
  const key = roomMemberKvKey(roomId, id)
  const cleanupKey = roomMemberCleanupKvKey(roomId, id)
  const siblingKeys = [key]
  if (record.identity !== undefined) siblingKeys.push(roomIdentityMemberKvKey(roomId, record.identity, id))
  const cleanup: PendingMemberCleanup = { cause: { cause: 'disconnected' } }
  const reap = await mutateCells<{ kind: 'missing' } | { kind: 'reaped' } | { kind: 'live'; record: RoomMemberRecord }>(
    roomId,
    inc,
    { keys: [...siblingKeys, cleanupKey] },
    (current) => {
      const latest = current.get(key)
      if (latest === undefined) return { value: { kind: 'missing' } as const, mutations: [] }
      const latestRecord = parse(decodeRoomText(latest)) as RoomMemberRecord
      return Date.now() - latestRecord.seenAt > ROOM_MEMBER_TTL_MS
        ? {
            value: { kind: 'reaped' } as const,
            mutations: [
              ...siblingKeys.map((siblingKey) => ({ key: siblingKey })),
              ...(current.has(cleanupKey)
                ? []
                : [{ key: cleanupKey, set: { bytes: encodeRoomText(stringify(cleanup)) } }]),
            ],
          }
        : { value: { kind: 'live', record: latestRecord } as const, mutations: [] }
    },
  )
  if (reap.kind === 'reaped') await finishPendingMemberCleanup(roomId, inc, id)
  return reap.kind === 'live' ? reap.record : null
}

function memberSnapshot(id: string, record: RoomMemberRecord): MemberSnapshot {
  return {
    id,
    meta: record.meta,
    joinedAt: record.joinedAt,
    metaSeq: record.metaSeq,
    identity: record.identity ?? null,
    ...(record.tracks === undefined ? {} : { tracks: record.tracks }),
    ...(record.hidden ? { hidden: true } : {}),
  }
}

async function listMemberKeys(roomId: string, inc: string): Promise<Array<{ key: string; id: string }>> {
  const prefix = roomMemberKvPrefix(roomId)
  const memberKeys: Array<{ key: string; id: string }> = []
  const { cells } = await readCellSet(roomId, inc, { prefix })
  for (const key of cells.keys()) {
    const id = key.slice(prefix.length)
    if (uuidToBytes(id)) memberKeys.push({ key, id })
  }
  return memberKeys
}

async function presenceCount(roomId: string, inc: string): Promise<number> {
  return (await readMembers(roomId, inc)).filter((member) => !member.hidden).length
}

async function resolveIdentityMembers(roomId: string, inc: string, identity: string): Promise<string[]> {
  const prefix = roomIdentityKvPrefix(roomId, identity)
  const members: string[] = []
  const markers = await readCellSet(roomId, inc, { prefix })
  for (const key of markers.cells.keys()) {
    const memberId = key.slice(prefix.length)
    if ((await readLiveMember(roomId, inc, memberId))?.identity === identity) {
      members.push(memberId)
    }
  }
  return members
}

async function dropRetainedTextOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  const retained = await backend.readRetained(roomId, inc, SEMANTIC_LANE)
  if (retained === null) return
  const envelope = parse(decodeRoomText(retained.payload)) as RoomDataEnvelope
  if (envelope.from !== memberId) return
  await backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: retained.seq })
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc)) {
    if (lane.kind === 'binary' && lane.member === memberId) await backend.deleteRetained(roomId, inc, lane)
  }
  await dropRetainedTextOwnedBy(roomId, inc, memberId)
}

async function evictMember(
  roomId: string,
  inc: string,
  memberId: string,
  identity: string | undefined,
  cause: LeaveCause,
): Promise<void> {
  const memberKey = roomMemberKvKey(roomId, memberId)
  const cleanupKey = roomMemberCleanupKvKey(roomId, memberId)
  const keys = [memberKey]
  if (identity !== undefined) keys.push(roomIdentityMemberKvKey(roomId, identity, memberId))
  const hasCleanup = await mutateCells(roomId, inc, { keys: [...keys, cleanupKey] }, (cells) => {
    const pending = cells.has(cleanupKey)
    if (!cells.has(memberKey) && !pending) return { value: false, mutations: [] }
    const cleanup: PendingMemberCleanup = { cause: leaveCauseToWire(cause) }
    return {
      value: true,
      mutations: [
        ...keys.map((key) => ({ key })),
        ...(pending ? [] : [{ key: cleanupKey, set: { bytes: encodeRoomText(stringify(cleanup)) } }]),
      ],
    }
  })
  if (hasCleanup) await finishPendingMemberCleanup(roomId, inc, memberId)
}

async function completePendingMemberCleanups(roomId: string, inc: string): Promise<void> {
  const prefix = roomMemberCleanupKvPrefix(roomId)
  const { cells } = await readCellSet(roomId, inc, { prefix })
  for (const key of cells.keys()) {
    const memberId = key.slice(prefix.length)
    if (uuidToBytes(memberId)) await finishPendingMemberCleanup(roomId, inc, memberId)
  }
}

async function finishPendingMemberCleanup(roomId: string, inc: string, memberId: string): Promise<void> {
  const key = roomMemberCleanupKvKey(roomId, memberId)
  const raw = await readCell(roomId, inc, key)
  if (raw === null) return
  const cleanup = parse(decodeRoomText(raw)) as PendingMemberCleanup
  await dropRetainedOwnedBy(roomId, inc, memberId)
  await publishCtrl(roomId, inc, { __r: 'leave', id: memberId, ...cleanup.cause })
  await mutateCells(roomId, inc, { keys: [key] }, (cells) => ({
    value: undefined,
    mutations: cells.has(key) ? [{ key }] : [],
  }))
}

function assertRoomId(id: unknown): asserts id is string {
  assertUsage(typeof id === 'string' && id.length > 0, 'The room ID should be a non-empty string')
}
