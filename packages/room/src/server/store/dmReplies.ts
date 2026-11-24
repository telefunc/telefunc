export { STORE_DM_REPLY, storeDmReply, readDmReply, takeDmReplies, dropDmReply }

import type { RoomStore } from '../../backend/RoomStore.js'
import type { RoomOperation } from '../../backend/contract.js'
import { Slice } from '../../shared/Slice.js'
import { roomClosedError } from '../../shared/errors.js'
import type { RoomResult } from '../../shared/protocol.js'
import type { RoomDmAckEnvelope } from '../protocol.js'
import { deleteCells, readOpenCells } from './cellStore.js'
import { runOperation } from './operations.js'
import { decodeRoomRecord } from '../lanes/lanes.js'
import { ackIdOfDmReplyCellKey, dmReplyCellKey, dmReplyCellPrefix, memberCellKey } from './cells.js'

// A DM's reply, stored for its sender to take until the sender leaves: the `dm-ack` frame's own bytes, which read as the
// reply they carry.

type StoreDmReplyArgs = { sender: string; ackId: string; reply: Uint8Array }

/** Kept only while its sender is a member, whose leave drops what it never settled. */
const STORE_DM_REPLY: RoomOperation<StoreDmReplyArgs, void> = {
  name: 'storeDmReply',
  plan(tx, { sender, ackId, reply }) {
    if (tx.cell(memberCellKey(sender)) !== undefined) tx.write({ key: dmReplyCellKey(sender, ackId), bytes: reply })
  },
}

async function storeDmReply(
  store: RoomStore,
  roomId: string,
  inc: string,
  sender: string,
  ackId: string,
  reply: Uint8Array,
): Promise<void> {
  await runOperation(store, { id: roomId, inc }, STORE_DM_REPLY, { sender, ackId, reply })
}

/** The reply stored for `ackId`, or null if none is. */
async function readDmReply(
  store: RoomStore,
  roomId: string,
  inc: string,
  sender: string,
  ackId: string,
): Promise<RoomResult | null> {
  const key = dmReplyCellKey(sender, ackId)
  const raw = (await readOpenCells(store, roomId, inc, [key])).cells.get(key)
  return raw === undefined ? null : decodeRoomRecord<RoomDmAckEnvelope>(raw)
}

/** Every reply stored for `sender`, by `ackId`, deleted a page at a time as it is read. */
async function takeDmReplies(
  store: RoomStore,
  roomId: string,
  inc: string,
  sender: string,
): Promise<Map<string, RoomResult>> {
  const replies = new Map<string, RoomResult>()
  const slice = new Slice()
  for await (const page of store.reads.readCellPages(roomId, inc, dmReplyCellPrefix(sender))) {
    if (page.kind === 'stale-incarnation') throw roomClosedError(roomId)
    for (const [key, raw] of page.cells)
      replies.set(ackIdOfDmReplyCellKey(key), decodeRoomRecord<RoomDmAckEnvelope>(raw))
    if (!(await deleteCells(store, roomId, inc, page.cells.keys()))) throw roomClosedError(roomId)
    if (slice.over) await slice.next()
  }
  return replies
}

async function dropDmReply(
  store: RoomStore,
  roomId: string,
  inc: string,
  sender: string,
  ackId: string,
): Promise<void> {
  if (!(await deleteCells(store, roomId, inc, [dmReplyCellKey(sender, ackId)]))) throw roomClosedError(roomId)
}
