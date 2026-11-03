export { storeDmReply, takeDmReply, takeDmReplies, dropDmReply }

import { getRoomPlane } from '../../backend/plane.js'
import { Slice } from '../../shared/Slice.js'
import { roomClosedError } from '../../shared/errors.js'
import type { RoomResult } from '../../shared/protocol.js'
import { deleteCells, mutateCells, readOpenCells } from './cellStore.js'
import { decodeRoomRecord, encodeRoomRecord } from '../lanes/lanes.js'
import { ackIdOfDmReplyCellKey, dmReplyCellKey, dmReplyCellPrefix, memberCellKey } from './cells.js'

// A DM's reply, stored for its sender to take until the sender leaves.

/** Kept only while its sender is a member, whose leave drops what it never settled. */
async function storeDmReply(
  roomId: string,
  inc: string,
  sender: string,
  ackId: string,
  reply: RoomResult,
): Promise<void> {
  const member = memberCellKey(sender)
  await mutateCells(roomId, inc, [member], (cells) => ({
    value: undefined,
    mutations: cells.has(member) ? [{ key: dmReplyCellKey(sender, ackId), bytes: encodeRoomRecord(reply) }] : [],
  }))
}

/** The reply stored for `ackId`, deleted as it is read, or null if none is. */
async function takeDmReply(roomId: string, inc: string, sender: string, ackId: string): Promise<RoomResult | null> {
  const key = dmReplyCellKey(sender, ackId)
  const raw = (await readOpenCells(roomId, inc, [key])).cells.get(key)
  if (raw === undefined) return null
  await dropDmReply(roomId, inc, sender, ackId)
  return decodeRoomRecord<RoomResult>(raw)
}

/** Every reply stored for `sender`, by `ackId`, deleted a page at a time as it is read. */
async function takeDmReplies(roomId: string, inc: string, sender: string): Promise<Map<string, RoomResult>> {
  const plane = getRoomPlane()
  const replies = new Map<string, RoomResult>()
  const slice = new Slice()
  for await (const page of plane.driver.readCellPages(roomId, inc, dmReplyCellPrefix(sender))) {
    if (page.kind === 'stale-incarnation') throw roomClosedError(roomId)
    for (const [key, raw] of page.cells) replies.set(ackIdOfDmReplyCellKey(key), decodeRoomRecord<RoomResult>(raw))
    if (!(await deleteCells(roomId, inc, page.cells.keys()))) throw roomClosedError(roomId)
    if (slice.over) await slice.next()
  }
  return replies
}

async function dropDmReply(roomId: string, inc: string, sender: string, ackId: string): Promise<void> {
  if (!(await deleteCells(roomId, inc, [dmReplyCellKey(sender, ackId)]))) throw roomClosedError(roomId)
}
