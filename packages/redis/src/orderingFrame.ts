export { decodeLaneFrame, decodeRetainedFrame }

import type { OrderingInfo } from 'telefunc/__internal'
import { isOrdering } from 'telefunc/__internal'
import { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH } from './commands.js'
import type { LaneFrame, LaneRoute } from './LaneHistory.js'
import { NO_ID } from './orderRecord.js'

// The ordering frame, as commands.ts's Lua writes it: `[seq_hi][seq_lo][ts_hi][ts_lo]`, u32 big-endian words, then the
// commit's id and its predecessor's on its lane, 8 bytes each, then the payload.
const ORDERING_FRAME_HEADER_BYTES = 32
const WORD_RANGE = 0x1_0000_0000

/**
 * A frame as a channel delivers it: its commit's fence token after the fence byte, if armed; its lane key, u16
 * big-endian length first, on a fan-in channel; its place, u64 big-endian, where the source's kinds share one seq;
 * then the ordering frame. Null where it isn't in that form: any Redis client can publish to a channel.
 */
function decodeLaneFrame(
  message: Buffer,
  { fanIn, placed }: { fanIn: boolean; placed: boolean },
  via: LaneRoute,
): LaneFrame | null {
  const fenced = message[0] === REDIS_DELIVERY_FENCE_BYTE
  let at = fenced ? 1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH : 0
  const token = fenced ? message.toString('latin1', 1, at) : null
  let lane = ''
  if (fanIn) {
    if (message.byteLength < at + 2) return null
    const end = at + 2 + message.readUInt16BE(at)
    if (end > message.byteLength) return null
    lane = message.toString('latin1', at + 2, end)
    at = end
  }
  let place = 0
  if (placed) {
    if (message.byteLength < at + 8) return null
    place = Number(message.readBigUInt64BE(at))
    at += 8
  }
  const info = orderingAt(message, at)
  if (info === null) return null
  const id = message.readBigUInt64BE(at + 16)
  if (!placed) place = info.seq
  if (id === NO_ID || !Number.isSafeInteger(place) || place < 1) return null
  return {
    lane,
    place,
    id,
    prev: message.readBigUInt64BE(at + 24),
    payload: message.subarray(at + ORDERING_FRAME_HEADER_BYTES),
    info,
    token,
    via,
  }
}

/** A retained frame, the ordering frame a lane's last retained commit wrote, or null where it isn't one. */
function decodeRetainedFrame(frame: Buffer): { payload: Uint8Array; info: OrderingInfo } | null {
  const info = orderingAt(frame, 0)
  if (info === null || frame.readBigUInt64BE(16) === NO_ID) return null
  return { payload: frame.subarray(ORDERING_FRAME_HEADER_BYTES), info }
}

/** The ordering of the ordering frame at `at`, or null where the frame is too short for one, or its ordering isn't one
 *  Telefunc's scripts write. */
function orderingAt(message: Buffer, at: number): OrderingInfo | null {
  if (message.byteLength < at + ORDERING_FRAME_HEADER_BYTES) return null
  const info = {
    seq: message.readUInt32BE(at) * WORD_RANGE + message.readUInt32BE(at + 4),
    timestamp: message.readUInt32BE(at + 8) * WORD_RANGE + message.readUInt32BE(at + 12),
  }
  return isOrdering(info) ? info : null
}
