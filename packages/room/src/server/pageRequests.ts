export { PendingDmReplies, roomResult, receiveFromPage, assertPublishShield }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import { isProtocolViolationError, type ServerBroadcastEnd } from 'telefunc/__internal'
import { ShieldValidationError, type ShieldValidator } from 'telefunc'
import { reportRoomError } from './reportRoomError.js'
import { ROOM_BUG_MESSAGE, toRoomFailure } from '../shared/errors.js'
import type { RoomFailure, RoomResult } from '../shared/protocol.js'
assertIsNotBrowser()

/** A stub's ack DMs awaiting their `dm-reply`: null at the next `await()` once its sender timed out, or on `drop()`. */
class PendingDmReplies {
  private readonly pending = new Map<string, { expiresAt: number; resolve: (reply: RoomResult | null) => void }>()

  /** Drops the replies whose sender already timed out. */
  await(ackId: string): Promise<RoomResult | null> {
    const now = performance.now()
    for (const [id, entry] of this.pending) {
      if (entry.expiresAt > now) break // constant offset ⇒ insertion order is deadline order; the rest are younger
      this.pending.delete(id)
      entry.resolve(null)
    }
    return new Promise((resolve) => this.pending.set(ackId, { expiresAt: now + ROOM_DM_ACK_TIMEOUT_MS, resolve }))
  }

  settle(ackId: string, reply: RoomResult): void {
    const entry = this.pending.get(ackId)
    // The sender drops a reply after its timeout.
    if (!entry) return
    this.pending.delete(ackId)
    entry.resolve(reply)
  }

  drop(): void {
    for (const entry of this.pending.values()) entry.resolve(null)
    this.pending.clear()
  }
}

async function roomResult(work: Promise<unknown>): Promise<RoomResult> {
  try {
    return { ok: true, value: await work }
  } catch (err) {
    return toRoomFailure(err, reportRoomError, ROOM_BUG_MESSAGE)
  }
}

/** Runs a stub's handling of what its page sent: a malformed message ends the page's wire and fails its request. */
function receiveFromPage<T>(end: ServerBroadcastEnd, handle: () => T): T | Promise<RoomFailure> {
  try {
    return handle()
  } catch (err) {
    if (!isProtocolViolationError(err)) throw err
    end.terminateWire()
    return Promise.resolve({ ok: false, kind: 'error', message: err.message })
  }
}

/** The publish shield validates a page's Room data at ingress only. */
function assertPublishShield(validate: ShieldValidator | undefined, data: unknown): void {
  if (!validate) return
  const result = validate(data)
  if (result !== true) throw new ShieldValidationError(result)
}
