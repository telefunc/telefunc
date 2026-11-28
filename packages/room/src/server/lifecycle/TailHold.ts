export { TailHold }
export type { TailEntry, TailHoldOwner }

import { unrefTimer } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { ROOM_TAIL_ATTACH_TIMEOUT_MS, ROOM_TAIL_HOLD_CODE_UNITS_MAX, ROOM_TAIL_HOLD_MAX } from '../constants.js'
import { reportDeliveryLost } from '../config.js'
import type { OrderingInfo } from 'telefunc/__internal'
assertIsNotBrowser()

type TailEntry = { serialized: string; ord: OrderingInfo; from: string }

/** The room or stub whose tail it holds. */
interface TailHoldOwner {
  /** The room's id, which a report of what the tail dropped names. */
  readonly roomId: string
  /** The lease expired before the holder subscribed. */
  tailExpired(): void
}

type TailHoldOptions = {
  owner: TailHoldOwner
  seed?: TailEntry[]
}

/** A bounded suffix of the recent text, held drop-oldest under both caps until its holder subscribes, or until its
 *  lease expires; what it drops is reported, once per hold, as a delivery Room lost. */
class TailHold {
  private readonly owner: TailHoldOwner
  private readonly entries: TailEntry[]
  private codeUnits = 0
  private dropReported = false
  private readonly lease: ReturnType<typeof setTimeout>

  constructor({ owner, seed = [] }: TailHoldOptions) {
    this.owner = owner
    this.entries = seed
    for (const entry of seed) this.codeUnits += entry.serialized.length
    this.lease = unrefTimer(
      setTimeout(() => {
        const held = this.entries.length
        if (held > 0) {
          this.reportDrop(
            `its tail dropped ${held === 1 ? 'the message' : `the ${held} messages`} it held, as no page subscribed ` +
              `within ${ROOM_TAIL_ATTACH_TIMEOUT_MS / 1000} s`,
          )
        }
        owner.tailExpired()
      }, ROOM_TAIL_ATTACH_TIMEOUT_MS),
    )
  }

  push(entry: TailEntry): void {
    if (entry.serialized.length > ROOM_TAIL_HOLD_CODE_UNITS_MAX) {
      this.reportDrop(
        `its tail dropped a message of more than ${ROOM_TAIL_HOLD_CODE_UNITS_MAX} UTF-16 code units, its cap`,
      )
      return
    }
    this.entries.push(entry)
    this.codeUnits += entry.serialized.length
    while (this.codeUnits > ROOM_TAIL_HOLD_CODE_UNITS_MAX || this.entries.length > ROOM_TAIL_HOLD_MAX) {
      const dropped = this.entries.shift()
      if (dropped === undefined) break
      this.codeUnits -= dropped.serialized.length
      this.reportDrop(
        `its tail dropped its oldest message, to hold at most the last ${ROOM_TAIL_HOLD_MAX} messages and ` +
          `${ROOM_TAIL_HOLD_CODE_UNITS_MAX} UTF-16 code units of them until a page subscribes`,
      )
    }
  }

  private reportDrop(reason: string): void {
    if (this.dropReported) return
    this.dropReported = true
    reportDeliveryLost(this.owner.roomId, reason)
  }

  /** Ends the lease and hands over the held entries, oldest first. */
  take(): TailEntry[] {
    this.end()
    return this.entries
  }

  end(): void {
    clearTimeout(this.lease)
  }
}
