export { TailHold }
export type { TailEntry }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { ROOM_TAIL_ATTACH_TIMEOUT_MS, ROOM_TAIL_HOLD_CODE_UNITS_MAX, ROOM_TAIL_HOLD_MAX } from '../constants.js'
import type { OrderingInfo } from 'telefunc/__internal'
assertIsNotBrowser()

type TailEntry = { serialized: string; ord: OrderingInfo; from: string }

/** Recent text held drop-oldest under both caps until its holder subscribes, or until its lease expires. */
class TailHold {
  private readonly entries: TailEntry[]
  private codeUnits = 0
  private readonly lease: ReturnType<typeof setTimeout>

  constructor(onExpire: () => void, seed: TailEntry[] = []) {
    this.entries = seed
    for (const entry of seed) this.codeUnits += entry.serialized.length
    this.lease = unrefTimer(setTimeout(onExpire, ROOM_TAIL_ATTACH_TIMEOUT_MS))
  }

  /** An entry larger than the whole budget is dropped, never held: the tail is best-effort. */
  push(entry: TailEntry): void {
    if (entry.serialized.length > ROOM_TAIL_HOLD_CODE_UNITS_MAX) return
    this.entries.push(entry)
    this.codeUnits += entry.serialized.length
    while (this.entries.length > ROOM_TAIL_HOLD_MAX || this.codeUnits > ROOM_TAIL_HOLD_CODE_UNITS_MAX) {
      this.codeUnits -= this.entries.shift()!.serialized.length
    }
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
