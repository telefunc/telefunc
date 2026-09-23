export { TailHold }
export type { TailEntry }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { ROOM_TAIL_ATTACH_TIMEOUT_MS, ROOM_TAIL_HOLD_CODE_UNITS_MAX, ROOM_TAIL_HOLD_MAX } from '../constants.js'
import type { RoomOrder } from '../protocol.js'
assertIsNotBrowser()

type TailEntry = { serialized: string; ord: RoomOrder; from: string }

/** Recent text held drop-oldest under both caps until its holder subscribes, or until its lease expires. */
class TailHold {
  private readonly _entries: TailEntry[]
  private _codeUnits = 0
  private readonly _lease: ReturnType<typeof setTimeout>

  constructor(onExpire: () => void, seed: TailEntry[] = []) {
    this._entries = seed
    for (const entry of seed) this._codeUnits += entry.serialized.length
    this._lease = unrefTimer(setTimeout(onExpire, ROOM_TAIL_ATTACH_TIMEOUT_MS))
  }

  /** An entry larger than the whole budget is dropped, never held: the tail is best-effort. */
  push(entry: TailEntry): void {
    if (entry.serialized.length > ROOM_TAIL_HOLD_CODE_UNITS_MAX) return
    this._entries.push(entry)
    this._codeUnits += entry.serialized.length
    while (this._entries.length > ROOM_TAIL_HOLD_MAX || this._codeUnits > ROOM_TAIL_HOLD_CODE_UNITS_MAX)
      this._codeUnits -= this._entries.shift()!.serialized.length
  }

  /** Ends the lease and hands over the held entries, oldest first. */
  take(): TailEntry[] {
    this.end()
    return this._entries
  }

  end(): void {
    clearTimeout(this._lease)
  }
}
