export { GossipOutbox }

import { Fifo } from '../../../utils/Fifo.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../../backend/install.js'
import type { RoomBackend } from '../../backend/room/contract.js'
import { ROOM_GOSSIP_IN_FLIGHT_MAX } from '../constants.js'
import type { RoomCtrlEnvelope } from '../protocol.js'
import { reportRoomError } from './errors.js'
import { CONTROL_LANE, commitRoomLane, encodeRoomRecord } from './lanes.js'
import { Slice } from '../slice.js'
assertIsNotBrowser()

/** Commits a room's demand gossip in order, `ROOM_GOSSIP_IN_FLIGHT_MAX` at a time and a slice per turn, so a burst
 *  waits as events rather than as commits, and its deliveries leave the event loop turning. Each goes to the backend
 *  installed when it was told. */
class GossipOutbox {
  private _waiting = new Fifo<{ backend: RoomBackend; event: RoomCtrlEnvelope }>()
  private _inFlight = 0
  private _closed = false
  /** While a burst is sent; `_yielding` while it waits for the next turn. */
  private _slice: Slice | null = null
  private _yielding = false

  constructor(
    private readonly _roomId: string,
    private readonly _inc: string,
  ) {}

  push(event: RoomCtrlEnvelope): void {
    if (this._closed) return
    this._waiting.push({ backend: getRoomBackend(), event })
    this._slice ??= new Slice()
    this._send()
  }

  /** A closed room's gossip has no reader: what waits is dropped, and nothing more is told. */
  close(): void {
    this._closed = true
    this._waiting = new Fifo()
  }

  private _send(): void {
    const slice = this._slice
    if (slice === null || this._yielding) return
    for (; this._inFlight < ROOM_GOSSIP_IN_FLIGHT_MAX && this._waiting.length > 0; this._inFlight++) {
      if (slice.over) {
        this._yielding = true
        void slice.next().then(() => {
          this._yielding = false
          this._send()
        })
        return
      }
      const { backend, event } = this._waiting.shift()!
      // A stale commit is a closed room's: its gossip has no reader.
      void commitRoomLane(this._roomId, this._inc, CONTROL_LANE, encodeRoomRecord(event), undefined, backend)
        .catch(reportRoomError)
        .finally(() => {
          this._inFlight--
          this._send()
        })
    }
    if (this._inFlight === 0) this._slice = null
  }
}
