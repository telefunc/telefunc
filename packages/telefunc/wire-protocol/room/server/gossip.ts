export { GossipOutbox }

import { Fifo } from '../../../utils/Fifo.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../../backend/install.js'
import { ROOM_DEMAND_PAGE_ENTRIES, ROOM_GOSSIP_IN_FLIGHT_MAX } from './constants.js'
import type { RoomDemandPage } from './protocol.js'
import { reportRoomError } from './errors.js'
import { DEMAND_LANE, encodeDemandPage } from './lanes.js'
import { Slice } from '../slice.js'
assertIsNotBrowser()

type Want = RoomDemandPage['wants'][number]

/** Commits a room's demand on its demand lane in order, the entries told in one turn sharing pages, and
 *  `ROOM_GOSSIP_IN_FLIGHT_MAX` pages at a time and a slice per turn, so a burst waits as entries rather than as commits,
 *  and its deliveries leave the event loop turning. Its pages are counted, so a reader sees one it missed. */
class GossipOutbox {
  private _waiting = new Fifo<Want>()
  private _pages = 0
  /** A page is due even with no entry waiting: each page renews this process's demand elsewhere. */
  private _renewing = false
  private _inFlight = 0
  private _closed = false
  private _scheduled = false
  /** While a burst is sent; `_yielding` while it waits for the next turn. */
  private _slice: Slice | null = null
  private _yielding = false

  constructor(
    private readonly _roomId: string,
    private readonly _inc: string,
    private readonly _instance: string,
  ) {}

  push(want: Want): void {
    if (this._closed) return
    this._waiting.push(want)
    this._schedule()
  }

  renew(): void {
    if (this._closed) return
    this._renewing = true
    this._schedule()
  }

  private _schedule(): void {
    this._slice ??= new Slice()
    if (this._scheduled) return
    this._scheduled = true
    queueMicrotask(() => {
      this._scheduled = false
      this._send()
    })
  }

  /** A closed room's gossip has no reader: what waits is dropped, and nothing more is told. */
  close(): void {
    this._closed = true
    this._waiting = new Fifo()
  }

  private _send(): void {
    const slice = this._slice
    if (slice === null || this._yielding) return
    for (
      ;
      this._inFlight < ROOM_GOSSIP_IN_FLIGHT_MAX && (this._waiting.length > 0 || this._renewing);
      this._inFlight++
    ) {
      if (slice.over) {
        this._yielding = true
        void slice.next().then(() => {
          this._yielding = false
          this._send()
        })
        return
      }
      this._renewing = false
      const wants: Want[] = []
      while (wants.length < ROOM_DEMAND_PAGE_ENTRIES && this._waiting.length > 0) wants.push(this._waiting.shift()!)
      const page: RoomDemandPage = { __r: 'demand', instance: this._instance, page: ++this._pages, wants }
      // A stale commit is a closed room's: its gossip has no reader.
      void getRoomBackend()
        .commitLane(this._roomId, this._inc, DEMAND_LANE, encodeDemandPage(page))
        .catch(reportRoomError)
        .finally(() => {
          this._inFlight--
          this._send()
        })
    }
    if (this._inFlight === 0) this._slice = null
  }
}
