export { GossipOutbox }

import { Fifo } from '../utils/Fifo.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getRoomBackend } from '../backend/plane.js'
import { ROOM_DEMAND_PAGE_ENTRIES, ROOM_GOSSIP_IN_FLIGHT_MAX } from './constants.js'
import type { RoomDemandPage } from './protocol.js'
import { reportRoomError } from './reportRoomError.js'
import { DEMAND_LANE, encodeDemandPage } from './lanes.js'
import type { RoomIncarnation } from './lifecycle.js'
import { Slice } from '../Slice.js'
assertIsNotBrowser()

type Want = RoomDemandPage['wants'][number]

/** Commits a room's demand on its demand lane in order, the entries told in one turn sharing pages, and
 *  `ROOM_GOSSIP_IN_FLIGHT_MAX` pages at a time and a slice per turn, so a burst waits as entries rather than as
 *  commits, and its deliveries leave the event loop turning. Its pages are counted, so a reader sees one it missed. */
class GossipOutbox {
  private waiting = new Fifo<Want>()
  private pages = 0
  /** A page is due even with no entry waiting: each page renews this process's demand elsewhere. */
  private renewing = false
  private inFlight = 0
  private closed = false
  private scheduled = false
  /** While a burst is sent; `yielding` while it waits for the next turn. */
  private slice: Slice | null = null
  private yielding = false

  constructor(
    private readonly room: RoomIncarnation,
    private readonly instance: string,
  ) {}

  push(want: Want): void {
    if (this.closed) return
    this.waiting.push(want)
    this.schedule()
  }

  renew(): void {
    if (this.closed) return
    this.renewing = true
    this.schedule()
  }

  private schedule(): void {
    this.slice ??= new Slice()
    if (this.scheduled) return
    this.scheduled = true
    queueMicrotask(() => {
      this.scheduled = false
      this.send()
    })
  }

  /** A closed room's gossip has no reader: what waits is dropped, and nothing more is told. */
  close(): void {
    this.closed = true
    this.waiting = new Fifo()
  }

  private send(): void {
    const slice = this.slice
    if (slice === null || this.yielding) return
    while (this.inFlight < ROOM_GOSSIP_IN_FLIGHT_MAX && (this.waiting.length > 0 || this.renewing)) {
      if (slice.over) {
        this.yielding = true
        void slice.next().then(() => {
          this.yielding = false
          this.send()
        })
        return
      }
      this.renewing = false
      const wants: Want[] = []
      while (wants.length < ROOM_DEMAND_PAGE_ENTRIES && this.waiting.length > 0) wants.push(this.waiting.shift()!)
      const page: RoomDemandPage = { __r: 'demand', instance: this.instance, page: ++this.pages, wants }
      this.inFlight++
      // A stale commit is a closed room's: its gossip has no reader.
      void getRoomBackend()
        .commitLane({ roomId: this.room.id, inc: this.room.inc, lane: DEMAND_LANE }, encodeDemandPage(page))
        .catch(reportRoomError)
        .finally(() => {
          this.inFlight--
          this.send()
        })
    }
    if (this.inFlight === 0) this.slice = null
  }
}
