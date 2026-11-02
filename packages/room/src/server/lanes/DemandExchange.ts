export { DemandExchange }
export type { DemandExchangeOptions }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomCtrlEnvelope } from '../../shared/protocol.js'
import { Slice } from '../../shared/Slice.js'
import type { DemandEntry } from '../protocol.js'
import { reportRoomError } from '../reportRoomError.js'
import { GossipOutbox } from '../GossipOutbox.js'
import type { RoomIncarnation } from '../lifecycle/hold.js'
import { DemandReport, type DemandReportOwner } from './DemandReport.js'
import type { RoomDemand } from './RoomDemand.js'
import { commitLane, controlLane, decodeDemandPage, encodeRoomRecord } from './lanes.js'
assertIsNotBrowser()

type DemandAsk = Extract<RoomCtrlEnvelope, { __r: 'demand-ask' }>

type DemandExchangeOptions = {
  incarnation: RoomIncarnation
  /** What the other processes tell of the members this one owns, which this process's own report reaches too. */
  incomingDemand: RoomDemand
}

/** A room's demand between processes: this process's report goes out as numbered pages on the demand lane, the other
 *  processes' pages come in, and a process that missed a page asks for everything again on the control lane. */
class DemandExchange implements DemandReportOwner {
  /** What every holder here receives, told to the other processes and to `incomingDemand`. */
  readonly outgoingDemand = new DemandReport({ owner: this })
  private readonly incarnation: RoomIncarnation
  private readonly incomingDemand: RoomDemand
  private readonly gossip: GossipOutbox
  /** Whether this process tells its demand again, and was asked to again meanwhile. */
  private tellingAgain = false
  private askedAgain = false

  constructor({ incarnation, incomingDemand }: DemandExchangeOptions) {
    this.incarnation = incarnation
    this.incomingDemand = incomingDemand
    this.gossip = new GossipOutbox({ room: incarnation, instance: this.outgoingDemand.instance })
  }

  /** `DemandReportOwner`: this process's demand entry, to the other processes and to its own owners. */
  tellDemand(entry: DemandEntry, on: boolean): void {
    this.gossip.push({ on, ...entry })
    this.incomingDemand.take(this.outgoingDemand.instance, entry, on)
  }

  /** Renews this process's demand elsewhere with one page, and drops the reporters that lapsed, a slice at a time. */
  async renew(): Promise<void> {
    if (this.outgoingDemand.reporting) this.gossip.renew()
    const slice = new Slice()
    for (const _ of this.incomingDemand.expire()) if (slice.over) await slice.next()
  }

  /** Another process's page; one that shows a gap asks that process for everything again. */
  onPage(serialized: string): void {
    const page = decodeDemandPage(serialized)
    if (page.instance === this.outgoingDemand.instance) return
    if (this.incomingDemand.applyPage(page)) this.ask(page.instance)
  }

  /** Another process asks this one, or every one, to tell its demand again. */
  onAsk({ instance, of }: DemandAsk): void {
    const self = this.outgoingDemand.instance
    if (instance !== self && (of ?? self) === self) void this.tellAgain().catch(reportRoomError)
  }

  /** Asks `of`, or every other process, to tell its demand again. */
  ask(of?: string): void {
    const ask: DemandAsk = { __r: 'demand-ask', instance: this.outgoingDemand.instance }
    if (of) ask.of = of
    // A stale commit is a closed room's, which has no demand.
    void commitLane(controlLane(this.incarnation.id, this.incarnation.inc), encodeRoomRecord(ask)).catch(
      reportRoomError,
    )
  }

  /** The demand lane stopped: what the other processes told lapses unread. */
  forget(): void {
    this.incomingDemand.forgetLeased()
  }

  /** A closed room's demand has no reader. */
  close(): void {
    this.gossip.close()
  }

  /** Tells every entry of this process's demand again as all it receives, a slice at a time; once more after it when
   *  asked again meanwhile, as an asker may not have read what it told before the ask. */
  private async tellAgain(): Promise<void> {
    if (this.tellingAgain) {
      this.askedAgain = true
      return
    }
    this.tellingAgain = true
    try {
      do {
        this.askedAgain = false
        if (this.outgoingDemand.reporting) await this.tellAll()
      } while (this.askedAgain)
    } finally {
      this.tellingAgain = false
    }
  }

  private async tellAll(): Promise<void> {
    this.gossip.push({ again: true })
    const slice = new Slice()
    for (const _ of this.outgoingDemand.all()) if (slice.over) await slice.next()
    this.gossip.push({ again: false })
  }
}
