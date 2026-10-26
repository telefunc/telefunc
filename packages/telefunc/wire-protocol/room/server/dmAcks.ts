export { PendingDmAcks }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import type { RoomResult } from '../protocol.js'
import { toggleEntry } from './sets.js'
assertIsNotBrowser()

type PendingDmAck = { from: string; to: string; settle: (reply: RoomResult) => void }

/** In-flight `send(…, { ack: true })`s by `ackId`, indexed by both ends too, so a leave settles only
 *  the ones it strands. */
class PendingDmAcks {
  private readonly _byId = new Map<string, PendingDmAck>()
  private readonly _byMember = new Map<string, Set<string>>()

  add(ackId: string, pending: PendingDmAck): void {
    this._byId.set(ackId, pending)
    for (const member of [pending.from, pending.to]) toggleEntry(this._byMember, member, ackId, true)
  }

  settle(ackId: string, reply: RoomResult): void {
    this.delete(ackId)?.settle(reply)
  }

  delete(ackId: string): PendingDmAck | undefined {
    const pending = this._byId.get(ackId)
    if (!pending) return undefined
    this._byId.delete(ackId)
    for (const member of [pending.from, pending.to]) toggleEntry(this._byMember, member, ackId, false)
    return pending
  }

  /** Whether the member sends or receives a wait. */
  waitsOn(member: string): boolean {
    return this._byMember.has(member)
  }

  /** Settles every wait the member sends or receives. */
  settleMember(member: string, reply: RoomResult): void {
    for (const ackId of this._byMember.get(member) ?? []) this.settle(ackId, reply)
  }

  settleAll(reply: RoomResult): void {
    for (const ackId of this._byId.keys()) this.settle(ackId, reply)
  }
}
