export { PendingDmAcks }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import type { DmReply } from '../protocol.js'
assertIsNotBrowser()

type PendingDmAck = { from: string; to: string; settle: (reply: DmReply) => void }

/** In-flight `send(…, { ack: true })`s by `ackId`, indexed by both ends too, so a leave settles only the ones it strands. */
class PendingDmAcks {
  private readonly _byId = new Map<string, PendingDmAck>()
  private readonly _byMember = new Map<string, Set<string>>()

  add(ackId: string, pending: PendingDmAck): void {
    this._byId.set(ackId, pending)
    for (const member of [pending.from, pending.to]) {
      let ackIds = this._byMember.get(member)
      if (!ackIds) this._byMember.set(member, (ackIds = new Set()))
      ackIds.add(ackId)
    }
  }

  settle(ackId: string, reply: DmReply): void {
    this.delete(ackId)?.settle(reply)
  }

  delete(ackId: string): PendingDmAck | undefined {
    const pending = this._byId.get(ackId)
    if (!pending) return undefined
    this._byId.delete(ackId)
    for (const member of [pending.from, pending.to]) {
      const ackIds = this._byMember.get(member)
      ackIds?.delete(ackId)
      if (ackIds?.size === 0) this._byMember.delete(member)
    }
    return pending
  }

  /** Settles every wait the member sends or receives. */
  settleMember(member: string, reply: DmReply): void {
    for (const ackId of [...(this._byMember.get(member) ?? [])]) this.settle(ackId, reply)
  }

  settleAll(reply: DmReply): void {
    for (const ackId of [...this._byId.keys()]) this.settle(ackId, reply)
  }
}
