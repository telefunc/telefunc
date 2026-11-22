// A leave event's length depends only on its cause and on `hidden`, as member ids have one length: so a removal of
// several tabs checks one record per visibility before its first eviction. The records are the ones LEAVE's plan
// commits.
import { expect, it } from 'vitest'
import type { OperationTx } from '../../backend/contract.js'
import { encodeRoomRecord } from '../lanes/lanes.js'
import { memberCellKey } from './cells.js'
import { LEAVE } from './members.js'

/** The leave event LEAVE's plan commits for member `id`, from a record with `hidden`. */
function committedLeave(id: string, hidden: boolean, reason: string): Uint8Array | undefined {
  const record = encodeRoomRecord({ joinedAt: 1, session: 's', metaSeq: 0, metaGen: 0, hidden })
  const committed: { payload?: Uint8Array } = {}
  const tx: OperationTx = {
    now: 1,
    cell: (key) => (key === memberCellKey(id) ? record : undefined),
    write() {},
    count() {},
    dropPrefix() {},
    dropMemberLanes() {},
    commit(payload) {
      committed.payload = payload
    },
    writeExpiring() {},
  }
  LEAVE.plan(tx, { id, cause: { cause: 'removed', reason }, hold: null })
  return committed.payload
}

it('commits leave events of one length for two ids of one visibility, and 14 bytes more for a hidden one', () => {
  const [a, b] = [crypto.randomUUID(), crypto.randomUUID()]
  const reason = 'a reason'
  const length = (id: string, hidden: boolean) => committedLeave(id, hidden, reason)?.byteLength

  expect({
    visible: length(a, false) === length(b, false),
    hidden: length(a, true) === length(b, true),
    hiddenMore: (length(a, true) ?? 0) - (length(a, false) ?? 0),
  }).toEqual({ visible: true, hidden: true, hiddenMore: 14 })
})
