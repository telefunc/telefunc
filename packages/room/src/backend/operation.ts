export { OPERATION_COMMIT_LANE, recordOperation }
export type { OperationReads, OperationWrites }

import type { CellCount, CellMutation, ExpiringCell, LaneId, OperationTx, RoomOperation } from './contract.js'
import { assert } from '../utils/assert.js'

// An operation's plan runs once over what its executor reads; the executor then applies what it recorded at once.

/** The lane an operation's commit lands on. */
const OPERATION_COMMIT_LANE: LaneId = { kind: 'control' }

/** What an executor reads a plan from. */
type OperationReads = Pick<OperationTx, 'now' | 'cell'>

/** What a plan recorded, which its executor applies in this order: the drops, so a plan may write under a prefix it
 *  dropped, the mutations and counts, then the commit. */
type OperationWrites = {
  prefixes: string[]
  members: string[]
  mutations: CellMutation[]
  counts: CellCount[]
  expiring: ExpiringCell[]
  commit: Uint8Array | null
}

function recordOperation<Args, Value>(
  operation: RoomOperation<Args, Value>,
  args: Args,
  reads: OperationReads,
): { value: Value; writes: OperationWrites } {
  const writes: OperationWrites = { prefixes: [], members: [], mutations: [], counts: [], expiring: [], commit: null }
  const tx: OperationTx = {
    now: reads.now,
    cell: (key) => reads.cell(key),
    write: (mutation) => void writes.mutations.push(mutation),
    count: (count) => void writes.counts.push(count),
    dropPrefix: (prefix) => void writes.prefixes.push(prefix),
    dropMemberLanes: (member) => void writes.members.push(member),
    writeExpiring: (cell) => void writes.expiring.push(cell),
    commit(payload) {
      assert(writes.commit === null)
      writes.commit = payload
    },
  }
  return { value: operation.plan(tx, args), writes }
}
