export { RoomStore }
export type { RoomReads }

import { assertDriverOrdering, type BackendCall, type OrderingInfo } from 'telefunc/__internal'
import type {
  CellsExchange,
  CellsExchangeResult,
  CommitOptions,
  CommitResult,
  CommitTarget,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  OperationOptions,
  OperationResult,
  RetainedFrame,
  RoomDriver,
  RoomOperation,
} from './contract.js'
import { assert } from '../utils/assert.js'

/** The driver's reads, as they are, with the orderings it reports checked. A read counts for nothing in a call's
 *  account: its failure is told at the call's close. */
type RoomReads = Pick<
  RoomDriver,
  | 'readHead'
  | 'readCells'
  | 'readExpiringCells'
  | 'readCellPages'
  | 'listRetained'
  | 'directoryGet'
  | 'directoryList'
  | 'directoryDue'
> & {
  readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null>
  readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null>
}

/** The store as one call sees it: the driver's writes, each counted in the call's account as it is handed over, with
 *  the head writes and the orderings checked; and its reads. Work that tells no answer has the store with no account
 *  (`RoomPlane.direct`), whose writes go straight through. A helper takes the store it is given: it can't tell which
 *  it has. */
class RoomStore {
  readonly reads: RoomReads

  constructor(
    private readonly driver: RoomDriver,
    reads: RoomReads,
    private readonly account: BackendCall | null,
  ) {
    this.reads = reads
  }

  compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    assertHeadNextWellFormed(next)
    return this.write(() => this.driver.compareExchangeHead(roomId, expected, next))
  }

  compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    return this.write(() => this.driver.compareExchangeCells(roomId, inc, exchange))
  }

  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    return this.write(async () => {
      const result = await this.driver.commitLane(target, payload, opts)
      if (result.kind === 'accepted') assertDriverOrdering(result)
      return result
    })
  }

  runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    return this.write(async () => {
      const result = await this.driver.runOperation(roomId, inc, operation, args, options)
      if (result.kind === 'done' && result.commit !== undefined) assertDriverOrdering(result.commit)
      return result
    })
  }

  dropIncarnation(roomId: string, inc: string): Promise<void> {
    return this.write(() => this.driver.dropIncarnation(roomId, inc))
  }

  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.write(() => this.driver.directoryPut(roomId, incTag, checkAt, minDelayMs))
  }

  directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.write(() => this.driver.directoryDelete(roomId, incTag))
  }

  private write<T>(step: () => Promise<T>): Promise<T> {
    return this.account === null ? step() : this.account.write(step)
  }
}

/** Every head write is checked here; drivers only compare-exchange. */
function assertHeadNextWellFormed(next: HeadNext): void {
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    assert(head.closeLease !== undefined, 'head exchange: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    assert(
      Number.isFinite(durationMs) && durationMs > 0,
      `head exchange: close lease durationMs ${durationMs} must be finite and positive`,
    )
  } else {
    assert(head.closeLease === undefined, `head exchange: a '${head.state}' head must not carry a close lease`)
  }
  assert(
    ttlMs === undefined || head.state !== 'closing',
    'head exchange: a closing head lapses with its close lease, not a ttl',
  )
  assert(
    head.state !== 'closed' || head.currentInc === null,
    'head exchange: a closed tombstone must clear currentInc to null',
  )
  assert(
    head.state === 'closed' || head.currentInc !== null,
    `head exchange: a '${head.state}' head must name an incarnation`,
  )
}
