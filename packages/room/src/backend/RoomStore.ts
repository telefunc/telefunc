export { RoomStore, CallStore }
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

/** The store as work that tells no answer sees it (`RoomPlane.direct`): the driver's writes, with the head writes and
 *  the orderings checked, and its reads. A call sees a `CallStore`, which counts each write in its account. A helper
 *  takes the store it is given: it can't tell which it has. */
class RoomStore {
  readonly reads: RoomReads

  constructor(
    protected readonly driver: RoomDriver,
    reads: RoomReads,
  ) {
    this.reads = reads
  }

  compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    assertHeadNextWellFormed(next)
    return this.driver.compareExchangeHead(roomId, expected, next)
  }

  compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    return this.driver.compareExchangeCells(roomId, inc, exchange)
  }

  async commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    const result = await this.driver.commitLane(target, payload, opts)
    if (result.kind === 'accepted') assertDriverOrdering(result)
    return result
  }

  async runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    const result = await this.driver.runOperation(roomId, inc, operation, args, options)
    if (result.kind === 'done' && result.commit !== undefined) assertDriverOrdering(result.commit)
    return result
  }

  dropIncarnation(roomId: string, inc: string): Promise<void> {
    return this.driver.dropIncarnation(roomId, inc)
  }

  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.driver.directoryPut(roomId, incTag, checkAt, minDelayMs)
  }

  directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.driver.directoryDelete(roomId, incTag)
  }

  /** The value of `work`, which runs on the store `shared` and may be shared with other calls, once it settled. */
  join<T>(_shared: RoomStore, work: Promise<T>): Promise<T> {
    return work
  }
}

/** The store as one call sees it: each write is counted in the call's account as it is handed over. */
class CallStore extends RoomStore {
  constructor(
    driver: RoomDriver,
    reads: RoomReads,
    private readonly account: BackendCall,
  ) {
    super(driver, reads)
  }

  override compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    assertHeadNextWellFormed(next)
    return this.account.write(() => this.driver.compareExchangeHead(roomId, expected, next))
  }

  override compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    return this.account.write(() => super.compareExchangeCells(roomId, inc, exchange))
  }

  override commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    return this.account.write(() => super.commitLane(target, payload, opts))
  }

  override runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    return this.account.write(() => super.runOperation(roomId, inc, operation, args, options))
  }

  override dropIncarnation(roomId: string, inc: string): Promise<void> {
    return this.account.write(() => super.dropIncarnation(roomId, inc))
  }

  override directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.account.write(() => super.directoryPut(roomId, incTag, checkAt, minDelayMs))
  }

  override directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.account.write(() => super.directoryDelete(roomId, incTag))
  }

  /** A write of `shared`'s call counts as this call's, and this call's close waits for `work`. */
  override join<T>(shared: RoomStore, work: Promise<T>): Promise<T> {
    return shared instanceof CallStore ? this.account.join(shared.account, work) : work
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
