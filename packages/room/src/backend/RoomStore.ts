export { RoomStore, CallStore }
export type { RoomReads }

import { assertDriverOrdering, type BackendCall } from 'telefunc/__internal'
import type {
  CellsExchange,
  ControlRecord,
  CellsExchangeResult,
  CommitOptions,
  CommitResult,
  CommitTarget,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  CommitKept,
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
  readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<{ seq: number } | null>
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

  compareExchangeCells(
    roomId: string,
    inc: string,
    exchange: CellsExchange,
    record?: ControlRecord,
  ): Promise<CellsExchangeResult> {
    assertCellsExchangeWellFormed(exchange, record)
    return this.driver.compareExchangeCells(roomId, inc, exchange, record)
  }

  async commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    const result = await this.driver.commitLane(target, payload, opts)
    if (result.kind === 'accepted') assertDriverOrdering(result)
    return result
  }

  /** The commit's size rule, asked before the call writes anything else: it writes nothing, so it counts for nothing. */
  assertCommittable(lane: LaneId, payload: Uint8Array, kept: CommitKept): void {
    this.driver.assertCommittable(lane, payload, kept)
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

  override compareExchangeCells(
    roomId: string,
    inc: string,
    exchange: CellsExchange,
    record?: ControlRecord,
  ): Promise<CellsExchangeResult> {
    assertCellsExchangeWellFormed(exchange, record)
    return this.account.write(() => this.driver.compareExchangeCells(roomId, inc, exchange, record))
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

/** Every cells exchange is checked here: a counted cell is counted once and isn't also written, which a driver's check
 *  of each count before the exchange's first write rests on, and a control record is numbered by one of its counts. */
function assertCellsExchangeWellFormed(exchange: CellsExchange, record: ControlRecord | undefined): void {
  const malformed = malformedCount(exchange) ?? (record === undefined ? null : malformedRecord(exchange, record))
  assert(malformed === null, malformed)
}

/** What makes the exchange's counts ones no caller makes, or null. */
function malformedCount({ mutations, counts = [] }: CellsExchange): string | null {
  for (const [i, { key }] of counts.entries()) {
    if (counts.findIndex((other) => other.key === key) < i) return `cells exchange: ${key} is counted twice`
    if (mutations.some((mutation) => mutation.key === key)) return `cells exchange: ${key} is both counted and written`
  }
  return null
}

/** What makes the control record one no caller makes, or null. */
function malformedRecord({ counts = [] }: CellsExchange, { counter, bytes }: ControlRecord): string | null {
  if (!counts.some(({ key }) => key === counter))
    return `cells exchange: its record is numbered by ${counter}, uncounted`
  if (!Number.isSafeInteger(bytes) || bytes < 0) return `cells exchange: its record is ${bytes} bytes long`
  return null
}

/** Every head write is checked here; drivers only compare-exchange. */
function assertHeadNextWellFormed(next: HeadNext): void {
  const { head, ttlMs } = next
  // Every driver stores the revision as it is given, so it can't repeat under the room's id.
  assert(
    typeof head.rev === 'string' && head.rev !== '',
    'head exchange: the next head must name the revision its caller minted',
  )
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
