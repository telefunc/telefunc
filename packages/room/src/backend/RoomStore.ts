export { RoomStore }
export type { RoomReads }

import { assertDriverOrdering, type BackendCall } from 'telefunc/__internal'
import type {
  CellsExchange,
  CellsExchangeResult,
  CommitOptions,
  CommitResult,
  CommitTarget,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  CommitKept,
  LaneId,
  JoinOperationOptions,
  OperationArgs,
  OperationName,
  OperationOf,
  OperationOptions,
  OperationResult,
  OperationValue,
  RetainedFrame,
  RoomDriver,
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

/** The store a Room server step reaches: the driver's writes, with the head writes and the orderings checked, and its
 *  reads. A public call's store (`RoomPlane.call`) counts each write in the call's account as it is handed over; the
 *  store of work that tells no answer (`RoomPlane.direct`) has no account. A helper takes the store it is given: it
 *  can't tell which it has. */
class RoomStore {
  readonly reads: RoomReads
  private readonly driver: RoomDriver
  private readonly account: BackendCall | null

  constructor(driver: RoomDriver, reads: RoomReads, account: BackendCall | null = null) {
    this.driver = driver
    this.reads = reads
    this.account = account
  }

  compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    assertHeadNextWellFormed(next)
    return this.write(() => this.driver.compareExchangeHead(roomId, expected, next))
  }

  compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    assertCellsExchangeWellFormed(exchange)
    return this.write(() => this.driver.compareExchangeCells(roomId, inc, exchange))
  }

  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    return this.write(async () => {
      const result = await this.driver.commitLane(target, payload, opts)
      if (result.kind === 'accepted') assertDriverOrdering(result)
      return result
    })
  }

  /** The commit's size rule, asked before the call writes anything else: it writes nothing, so it counts for nothing. */
  assertCommittable(lane: LaneId, payload: Uint8Array, kept: CommitKept): void {
    this.driver.assertCommittable(lane, payload, kept)
  }

  runOperation<N extends OperationName>(
    roomId: string,
    inc: string,
    operation: OperationOf<N>,
    args: OperationArgs[N],
    options?: OperationOptions,
  ): Promise<OperationResult<OperationValue[N]>> {
    return this.write(async () =>
      checkedOperation(await this.driver.runOperation(roomId, inc, operation, args, options)),
    )
  }

  runJoin(
    roomId: string,
    inc: string,
    join: OperationOf<'join'>,
    args: OperationArgs['join'],
    options: JoinOperationOptions,
  ): Promise<OperationResult<OperationValue['join']>> {
    return this.write(async () => checkedOperation(await this.driver.runJoin(roomId, inc, join, args, options)))
  }

  dropIncarnation(roomId: string, inc: string): Promise<void> {
    return this.write(() => this.driver.dropIncarnation(roomId, inc))
  }

  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.write(() => this.driver.directoryPut(roomId, incTag, checkAt, minDelayMs))
  }

  directoryDelete(roomId: string, version: string): Promise<void> {
    return this.write(() => this.driver.directoryDelete(roomId, version))
  }

  /** The value of `work`, which runs on the store `shared` and may be shared with other calls, once it settled. Where
   *  both stores count for a call, a write of `shared`'s call counts as this call's, and this call's close waits for
   *  `work`. */
  join<T>(shared: RoomStore, work: Promise<T>): Promise<T> {
    return this.account !== null && shared.account !== null ? this.account.join(shared.account, work) : work
  }

  /** `step`, counted in the call's account where the store has one. */
  private write<T>(step: () => Promise<T>): Promise<T> {
    return this.account === null ? step() : this.account.write(step)
  }
}

/** Every cells exchange is checked here: a counted cell is counted once and isn't also written, which a driver's check
 *  of each count before the exchange's first write rests on. */
function assertCellsExchangeWellFormed(exchange: CellsExchange): void {
  const malformed = malformedCount(exchange)
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

/** `result`, its commit's order checked. */
function checkedOperation<Value>(result: OperationResult<Value>): OperationResult<Value> {
  if (result.kind === 'done' && result.commit !== undefined) assertDriverOrdering(result.commit)
  return result
}
