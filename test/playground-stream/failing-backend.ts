export {
  installFailingBackend,
  addBackendFault,
  pendingBackendFaults,
  dropBackendSubscriptions,
  holdCellPages,
  holdRetainedReads,
}
export type { BackendFault }

import {
  DriverAttempt,
  installBackend,
  MemoryBackend,
  type BackendPayload,
  type BackendReceiver,
  type BroadcastRoute,
  servePlane,
  type OrderingInfo,
  type SubscriptionDriver,
} from 'telefunc/__internal'
import {
  getRoomPlane,
  MemoryRoomDriver,
  ROOM_PLANE,
  type CellsRead,
  type CommitResult,
  type RoomPlane,
  type RoomSubscriptionSource,
} from '@telefunc/room/backend'
import { createDeferred } from './createDeferred'
import { fieldOf, listOf, optionalFieldOf, present, textAt, textListOf, textOf } from './checked'

/** `op` is a driver method (its next matching call rejects; with `after`, once it ran, as a store whose reply is lost),
 *  'subscribe' (the next matching subscribe throws, a refusal), 'deliver' (the next message a matching subscription
 *  gets is dropped), 'lost' (a matching subscription is lost right before its next message, as a store that keeps
 *  delivering while it restores its link), 'early' (the next matching subscription is ready only after its first
 *  message, as a store whose acknowledgment comes after what it delivered; each further time, one message later),
 *  'handoff' (the next matching Room commit's handoff rejects once it committed, as a fan-out that failed) or
 *  'published' (the next matching call answers 0, as a store that doesn't fix a publish's recipients, like Redis).
 *  `roomId`, or `key` (a Broadcast key, the start of a Room cell key the call reads or writes, a commit's or a
 *  subscription's lane kind or a required cell, or an operation's name), narrows what matches. */
type BackendFault = { op: string; roomId?: string; key?: string; times?: number; after?: boolean }

type Source = BroadcastRoute | RoomSubscriptionSource
type Call = { roomId?: string; keys: string[] }
type CellPagesHold = { roomId: string; key: string; pages: number; wait: () => Promise<void> }
type RetainedReadsHold = { roomId: string; waiting: number; released: Promise<void> }

type FailingBackendState = {
  faults: (BackendFault & { times: number })[]
  subscriptions: Set<FailingAttempt>
  holds: CellPagesHold[]
  holding: WeakSet<RoomPlane['direct']['reads']>
  retainedHolds: RetainedReadsHold[]
  holdingRetained: WeakSet<RoomPlane['direct']['reads']>
}

declare global {
  /** The faults and holds, per process: a dev server's second module graph shares the first one's. */
  var playgroundFailingBackend: FailingBackendState | undefined
}

const { faults, subscriptions, holds, holding, retainedHolds, holdingRetained } =
  (globalThis.playgroundFailingBackend ??= {
    faults: [],
    subscriptions: new Set(),
    holds: [],
    holding: new WeakSet(),
    retainedHolds: [],
    holdingRetained: new WeakSet(),
  })

type FailingAttemptOptions = {
  source: Source
  /** Core's end, which this one passes the memory attempt's deliveries on to. */
  outer: BackendReceiver
}

/** An attempt of the in-memory backend as core sees it: what the memory attempt says, passed on unless a test drops
 *  it. */
class FailingAttempt extends DriverAttempt implements BackendReceiver {
  readonly source: Source
  private readonly outer: BackendReceiver
  private inner: DriverAttempt | null = null
  private unobserve: (() => void) | null = null
  /** It is ready only after its next message (the 'early' fault). */
  private readyAfterMessage = false

  constructor({ source, outer }: FailingAttemptOptions) {
    super()
    this.source = source
    this.outer = outer
  }

  /** Follows the memory attempt, which this one is the receiver of. */
  follow(inner: DriverAttempt): void {
    this.inner = inner
    this.unobserve = inner.onStateChange((state, reason) => this.innerChanged(state, reason))
    const state = inner.state()
    if (state !== 'establishing') this.innerChanged(state, undefined)
  }

  deliver(payload: BackendPayload, ordering: OrderingInfo): void {
    const call = callOf(this.source)
    if (takeFault('deliver', call)) return
    if (takeFault('lost', call)) this.transition('lost')
    this.outer.deliver(payload, ordering)
    if (!this.readyAfterMessage) return
    this.readyAfterMessage = false
    this.ready()
  }

  async unsubscribe(): Promise<void> {
    this.stop()
    this.transition('closed')
    await this.inner?.unsubscribe()
  }

  /** As a driver reports a store that dropped it and subscribed it again: lost, then ready, still delivering. */
  drop(): void {
    if (this.state() !== 'ready') return
    this.transition('lost')
    this.transition('ready')
  }

  private innerChanged(state: 'ready' | 'lost' | 'closed', reason: unknown): void {
    if (state === 'ready') return this.ready()
    if (state === 'lost') return this.transition('lost')
    this.stop()
    this.transition('closed', reason)
  }

  private ready(): void {
    if (takeFault('early', callOf(this.source))) this.readyAfterMessage = true
    else this.transition('ready')
  }

  private stop(): void {
    subscriptions.delete(this)
    this.unobserve?.()
    this.unobserve = null
  }
}

/** Installs the in-memory backend, failing what a test asks it to fail. */
function installFailingBackend() {
  const room = failing(new MemoryRoomDriver())
  const backend = failing(Object.assign(new MemoryBackend(), { planes: [servePlane(ROOM_PLANE, () => room)] }))
  installBackend(() => backend, ['failing-backend'])
}

/** `memory`, failing what a test asks it to fail. */
function failing<Driver extends SubscriptionDriver<Source>>(memory: Driver): Driver {
  const subscribe = (source: Source, outer: BackendReceiver): DriverAttempt => {
    if (takeFault('subscribe', callOf(source))) throw new Error('failing-backend: subscribe refused')
    const attempt = new FailingAttempt({ source, outer })
    subscriptions.add(attempt)
    attempt.follow(memory.subscribe(source, attempt))
    return attempt
  }
  return new Proxy(memory, {
    get(target, op) {
      if (op === 'subscribe') return subscribe
      const value = Reflect.get(target, op)
      if (typeof value !== 'function' || typeof op !== 'string') return value
      if (op === 'published')
        return (key: string) => (takeFault(op, { keys: [key] }) ? 0 : Reflect.apply(value, target, [key]))
      return (...args: unknown[]) => {
        const call = callOfArgs(op, args)
        if (op === 'commitLane' && takeFault('handoff', call)) {
          const commit = Reflect.apply(value, target, args)
          if (!(commit instanceof Promise)) throw new Error('failing-backend: a commit that is no promise')
          return handoffLost(commit)
        }
        const fault = takeFault(op, call)
        if (!fault) return Reflect.apply(value, target, args)
        const error = new Error(`failing-backend: ${op} failed`)
        if (fault.after) {
          return Promise.resolve(Reflect.apply(value, target, args)).then(() => Promise.reject(error))
        }
        return value.constructor.name === 'AsyncGeneratorFunction'
          ? (async function* () {
              throw error
            })()
          : Promise.reject(error)
      }
    },
  })
}

/** `commit`, its handoff rejected. */
async function handoffLost(commit: Promise<CommitResult>): Promise<CommitResult> {
  const result = await commit
  if (result.kind !== 'accepted' || result.delivery === undefined) return result
  const delivery = Promise.reject(new Error('failing-backend: handoff lost'))
  delivery.catch(() => {})
  return { ...result, delivery }
}

function addBackendFault(fault: BackendFault) {
  faults.push({ ...fault, times: fault.times ?? 1 })
}

/** How many failures the added faults still hold. */
function pendingBackendFaults(): number {
  return faults.reduce((sum, fault) => sum + fault.times, 0)
}

/** Drops every subscription open on `roomId` or the Broadcast `key`, as a store that drops them and is back at once
 *  would; returns how many. */
function dropBackendSubscriptions(match: { roomId?: string; key?: string }): number {
  const dropped = [...subscriptions].filter((subscription) => matches(match, callOf(subscription.source)))
  for (const subscription of dropped) subscription.drop()
  return dropped.length
}

/** Holds the next `readCellPages` call of `roomId` whose prefix starts with `key`, once it yielded `pages` pages, until
 *  `release()`; `reached` resolves when it holds. Works on any installed backend, Redis included. */
function holdCellPages(roomId: string, key: string, pages: number): { reached: Promise<void>; release: () => void } {
  const reached = createDeferred()
  const released = createDeferred()
  const wait = () => {
    reached.resolve()
    return released.promise
  }
  holds.push({ roomId, key, pages, wait })
  // Room reads cell pages through the plane's reads, which every store of the plane shares.
  const { reads } = getRoomPlane().direct
  if (!holding.has(reads)) {
    holding.add(reads)
    const read = reads.readCellPages.bind(reads)
    reads.readCellPages = (roomId, inc, prefix, beside) =>
      heldCellPages(read(roomId, inc, prefix, beside), takeHold(roomId, prefix))
  }
  return { reached: reached.promise, release: released.resolve }
}

/** Holds every read of `roomId`'s retained binary frames until `release()`; `held()` counts the reads waiting. Works on
 *  any installed backend, Redis included. */
function holdRetainedReads(roomId: string): { held: () => number; release: () => void } {
  const released = createDeferred()
  const hold: RetainedReadsHold = { roomId, waiting: 0, released: released.promise }
  retainedHolds.push(hold)
  // Room reads retained frames through the plane's reads, which every store of the plane shares.
  const { reads } = getRoomPlane().direct
  if (!holdingRetained.has(reads)) {
    holdingRetained.add(reads)
    const read = reads.readRetained.bind(reads)
    reads.readRetained = async (roomId, inc, lane) => {
      const hold = lane.kind === 'binary' ? retainedHolds.find((hold) => hold.roomId === roomId) : undefined
      if (hold !== undefined) {
        hold.waiting++
        await hold.released
      }
      return read(roomId, inc, lane)
    }
  }
  return {
    held: () => hold.waiting,
    release: () => {
      retainedHolds.splice(retainedHolds.indexOf(hold), 1)
      released.resolve()
    },
  }
}

function takeHold(roomId: string, prefix: string): CellPagesHold | undefined {
  const index = holds.findIndex((hold) => hold.roomId === roomId && prefix.startsWith(hold.key))
  return index === -1 ? undefined : holds.splice(index, 1)[0]
}

async function* heldCellPages(pages: AsyncIterable<CellsRead>, hold: CellPagesHold | undefined) {
  let yielded = 0
  if (hold?.pages === 0) await hold.wait()
  for await (const page of pages) {
    yield page
    if (++yielded === hold?.pages) await hold.wait()
  }
}

function takeFault(op: string, call: Call): BackendFault | null {
  const index = faults.findIndex((fault) => fault.op === op && matches(fault, call))
  if (index === -1) return null
  const fault = present(faults[index], 'the fault')
  if (--fault.times === 0) faults.splice(index, 1)
  return fault
}

function matches(fault: { roomId?: string; key?: string }, call: Call): boolean {
  if (fault.roomId !== undefined && fault.roomId !== call.roomId) return false
  const { key } = fault
  return key === undefined || call.keys.some((each) => each.startsWith(key))
}

function callOf(source: Source): Call {
  return 'roomId' in source ? { roomId: source.roomId, keys: [source.lane.kind] } : { keys: [source.key] }
}

function callOfArgs(op: string, args: unknown[]): Call {
  const [first, , selector] = args
  if (op === 'publish') return { keys: [textAt(first, 'key')] }
  if (op === 'commitLane') {
    const required = optionalFieldOf(args[2], 'requiredCellKeys')
    return {
      roomId: textAt(first, 'roomId'),
      keys: [textAt(fieldOf(first, 'lane'), 'kind'), ...(required === undefined ? [] : textListOf(required))],
    }
  }
  const roomId = typeof first === 'string' ? first : undefined
  if (op === 'runOperation') return { roomId, keys: [textAt(selector, 'name')] }
  if (op === 'readCellPages') return { roomId, keys: [textOf(selector)] }
  if (op === 'readCells') {
    const keys = optionalFieldOf(selector, 'keys')
    return { roomId, keys: keys === undefined ? [textAt(selector, 'prefix')] : textListOf(keys) }
  }
  if (op === 'compareExchangeCells') {
    const counts = optionalFieldOf(selector, 'counts')
    const cells = [
      ...listOf(fieldOf(selector, 'expect')),
      ...listOf(fieldOf(selector, 'mutations')),
      ...(counts === undefined ? [] : listOf(counts)),
    ]
    return { roomId, keys: cells.map((cell) => textAt(cell, 'key')) }
  }
  return { roomId, keys: [] }
}
