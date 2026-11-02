export {
  installFailingBackend,
  addBackendFault,
  pendingBackendFaults,
  endBackendSubscriptions,
  holdCellPages,
  holdRetainedReads,
}
export type { BackendFault }

import {
  installBackend,
  MemoryBackend,
  type BackendPayload,
  type BroadcastRoute,
  type OrderingInfo,
  type Subscriber,
  type Unsubscribe,
} from 'telefunc/__internal'
import {
  getRoomPlane,
  MemoryRoomDriver,
  type CellsRead,
  type RoomDriver,
  type RoomPlane,
  type RoomSubscriptionSource,
} from '@telefunc/room/backend'
import { createDeferred } from './createDeferred'

/** `op` is a driver method (its next matching call rejects), 'bind' (the next matching subscribe throws), 'subscribe'
 *  (the next matching subscription is refused), 'deliver' (the next message a matching subscription gets is dropped),
 *  'lost' (a matching subscription is lost right before its next message, as a store that keeps delivering while it
 *  restores its link), 'early' (the next matching subscription is ready only after its first message, as a store whose
 *  acknowledgment comes after what it delivered; each further time, one message later) or 'lastSeq' (the next matching
 *  call answers 0, as a store that doesn't fix a publish's recipients, like Redis). `roomId`, or `key` (a Broadcast
 *  key, the start of a Room cell key the call reads or writes, or a commit's lane kind or required cell), narrows what
 *  matches. */
type BackendFault = { op: string; roomId?: string; key?: string; times?: number }

type Source = BroadcastRoute | RoomSubscriptionSource
type Call = { roomId?: string; keys: string[] }
type CellPagesHold = { roomId: string; key: string; pages: number; wait: () => Promise<void> }
type RetainedReadsHold = { roomId: string; waiting: number; released: Promise<void> }

const { faults, subscriptions, holds, holding, retainedHolds, holdingRetained } = ((
  globalThis as Record<symbol, unknown>
)[Symbol.for('playground-stream:failingBackend')] ??= {
  faults: [],
  subscriptions: new Set(),
  holds: [],
  holding: new WeakSet(),
  retainedHolds: [],
  holdingRetained: new WeakSet(),
}) as {
  faults: (BackendFault & { times: number })[]
  subscriptions: Set<FailingSubscription>
  holds: CellPagesHold[]
  holding: WeakSet<RoomDriver>
  retainedHolds: RetainedReadsHold[]
  holdingRetained: WeakSet<RoomPlane>
}

type FailingSubscriptionOptions = {
  source: Source
  /** Core's end, which this one passes the memory driver's calls on to. */
  outer: Subscriber
}

/** What a subscription of the in-memory backend tells core, passed on unless a test drops it. */
class FailingSubscription implements Subscriber {
  readonly source: Source
  private readonly outer: Subscriber
  unsubscribeInner: Unsubscribe | null = null
  /** Its `ready()` waits for its next message (the 'early' fault). */
  private readyAfterMessage = false

  constructor({ source, outer }: FailingSubscriptionOptions) {
    this.source = source
    this.outer = outer
  }

  get consumers(): number {
    return this.outer.consumers
  }

  message(payload: BackendPayload, ordering: OrderingInfo): void {
    const call = callOf(this.source)
    if (takeFault('deliver', call)) return
    if (takeFault('lost', call)) this.outer.lost()
    this.outer.message(payload, ordering)
    if (!this.readyAfterMessage) return
    this.readyAfterMessage = false
    this.ready()
  }

  ready(): void {
    if (takeFault('early', callOf(this.source))) this.readyAfterMessage = true
    else this.outer.ready()
  }

  lost(): void {
    this.outer.lost()
  }

  ended(): void {
    subscriptions.delete(this)
    this.outer.ended()
  }

  failed(error: unknown): void {
    subscriptions.delete(this)
    this.outer.failed(error)
  }

  async unsubscribe(): Promise<void> {
    subscriptions.delete(this)
    await this.unsubscribeInner?.()
  }

  end(): void {
    subscriptions.delete(this)
    this.outer.failed(new Error('failing-backend: subscription ended'))
    void this.unsubscribeInner?.()
  }
}

/** Installs the in-memory backend, failing what a test asks it to fail. */
function installFailingBackend() {
  const room = failing(new MemoryRoomDriver())
  const backend = failing(
    Object.assign(new MemoryBackend(), { planes: { [Symbol.for('telefunc.room.plane')]: () => room } }),
  )
  installBackend(() => backend, ['failing-backend'])
}

/** `memory`, failing what a test asks it to fail. */
function failing<Driver extends { subscribe(source: Source, subscriber: Subscriber): Unsubscribe }>(
  memory: Driver,
): Driver {
  const subscribe = (source: Source, outer: Subscriber): Unsubscribe => {
    const call = callOf(source)
    if (takeFault('bind', call)) throw new Error('failing-backend: bind refused')
    if (takeFault('subscribe', call)) {
      outer.failed(new Error('failing-backend: subscribe refused'))
      return async () => {}
    }
    const subscription = new FailingSubscription({ source, outer })
    subscriptions.add(subscription)
    subscription.unsubscribeInner = memory.subscribe(source, subscription)
    return () => subscription.unsubscribe()
  }
  return new Proxy(memory, {
    get(target, op) {
      if (op === 'subscribe') return subscribe
      const value = Reflect.get(target, op)
      if (typeof value !== 'function' || typeof op !== 'string') return value
      if (op === 'lastSeq')
        return (key: string) => (takeFault(op, { keys: [key] }) ? 0 : Reflect.apply(value, target, [key]))
      return (...args: unknown[]) => {
        if (!takeFault(op, callOfArgs(op, args))) return Reflect.apply(value, target, args)
        const error = new Error(`failing-backend: ${op} failed`)
        return value.constructor.name === 'AsyncGeneratorFunction'
          ? (async function* () {
              throw error
            })()
          : Promise.reject(error)
      }
    },
  })
}

function addBackendFault(fault: BackendFault) {
  faults.push({ ...fault, times: fault.times ?? 1 })
}

/** How many failures the added faults still hold. */
function pendingBackendFaults(): number {
  return faults.reduce((sum, fault) => sum + fault.times, 0)
}

/** Ends every subscription open on `roomId` or the Broadcast `key`, as a store that drops them would; returns how many. */
function endBackendSubscriptions(match: { roomId?: string; key?: string }): number {
  const ended = [...subscriptions].filter((subscription) => matches(match, callOf(subscription.source)))
  for (const subscription of ended) subscription.end()
  return ended.length
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
  const { driver } = getRoomPlane()
  if (!holding.has(driver)) {
    holding.add(driver)
    const read = driver.readCellPages.bind(driver)
    driver.readCellPages = (roomId, inc, prefix) => heldCellPages(read(roomId, inc, prefix), takeHold(roomId, prefix))
  }
  return { reached: reached.promise, release: released.resolve }
}

/** Holds every read of `roomId`'s retained binary frames until `release()`; `held()` counts the reads waiting. Works on
 *  any installed backend, Redis included. */
function holdRetainedReads(roomId: string): { held: () => number; release: () => void } {
  const released = createDeferred()
  const hold: RetainedReadsHold = { roomId, waiting: 0, released: released.promise }
  retainedHolds.push(hold)
  const plane = getRoomPlane()
  if (!holdingRetained.has(plane)) {
    holdingRetained.add(plane)
    const read = plane.readRetained.bind(plane)
    plane.readRetained = async (roomId, inc, lane) => {
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

function takeFault(op: string, call: Call): boolean {
  const index = faults.findIndex((fault) => fault.op === op && matches(fault, call))
  if (index === -1) return false
  if (--faults[index]!.times === 0) faults.splice(index, 1)
  return true
}

function matches(fault: { roomId?: string; key?: string }, call: Call): boolean {
  if (fault.roomId !== undefined && fault.roomId !== call.roomId) return false
  return fault.key === undefined || call.keys.some((key) => key.startsWith(fault.key!))
}

function callOf(source: Source): Call {
  return 'roomId' in source ? { roomId: source.roomId, keys: [] } : { keys: [source.key] }
}

function callOfArgs(op: string, args: unknown[]): Call {
  if (op === 'publish') return { keys: [(args[0] as BroadcastRoute).key] }
  if (op === 'commitLane') {
    const target = args[0] as { roomId: string; lane: { kind: string } }
    const opts = args[2] as { requiredCellKeys?: string[] } | undefined
    return { roomId: target.roomId, keys: [target.lane.kind, ...(opts?.requiredCellKeys ?? [])] }
  }
  const [roomId, , selector] = args as [string, string, unknown]
  if (op === 'readCellPages') return { roomId, keys: [selector as string] }
  if (op === 'readCells') {
    const sel = selector as { keys: string[] } | { prefix: string }
    return { roomId, keys: 'keys' in sel ? sel.keys : [sel.prefix] }
  }
  if (op === 'compareExchangeCells') {
    const cx = selector as { expect: { key: string }[]; mutations: { key: string }[]; counts?: { key: string }[] }
    return { roomId, keys: [...cx.expect, ...cx.mutations, ...(cx.counts ?? [])].map((cell) => cell.key) }
  }
  return { roomId, keys: [] }
}
