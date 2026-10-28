export {
  installFailingBackend,
  addBackendFault,
  pendingBackendFaults,
  endBackendSubscriptions,
  holdCellPages,
  holdSubscriptions,
}
export type { BackendFault }

import {
  installBackend,
  getRoomBackend,
  createDeferred,
  DriverAttempt,
  MemoryBackend,
  MemoryRoomDriver,
  type CellsRead,
  type RoomBackend,
  type BackendPayload,
  type BackendReceiver,
  type BroadcastRoute,
  type RoomSubscriptionSource,
  type SubscriptionAttempt,
  type SubscriptionDriver,
} from 'telefunc/__internal'

/** `op` is a driver method (its next matching call rejects), 'bind' (the next matching subscribe throws), 'subscribe'
 *  (the next matching subscription refuses to open) or 'deliver' (the next message a matching subscription gets is
 *  dropped). `roomId`, or `key` (a Broadcast key, the start of a Room cell key the call reads or writes, or a commit's
 *  lane kind or required cell), narrows what matches. */
type BackendFault = { op: string; roomId?: string; key?: string; times?: number }

type Source = BroadcastRoute | RoomSubscriptionSource
type Call = { roomId?: string; keys: string[] }
type CellPagesHold = { roomId: string; key: string; pages: number; wait: () => Promise<void> }

const { faults, attempts, holds, holding, subscriptionHolds } = ((globalThis as Record<symbol, unknown>)[
  Symbol.for('playground-stream:failingBackend')
] ??= {
  faults: [],
  attempts: new Set(),
  holds: [],
  holding: new WeakSet(),
  subscriptionHolds: new Map(),
}) as {
  faults: (BackendFault & { times: number })[]
  attempts: Set<FailingAttempt>
  holds: CellPagesHold[]
  holding: WeakSet<RoomBackend>
  subscriptionHolds: Map<string, Promise<void>>
}

class FailingAttempt extends DriverAttempt {
  constructor(
    readonly source: Source,
    private readonly _inner: SubscriptionAttempt,
    held: Promise<void> | undefined,
  ) {
    super()
    const follow = () => {
      this.transition(_inner.state())
      _inner.onStateChange((state, reason) => this.transition(state, reason))
    }
    if (held === undefined) follow()
    else void held.then(follow)
  }

  async unsubscribe(): Promise<void> {
    attempts.delete(this)
    await this._inner.unsubscribe()
  }

  end(): void {
    attempts.delete(this)
    this.transition('closed', new Error('failing-backend: subscription ended'))
    void this._inner.unsubscribe()
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
function failing<Driver extends { subscriptions: SubscriptionDriver<Source> }>(memory: Driver): Driver {
  const subscriptions = {
    bind(source: Source) {
      const call = callOf(source)
      if (takeFault('bind', call)) throw new Error('failing-backend: bind refused')
      const binding = memory.subscriptions.bind(source)
      return {
        partition: binding.partition,
        open(receiver: BackendReceiver<BackendPayload>, localReceiverCount: () => number) {
          if (takeFault('subscribe', call)) throw new Error('failing-backend: subscribe refused')
          const inner = binding.open((payload, info) => {
            if (!takeFault('deliver', call)) receiver(payload, info)
          }, localReceiverCount)
          const held = 'roomId' in source ? subscriptionHolds.get(source.roomId) : undefined
          const attempt = new FailingAttempt(source, inner, held)
          attempts.add(attempt)
          return attempt
        },
      }
    },
    partitionHere: (source: Source) => memory.subscriptions.partitionHere(source),
  }
  return new Proxy(memory, {
    get(target, op) {
      if (op === 'subscriptions') return subscriptions
      const value = Reflect.get(target, op)
      if (typeof value !== 'function' || typeof op !== 'string') return value
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

/** Holds the subscriptions opened on `roomId` until `release()` short of ready, as a store slow to confirm them would. */
function holdSubscriptions(roomId: string): () => void {
  const released = createDeferred()
  subscriptionHolds.set(roomId, released.promise)
  return () => {
    subscriptionHolds.delete(roomId)
    released.resolve()
  }
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
  const ended = [...attempts].filter((attempt) => matches(match, callOf(attempt.source)))
  for (const attempt of ended) attempt.end()
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
  const backend = getRoomBackend()
  if (!holding.has(backend)) {
    holding.add(backend)
    const read = backend.readCellPages
    backend.readCellPages = (roomId, inc, prefix) => heldCellPages(read(roomId, inc, prefix), takeHold(roomId, prefix))
  }
  return { reached: reached.promise, release: released.resolve }
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
