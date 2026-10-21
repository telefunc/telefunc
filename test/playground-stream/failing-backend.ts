export { installFailingBackend, addBackendFault, pendingBackendFaults, endBackendSubscriptions }
export type { BackendFault }

import {
  installBackend,
  DriverAttempt,
  MemoryBackend,
  type BackendPayload,
  type BackendReceiver,
  type BroadcastRoute,
  type RoomSubscriptionSource,
  type SubscriptionAttempt,
} from 'telefunc/__internal'

/** `op` is a driver method (its next matching call rejects), 'bind' (the next matching subscribe throws), 'subscribe'
 *  (the next matching subscription refuses to open) or 'deliver' (the next message a matching subscription gets is
 *  dropped). `roomId`, or `key` (a Broadcast key, the start of a Room cell key the call reads or writes, or a commit's
 *  lane kind or required cell), narrows what matches. */
type BackendFault = { op: string; roomId?: string; key?: string; times?: number }

type Source = BroadcastRoute | RoomSubscriptionSource
type Call = { roomId?: string; keys: string[] }

const { faults, attempts } = ((globalThis as Record<symbol, unknown>)[
  Symbol.for('playground-stream:failingBackend')
] ??= {
  faults: [],
  attempts: new Set(),
}) as { faults: (BackendFault & { times: number })[]; attempts: Set<FailingAttempt> }

class FailingAttempt extends DriverAttempt {
  constructor(
    readonly source: Source,
    private readonly _inner: SubscriptionAttempt,
  ) {
    super()
    this.transition(_inner.state())
    _inner.onStateChange((state, reason) => this.transition(state, reason))
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
  const memory = new MemoryBackend()
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
          const attempt = new FailingAttempt(source, inner)
          attempts.add(attempt)
          return attempt
        },
      }
    },
    partitionHere: (source: Source) => memory.subscriptions.partitionHere(source),
  }
  const driver = new Proxy(memory, {
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
  installBackend(() => driver, ['failing-backend'])
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
