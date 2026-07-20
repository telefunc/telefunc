// Scenario vocabulary shared by the conformance modules: the ceremony steps of spi.md §4, the narrowing
// helpers that turn an unexpected SPI result into a failure at the point it happens, and the two
// subscriber models the readiness tests need.
//
// Deliberately free of any vitest import — these helpers are production-compiled alongside the backends
// (tsc excludes only *.spec.ts), and keeping assertions in the spec modules keeps one altitude per file.

import type {
  CommitAccepted,
  CommitResult,
  HeadCx,
  LaneId,
  LaneReceiver,
  LaneSubscription,
  RoomBackendSpi,
  RoomHead,
} from '../spi.js'

// Room core's own close-lease duration, and the tombstone TTL the close ceremony installs.
export const CLOSE_LEASE_MS = 15_000
export const TOMBSTONE_TTL_MS = 60_000
// Every wait in the suite is bounded: a scenario that hangs is a failure, never a timeout of the runner.
export const DELIVERY_BOUND_MS = 1_000

export const SEMANTIC: LaneId = { kind: 'semantic' }
export const CONTROL: LaneId = { kind: 'control' }
export const binaryLane = (member: string, track: string): LaneId => ({ kind: 'binary', member, track })
export const inboxLane = (member: string): LaneId => ({ kind: 'inbox', member })

let idSeq = 0
export function nextId(prefix: string): string {
  return `${prefix}-${++idSeq}`
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()
export const bytes = (value: string): Uint8Array => encoder.encode(value)
export const text = (value: Uint8Array): string => decoder.decode(value)

export function deferred<T = void>(): {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (e: unknown) => void
} {
  let resolve!: (value: T) => void
  let reject!: (e: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

export const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0))
export const settled = (promise: Promise<unknown>): Promise<'resolved' | 'rejected'> =>
  promise.then(
    () => 'resolved' as const,
    () => 'rejected' as const,
  )

// ── result narrowing ──

export type HeadCxResult = Awaited<ReturnType<RoomBackendSpi['compareExchangeHead']>>

export function okHead(result: HeadCxResult): RoomHead {
  if ('conflict' in result) throw new Error(`expected a head CX success, got a conflict on ${describe(result.current)}`)
  if (!('head' in result)) throw new Error('expected a stored head, got a delete result')
  return result.head
}

export function okDeleted(result: HeadCxResult): void {
  if ('conflict' in result) throw new Error('expected a head CX delete success, got a conflict')
  if (!('deleted' in result)) throw new Error('expected a delete result, got a stored head')
}

export function conflicted(result: HeadCxResult): RoomHead | null {
  if (!('conflict' in result)) throw new Error('expected a head CX conflict, got a success')
  return result.current
}

export type CellsRead = Awaited<ReturnType<RoomBackendSpi['readCells']>>

export function cellsOf(result: CellsRead): { revision: string; cells: Map<string, Uint8Array> } {
  if ('staleInc' in result) throw new Error('expected a cell read, got { staleInc: true }')
  return result
}

export function isStaleInc(result: CellsRead): boolean {
  return 'staleInc' in result
}

export function accepted(result: CommitResult): CommitAccepted {
  if (!('accepted' in result)) throw new Error('expected the commit to be accepted, got { stale: true }')
  return result
}

export function isStale(result: CommitResult): boolean {
  return !('accepted' in result)
}

function describe(head: RoomHead | null): string {
  return head === null ? 'an absent head' : `${head.state}/${head.currentInc ?? 'null'}@${head.rev}`
}

// ── ceremony steps (spi.md §4) ──

export async function openRoom(
  backend: RoomBackendSpi,
  roomId: string,
  opts: { prior?: RoomHead | null; config?: Uint8Array } = {},
): Promise<{ inc: string; head: RoomHead }> {
  const inc = nextId('inc')
  const prior = opts.prior ?? null
  const cx: HeadCx = prior === null ? { expect: 'absent' } : { expect: { rev: prior.rev } }
  const head = okHead(
    await backend.compareExchangeHead(roomId, cx, {
      head: { currentInc: inc, state: 'open', config: opts.config ?? bytes('config') },
    }),
  )
  return { inc, head }
}

// open → closing. The lease id is the caller's; the DEADLINE is the backend's, so the stored head is the
// only place the closer can learn it.
export async function enterClosing(
  backend: RoomBackendSpi,
  roomId: string,
  head: RoomHead,
  opts: { leaseId?: string; durationMs?: number } = {},
): Promise<{ head: RoomHead; leaseId: string }> {
  const leaseId = opts.leaseId ?? nextId('lease')
  const stored = okHead(
    await backend.compareExchangeHead(
      roomId,
      { expect: { rev: head.rev } },
      {
        head: {
          currentInc: head.currentInc,
          state: 'closing',
          config: head.config,
          closeLease: { id: leaseId, durationMs: opts.durationMs ?? CLOSE_LEASE_MS },
        },
      },
    ),
  )
  return { head: stored, leaseId }
}

// The recovery CX: succeeds only on an expired lease, replaces only the lease.
export async function takeoverClose(
  backend: RoomBackendSpi,
  roomId: string,
  head: RoomHead,
  opts: { leaseId?: string; durationMs?: number } = {},
): Promise<{ result: HeadCxResult; leaseId: string }> {
  const leaseId = opts.leaseId ?? nextId('lease')
  const result = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: head.rev, closingLeaseExpired: true } },
    {
      head: {
        currentInc: head.currentInc,
        state: 'closing',
        config: head.config,
        closeLease: { id: leaseId, durationMs: opts.durationMs ?? CLOSE_LEASE_MS },
      },
    },
  )
  return { result, leaseId }
}

// closing → closed(currentInc: null). Lease-guarded by contract; the generic {rev} form is forbidden here
// and `finalizeUnguarded` below exists purely so the scenarios can prove that refusal.
export function finalizeClose(
  backend: RoomBackendSpi,
  roomId: string,
  head: RoomHead,
  leaseId: string,
  ttlMs: number = TOMBSTONE_TTL_MS,
): Promise<HeadCxResult> {
  return backend.compareExchangeHead(
    roomId,
    { expect: { rev: head.rev, closingLease: leaseId } },
    { head: { currentInc: null, state: 'closed', config: head.config }, ttlMs },
  )
}

export function finalizeUnguarded(
  backend: RoomBackendSpi,
  roomId: string,
  head: RoomHead,
  ttlMs: number = TOMBSTONE_TTL_MS,
): Promise<HeadCxResult> {
  return backend.compareExchangeHead(
    roomId,
    { expect: { rev: head.rev } },
    { head: { currentInc: null, state: 'closed', config: head.config }, ttlMs },
  )
}

export async function readHeadOrThrow(backend: RoomBackendSpi, roomId: string): Promise<RoomHead> {
  const result = await backend.readHead(roomId)
  if (result === null) throw new Error(`expected room '${roomId}' to have a head`)
  return result.head
}

// ── subscriber models ──

export type Frame = { payload: string; seq: number; timestamp: number }

export type Collector = {
  receiver: LaneReceiver
  frames: Frame[]
  payloads(): string[]
  waitFor(count: number, timeoutMs?: number): Promise<void>
}

export function collector(): Collector {
  const frames: Frame[] = []
  let waiters: Array<() => void> = []
  const receiver: LaneReceiver = (payload, info) => {
    frames.push({ payload: text(payload), seq: info.seq, timestamp: info.timestamp })
    const woken = waiters
    waiters = []
    for (const wake of woken) wake()
  }
  const waitFor = (count: number, timeoutMs: number = DELIVERY_BOUND_MS): Promise<void> =>
    new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`waited ${timeoutMs}ms for ${count} frame(s), observed ${frames.length}`)),
        timeoutMs,
      )
      const check = (): void => {
        if (frames.length < count) {
          waiters.push(check)
          return
        }
        clearTimeout(timer)
        resolve()
      }
      check()
    })
  return { receiver, frames, payloads: () => frames.map((frame) => frame.payload), waitFor }
}

// A receiver whose dispatch does not complete until `gate` settles. Only meaningful on backends whose
// handoff attempt extends to the receiver callback (BackendTraces.handoffAwaitsReceiver).
export function stallingReceiver(gate: Promise<unknown>, onEnter?: () => void): LaneReceiver {
  return (): void => {
    onEnter?.()
    // The SPI types a receiver as `=> void`; a backend that awaits the return value is exercising its own
    // documented trace, which is exactly what this helper is for.
    return gate as unknown as void
  }
}

// The subscriber side of the retained-overlap schedules: a live subscription plus one retained seed read,
// deduped against a single watermark in the lane's own order domain — the SeedGate contract, modelled
// here so the suite proves the BACKEND supplies the primitives that make exactly-once reachable.
export type SeededSubscriber = {
  sub: LaneSubscription
  observed: Array<{ payload: string; seq: number; source: 'seed' | 'live' }>
  seed(): Promise<void>
  close(): Promise<void>
}

export function seededSubscriber(backend: RoomBackendSpi, roomId: string, inc: string, lane: LaneId): SeededSubscriber {
  const observed: SeededSubscriber['observed'] = []
  const pending: Array<{ payload: string; seq: number }> = []
  let watermark = 0
  let seeded = false

  const emit = (frame: { payload: string; seq: number }, source: 'seed' | 'live'): void => {
    if (frame.seq <= watermark) return // the dedupe: a frame at or below the watermark was already observed
    watermark = frame.seq
    observed.push({ ...frame, source })
  }

  const sub = backend.subscribeLane(roomId, inc, lane, (payload, info) => {
    const frame = { payload: text(payload), seq: info.seq }
    if (seeded) emit(frame, 'live')
    else pending.push(frame) // live frames are held until the seed is applied, then deduped against it
  })

  const seed = async (): Promise<void> => {
    const retained = await backend.readRetained(roomId, inc, lane)
    if (retained !== null) emit({ payload: text(retained.payload), seq: retained.seq }, 'seed')
    seeded = true
    for (const frame of pending.splice(0)) emit(frame, 'live')
  }

  return { sub, observed, seed, close: () => sub.unsubscribe() }
}
