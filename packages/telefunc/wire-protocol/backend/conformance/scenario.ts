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
import {
  conformanceReceiver,
  pollRemoteReceiver,
  releaseRemoteReceiver,
  seedRemoteReceiver,
  type ReceiverFrame,
} from './receiver.js'

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

export type Frame = ReceiverFrame

export type Collector = {
  receiver: LaneReceiver
  frames: Frame[]
  payloads(): string[]
  waitFor(count: number, timeoutMs?: number): Promise<void>
}

export function collector(): Collector {
  const frames: Frame[] = []
  let waiters: Array<() => void> = []
  const local: LaneReceiver = (payload, info) => {
    frames.push({ payload: text(payload), seq: info.seq, timestamp: info.timestamp })
    const woken = waiters
    waiters = []
    for (const wake of woken) wake()
  }
  const receiver = conformanceReceiver({ kind: 'collect' }, local, (observations) => {
    for (const observation of observations) {
      frames.push({ payload: observation.payload, seq: observation.seq, timestamp: observation.timestamp })
    }
    const woken = waiters
    waiters = []
    for (const wake of woken) wake()
  })
  const waitFor = (count: number, timeoutMs: number = DELIVERY_BOUND_MS): Promise<void> =>
    new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`waited ${timeoutMs}ms for ${count} frame(s), observed ${frames.length}`)),
        timeoutMs,
      )
      const check = (): void => {
        if (frames.length < count) {
          waiters.push(check)
          void pollRemoteReceiver(receiver).catch(reject)
          return
        }
        clearTimeout(timer)
        resolve()
      }
      check()
    })
  return { receiver, frames, payloads: () => frames.map((frame) => frame.payload), waitFor }
}

export function noopReceiver(): LaneReceiver {
  return collector().receiver
}

export type ControlledReceiver = {
  receiver: LaneReceiver
  frames: Frame[]
  payloads(): string[]
  entries(): number
  waitForEntry(timeoutMs?: number): Promise<void>
  release(): Promise<void>
}

// A transport-neutral stalled receiver. Memory/Redis execute the local closure; Cloudflare binds the
// same command to the worker-owned session manager and exposes only release/observation controls.
export function stallingReceiver(onEnter?: () => void): ControlledReceiver {
  const gate = deferred()
  const frames: Frame[] = []
  let entries = 0
  let waiters: Array<() => void> = []
  const entered = (): void => {
    entries += 1
    onEnter?.()
    const current = waiters
    waiters = []
    for (const wake of current) wake()
  }
  const local: LaneReceiver = (payload, info) => {
    frames.push({ payload: text(payload), seq: info.seq, timestamp: info.timestamp })
    entered()
    return gate.promise as unknown as void
  }
  const receiver = conformanceReceiver({ kind: 'stall' }, local, (observations) => {
    for (const observation of observations) {
      frames.push({ payload: observation.payload, seq: observation.seq, timestamp: observation.timestamp })
      entered()
    }
  })
  return {
    receiver,
    frames,
    payloads: () => frames.map((frame) => frame.payload),
    entries: () => entries,
    waitForEntry: (timeoutMs: number = DELIVERY_BOUND_MS) =>
      new Promise<void>((resolve, reject) => {
        if (entries > 0) return resolve()
        const timer = setTimeout(() => reject(new Error(`waited ${timeoutMs}ms for stalled receiver entry`)), timeoutMs)
        waiters.push(() => {
          clearTimeout(timer)
          resolve()
        })
        void pollRemoteReceiver(receiver).catch(reject)
      }),
    release: async () => {
      if (!(await releaseRemoteReceiver(receiver))) gate.resolve()
    },
  }
}

export type ThrowingReceiver = { receiver: LaneReceiver; calls(): number }

export function throwingReceiver(message: string, payload?: string): ThrowingReceiver {
  let calls = 0
  const local: LaneReceiver = (frame) => {
    calls += 1
    if (payload === undefined || text(frame) === payload) throw new Error(message)
  }
  const receiver = conformanceReceiver({ kind: 'throw', message, payload }, local, (observations) => {
    calls += observations.length
  })
  return { receiver, calls: () => calls }
}

export function sequencedReceiver(
  outcomes: Array<'collect' | 'throw'>,
  message: string = 'receiver failed',
): Collector {
  const frames: Frame[] = []
  let call = 0
  const local: LaneReceiver = (payload, info) => {
    frames.push({ payload: text(payload), seq: info.seq, timestamp: info.timestamp })
    if (outcomes[call++] === 'throw') throw new Error(message)
  }
  const receiver = conformanceReceiver({ kind: 'sequence', outcomes, message }, local, (observations) => {
    frames.push(...observations.map(({ payload, seq, timestamp }) => ({ payload, seq, timestamp })))
  })
  return {
    receiver,
    frames,
    payloads: () => frames.map((frame) => frame.payload),
    waitFor: async (count, timeoutMs = DELIVERY_BOUND_MS) => {
      const deadline = Date.now() + timeoutMs
      while (frames.length < count) {
        await pollRemoteReceiver(receiver)
        if (frames.length >= count) return
        if (Date.now() >= deadline)
          throw new Error(`waited ${timeoutMs}ms for ${count} frame(s), observed ${frames.length}`)
        await new Promise<void>((resolve) => setTimeout(resolve, 0))
      }
    },
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

  const local: LaneReceiver = (payload, info) => {
    const frame = { payload: text(payload), seq: info.seq }
    if (seeded) emit(frame, 'live')
    else pending.push(frame) // live frames are held until the seed is applied, then deduped against it
  }
  const receiver = conformanceReceiver({ kind: 'seeded' }, local, (observations) => {
    for (const frame of observations) {
      observed.push({ payload: frame.payload, seq: frame.seq, source: frame.source ?? 'live' })
    }
  })
  const sub = backend.subscribeLane(roomId, inc, lane, receiver)

  const seed = async (): Promise<void> => {
    if (await seedRemoteReceiver(receiver)) {
      await pollRemoteReceiver(receiver)
      return
    }
    const retained = await backend.readRetained(roomId, inc, lane)
    if (retained !== null) emit({ payload: text(retained.payload), seq: retained.seq }, 'seed')
    seeded = true
    for (const frame of pending.splice(0)) emit(frame, 'live')
  }

  return { sub, observed, seed, close: () => sub.unsubscribe() }
}
