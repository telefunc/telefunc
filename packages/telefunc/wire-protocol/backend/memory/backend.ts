// The memory reference realization of RoomBackendSpi — the executable specification the Redis and
// Cloudflare backends are proved against. Single isolate, so every operation is synchronous over Maps
// and the isolate clock IS authority time; the only asynchrony is the delivery attempt, which is
// deliberately queued through a per-lane chain so a receiver can never run reentrantly inside
// commitLane.
//
// Mechanism map (spi.md §5.1):
//   head CX          one compare branch per HeadCx form, all resolved on the authority clock
//   lease minting    every CX installing 'closing' stores until = authorityNow + durationMs
//   cells            per-generation revision counter, bumped by any deliberate cell write
//   commit           ONE boolean precondition with two branches (default open / closing-control)
//   delivery         per-(inc, lane) promise chain, settlement-gated, never poisoned
//   TTL              lazy expiry on read + a sweep on the janitor path

import {
  type CellMutation,
  type CommitResult,
  type CxResult,
  type HeadCx,
  type HeadNext,
  type LaneId,
  type LaneReceiver,
  type LaneSubscription,
  MAX_CLOSE_LEASE_MS,
  MIN_CLOSE_LEASE_MS,
  type ReadinessState,
  ROOM_SPI_VERSION,
  type RoomBackendSpi,
  type RoomHead,
} from '../spi.js'

const DEFAULT_MAX_RETAINED_BYTES = 16 * 1024 * 1024
const DIRECTORY_PAGE_SIZE = 100

export type MemoryRoomBackendOptions = {
  // The authority clock. Defaults to the isolate clock, which is what authority time means in a single
  // isolate; conformance injects a controlled clock so lease expiry is provable without wall-clock waits
  // — and so a skewed CALLER clock (Date.now) stays distinguishable from authority time.
  authorityNow?: () => number
  maxRetainedPayloadBytes?: number
}

type StoredHead = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array
  closeLease?: { id: string; until: number }
  expiresAt: number | null
}

type Expiring = { expiresAt: number | null }
type StoredCell = Expiring & { bytes: Uint8Array }
type OrderMark = Expiring & { seq: number; timestamp: number }
type RetainedEntry = { lane: LaneId; payload: Uint8Array; seq: number; timestamp: number }

type Generation = {
  revision: number
  cells: Map<string, StoredCell>
  order: Map<string, OrderMark>
  retained: Map<string, RetainedEntry>
  subs: Map<string, Set<MemoryLaneSubscription>>
  chains: Map<string, Promise<void>>
}

type RoomRecord = { head: StoredHead | null; gens: Map<string, Generation> }

// In the fixed lane table every lane's order domain and channel correspond one to one, so a single key
// indexes both the order watermarks and the subscription channels. Member and track are encoded so a
// member named `a:b` can never collide with another lane.
function laneKey(lane: LaneId): string {
  switch (lane.kind) {
    case 'semantic':
      return 'semantic'
    case 'control':
      return 'control'
    case 'binary':
      return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
    case 'inbox':
      return `inbox:${encodeURIComponent(lane.member)}`
  }
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

function isExpired(entry: Expiring, now: number): boolean {
  return entry.expiresAt !== null && entry.expiresAt <= now
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  return a.byteLength === b.byteLength && a.every((byte, index) => byte === b[index])
}

function newGeneration(): Generation {
  return { revision: 0, cells: new Map(), order: new Map(), retained: new Map(), subs: new Map(), chains: new Map() }
}

function publicHead(head: StoredHead): RoomHead {
  const view: RoomHead = {
    rev: head.rev,
    currentInc: head.currentInc,
    state: head.state,
    config: copyBytes(head.config),
  }
  if (head.closeLease !== undefined) view.closeLease = { ...head.closeLease }
  return view
}

// Shape rules that hold regardless of what is stored: the lease is present iff the head is closing, its
// duration is finite and bounded, a tombstone is the only head that carries a TTL, and only a tombstone
// has a null incarnation.
function assertHeadNextWellFormed(next: HeadNext): void {
  if ('delete' in next) return
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    if (head.closeLease === undefined) throw new Error('head CX: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    if (!(durationMs >= MIN_CLOSE_LEASE_MS && durationMs <= MAX_CLOSE_LEASE_MS)) {
      throw new Error(
        `head CX: close lease durationMs ${durationMs} outside [${MIN_CLOSE_LEASE_MS}, ${MAX_CLOSE_LEASE_MS}]`,
      )
    }
  } else if (head.closeLease !== undefined) {
    throw new Error(`head CX: a '${head.state}' head must not carry a close lease`)
  }
  if (ttlMs !== undefined && head.state !== 'closed') {
    throw new Error(`head CX: ttlMs is only valid for a 'closed' tombstone, got '${head.state}'`)
  }
  if (head.state === 'closed' && head.currentInc !== null) {
    throw new Error('head CX: a closed tombstone must clear currentInc to null')
  }
  if (head.state !== 'closed' && head.currentInc === null) {
    throw new Error(`head CX: a '${head.state}' head must name an incarnation`)
  }
}

type HeadCxForm = 'absent' | 'generic' | 'takeover' | 'finalize'

function headCxForm(cx: HeadCx): HeadCxForm {
  if (cx.expect === 'absent') return 'absent'
  if ('closingLeaseExpired' in cx.expect) return 'takeover'
  if ('closingLease' in cx.expect) return 'finalize'
  return 'generic'
}

// The tombstone-expiry delete is the one operation whose legality is decided BEFORE any compare, so
// misuse throws even where the compare would have conflicted (spi.md §2 transition table).
function assertDeleteLegal(next: HeadNext, current: StoredHead | null): void {
  if (!('delete' in next)) return
  if (current?.state !== 'closed') {
    throw new Error(`head CX: {delete} is legal only against a 'closed' tombstone, not '${current?.state ?? 'absent'}'`)
  }
}

// The NORMATIVE head-transition table of spi.md §2, exhaustive by the triple (current state, HeadCx
// form, next state). Anything off the table is a programming error and THROWS — it is never encoded as
// a conflict. This is what makes the I13 guards unreachable through any other compare form: a generic
// {rev} can never install or replace a close lease, never re-lease a 'closing' head pre-expiry, and
// never reach 'closed'.
function assertTransitionAllowed(
  cx: HeadCx,
  next: Extract<HeadNext, { head: unknown }>,
  current: StoredHead | null,
  generations: ReadonlyMap<string, Generation> | undefined,
): void {
  const from = current === null ? 'absent' : current.state
  const transition = `${from} + ${headCxForm(cx)} -> ${next.head.state}`
  switch (transition) {
    case 'absent + absent -> open':
    case 'closed + generic -> open':
      assertFreshIncarnation(next.head.currentInc, generations)
      return
    case 'open + generic -> open':
    case 'open + generic -> closing':
      // A generic CX can never swap the incarnation — on a config/LWW edit or on the close that fences
      // this very generation.
      if (next.head.currentInc !== current?.currentInc) {
        throw new Error(`head CX: ${transition} must keep the same incarnation`)
      }
      return
    case 'closing + takeover -> closing':
      assertReplacesOnlyTheLease(next.head, current as StoredHead)
      return
    case 'closing + finalize -> closed':
      return
    default:
      throw new Error(`head CX: '${transition}' is not a legal head transition`)
  }
}

// FRESH-INC on create/recreate: the incarnation a room is (re)created on must have NO surviving
// generation state, checked against the very storage listGenerations reads. Room core mints fresh uuids
// anyway; this makes stale-generation resurrection — an old inc's cells silently becoming the "new"
// room's state — unrepresentable even under caller error. A fully-dropped id is harmless by
// construction, because no state survives to expose.
function assertFreshIncarnation(inc: string | null, generations: ReadonlyMap<string, Generation> | undefined): void {
  if (inc !== null && generations?.has(inc) === true) {
    throw new Error(
      `head CX: incarnation '${inc}' still has surviving generation state — creating a room on it would resurrect it`,
    )
  }
}

function assertReplacesOnlyTheLease(next: Extract<HeadNext, { head: unknown }>['head'], current: StoredHead): void {
  if (next.currentInc !== current.currentInc) {
    throw new Error('head CX: an expired-close takeover must keep the same incarnation')
  }
  if (!bytesEqual(next.config, current.config)) {
    throw new Error('head CX: an expired-close takeover must not change the config — it replaces only the lease')
  }
  if (next.closeLease?.id === current.closeLease?.id) {
    throw new Error('head CX: an expired-close takeover must mint a different lease id')
  }
}

class MemoryLaneSubscription implements LaneSubscription {
  readonly ready: Promise<void>
  #state: ReadinessState = 'establishing'
  #settle!: { resolve: () => void; reject: (err: unknown) => void }
  readonly #listeners = new Set<(state: ReadinessState) => void>()
  readonly #receiver: LaneReceiver
  #detach: () => void

  constructor(receiver: LaneReceiver, detach: () => void) {
    this.#receiver = receiver
    this.#detach = detach
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    // A fail-closed establishment rejects `ready` whether or not the caller is awaiting it yet; the
    // handler below only marks the rejection observed, it does not swallow it for the caller.
    void this.ready.catch(() => {})
  }

  get closed(): boolean {
    return this.#state === 'closed'
  }

  state(): ReadinessState {
    return this.#state
  }

  onStateChange(cb: (state: ReadinessState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.closed) return
    this.#detach()
    this.#transition('closed')
  }

  establish(): void {
    this.#transition('ready')
    this.#settle.resolve()
  }

  failEstablishment(reason: string): void {
    this.#transition('closed')
    this.#settle.reject(new Error(reason))
  }

  // The generation this subscription belongs to was dropped: its channel no longer exists and can never
  // come back, so the subscription is terminal rather than merely lost.
  generationDropped(): void {
    this.#detach = () => {}
    this.#transition('closed')
  }

  async deliver(payload: Uint8Array, info: { seq: number; timestamp: number }): Promise<void> {
    // A receiver is typed `=> void`, so its completion is never a cross-backend guarantee. In memory the
    // handoff attempt IS the dispatch call, so a receiver that returns a thenable extends that attempt —
    // this is the trace that makes the ordered-chain algorithm observable here.
    await (this.#receiver(payload, info) as unknown)
  }

  #transition(state: ReadinessState): void {
    if (this.#state === state) return
    this.#state = state
    for (const cb of this.#listeners) cb(state)
  }
}

export class MemoryRoomBackend implements RoomBackendSpi {
  readonly spiVersion = ROOM_SPI_VERSION
  readonly capabilities: RoomBackendSpi['capabilities']

  readonly #now: () => number
  readonly #rooms = new Map<string, RoomRecord>()
  readonly #directory = new Map<string, string>()
  #revSeq = 0
  #disposed = false

  constructor(options: MemoryRoomBackendOptions = {}) {
    this.#now = options.authorityNow ?? Date.now
    this.capabilities = {
      receivers: 'global',
      maxRetainedPayloadBytes: options.maxRetainedPayloadBytes ?? DEFAULT_MAX_RETAINED_BYTES,
      clusterSafe: false,
      directory: true,
    }
  }

  // ── head ──

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    this.#assertLive()
    const head = this.#liveHead(this.#rooms.get(roomId))
    return head === null ? null : { head: publicHead(head) }
  }

  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    this.#assertLive()
    assertHeadNextWellFormed(next)
    const existing = this.#rooms.get(roomId)
    const current = this.#liveHead(existing)
    // Operation legality precedes the compare for the delete path only; every other transition is
    // validated against the head the compare actually matched, so a genuine race still conflicts.
    assertDeleteLegal(next, current)
    if (!this.#headCxMatches(cx, current)) {
      return { conflict: true, current: current === null ? null : publicHead(current) }
    }
    // Only a CX that actually applies materializes a room record.
    const room = this.#roomFor(roomId)
    if ('delete' in next) {
      room.head = null
      return { ok: true, deleted: true }
    }
    assertTransitionAllowed(cx, next, current, existing?.gens)
    return { ok: true, head: publicHead(this.#storeHead(room, next)) }
  }

  #headCxMatches(cx: HeadCx, current: StoredHead | null): boolean {
    if (cx.expect === 'absent') return current === null
    if (current === null || current.rev !== cx.expect.rev) return false
    const expect = cx.expect
    if ('closingLeaseExpired' in expect) {
      return current.state === 'closing' && current.closeLease !== undefined && current.closeLease.until < this.#now()
    }
    if ('closingLease' in expect) {
      return current.state === 'closing' && current.closeLease?.id === expect.closingLease
    }
    return true
  }

  #storeHead(room: RoomRecord, next: Extract<HeadNext, { head: unknown }>): StoredHead {
    const now = this.#now()
    const stored: StoredHead = {
      rev: `rev-${++this.#revSeq}`,
      currentInc: next.head.currentInc,
      state: next.head.state,
      config: copyBytes(next.head.config),
      expiresAt: next.ttlMs === undefined ? null : now + next.ttlMs,
    }
    // The lease deadline is minted here, inside the CX, from authority time — never supplied by a caller.
    if (next.head.closeLease !== undefined) {
      stored.closeLease = { id: next.head.closeLease.id, until: now + next.head.closeLease.durationMs }
    }
    room.head = stored
    if (stored.currentInc !== null) this.#generation(room, stored.currentInc)
    return stored
  }

  // ── generation cells ──

  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    this.#assertLive()
    const room = this.#rooms.get(roomId)
    const head = this.#liveHead(room)
    // Reads stay available while the head is closing — the closer's tail needs them; only writes require
    // an open head (CxResult 'stale-inc').
    if (room === undefined || head === null || head.currentInc !== inc) return { staleInc: true }
    const gen = this.#generation(room, inc)
    const now = this.#now()
    const keys = 'keys' in sel ? sel.keys : [...gen.cells.keys()].filter((key) => key.startsWith(sel.prefix))
    const cells = new Map<string, Uint8Array>()
    for (const key of keys) {
      const cell = gen.cells.get(key)
      if (cell === undefined || isExpired(cell, now)) continue
      cells.set(key, copyBytes(cell.bytes))
    }
    return { revision: String(gen.revision), cells }
  }

  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    this.#assertLive()
    const room = this.#rooms.get(roomId)
    const head = this.#liveHead(room)
    if (room === undefined || head === null || head.currentInc !== inc || head.state !== 'open') return 'stale-inc'
    const gen = this.#generation(room, inc)
    if (String(gen.revision) !== revision) return 'conflict'
    const now = this.#now()
    for (const mutation of mutations) {
      if (mutation.set === undefined) gen.cells.delete(mutation.key)
      else {
        const { bytes, ttlMs } = mutation.set
        gen.cells.set(mutation.key, { bytes: copyBytes(bytes), expiresAt: ttlMs === undefined ? null : now + ttlMs })
      }
    }
    gen.revision += 1
    return 'committed'
  }

  // ── lane commit ──

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string },
  ): Promise<CommitResult> {
    this.#assertLive()
    const room = this.#rooms.get(roomId)
    const head = this.#liveHead(room)
    if (room === undefined || head === null || !this.#commitPreconditionHolds(head, inc, lane, opts?.closingLease)) {
      return { stale: true }
    }
    const gen = this.#generation(room, inc)
    const key = laneKey(lane)
    const frame = copyBytes(payload)
    // Over-cap retain is rejected before anything is mutated, so a throw never half-accepts a commit.
    if (opts?.retain) this.#assertRetainedCapacity(gen, key, frame)
    const mark = this.#advanceOrder(gen, key, opts?.orderTtlMs)
    if (opts?.retain) gen.retained.set(key, { lane, payload: frame, seq: mark.seq, timestamp: mark.timestamp })
    const targets = [...(gen.subs.get(key) ?? [])]
    const info = { seq: mark.seq, timestamp: mark.timestamp }
    return {
      accepted: true,
      seq: mark.seq,
      timestamp: mark.timestamp,
      receivers: targets.length,
      delivery: this.#enqueueAttempt(gen, key, targets, frame, info),
    }
  }

  // The whole commit precondition: one boolean, two branches. Supplying a closing lease selects the
  // narrow closing-control branch outright, which is what makes every other lane stale while closing.
  #commitPreconditionHolds(head: StoredHead, inc: string, lane: LaneId, closingLease: string | undefined): boolean {
    if (head.currentInc !== inc) return false
    return closingLease === undefined
      ? head.state === 'open'
      : lane.kind === 'control' &&
          head.state === 'closing' &&
          head.closeLease !== undefined &&
          head.closeLease.id === closingLease &&
          this.#now() <= head.closeLease.until
  }

  #advanceOrder(gen: Generation, domain: string, ttlMs: number | undefined): OrderMark {
    const now = this.#now()
    const previous = gen.order.get(domain)
    const live = previous !== undefined && !isExpired(previous, now) ? previous : undefined
    // seq is strictly increasing; the timestamp is clamped so it can never move backwards within a domain.
    const mark: OrderMark = {
      seq: (live?.seq ?? 0) + 1,
      timestamp: Math.max(now, live?.timestamp ?? 0),
      expiresAt: ttlMs === undefined ? null : now + ttlMs,
    }
    gen.order.set(domain, mark)
    return mark
  }

  #assertRetainedCapacity(gen: Generation, key: string, frame: Uint8Array): void {
    let total = frame.byteLength
    for (const [laneKeyOfEntry, entry] of gen.retained) {
      if (laneKeyOfEntry !== key) total += entry.payload.byteLength
    }
    const cap = this.capabilities.maxRetainedPayloadBytes
    if (total > cap) throw new Error(`commitLane: retained aggregate ${total} bytes exceeds the ${cap} byte cap`)
  }

  // The ordered at-most-once chain, per (incarnation, lane). The chain is gated on SETTLEMENT — success
  // or failure — so a failed frame never poisons the lane, and the returned promise rejects only on its
  // own failure.
  #enqueueAttempt(
    gen: Generation,
    key: string,
    targets: MemoryLaneSubscription[],
    frame: Uint8Array,
    info: { seq: number; timestamp: number },
  ): Promise<void> {
    const previous = gen.chains.get(key) ?? Promise.resolve()
    const attempt = previous.then(async () => {
      await Promise.all(
        targets.map(async (target) => {
          if (target.closed) return
          await target.deliver(copyBytes(frame), { ...info })
        }),
      )
    })
    gen.chains.set(
      key,
      attempt.then(
        () => {},
        () => {},
      ),
    )
    return attempt
  }

  // ── retained ──

  async readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null> {
    this.#assertLive()
    const entry = this.#rooms.get(roomId)?.gens.get(inc)?.retained.get(laneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    this.#assertLive()
    const gen = this.#rooms.get(roomId)?.gens.get(inc)
    return gen === undefined ? [] : [...gen.retained.values()].map((entry) => entry.lane)
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId): Promise<void> {
    this.#assertLive()
    const gen = this.#rooms.get(roomId)?.gens.get(inc)
    if (gen === undefined) return
    if (lane === undefined) gen.retained.clear()
    else gen.retained.delete(laneKey(lane))
  }

  // ── subscriptions ──

  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver): LaneSubscription {
    this.#assertLive()
    const room = this.#rooms.get(roomId)
    const head = this.#liveHead(room)
    const key = laneKey(lane)
    const sub: MemoryLaneSubscription = new MemoryLaneSubscription(receiver, () => {
      this.#rooms.get(roomId)?.gens.get(inc)?.subs.get(key)?.delete(sub)
    })
    if (room === undefined || head === null || head.currentInc !== inc || head.state !== 'open') {
      sub.failEstablishment(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
      return sub
    }
    // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
    const gen = this.#generation(room, inc)
    const subs = gen.subs.get(key) ?? new Set<MemoryLaneSubscription>()
    subs.add(sub)
    gen.subs.set(key, subs)
    sub.establish()
    return sub
  }

  // ── generation lifecycle ──

  async listGenerations(roomId: string): Promise<string[]> {
    this.#assertLive()
    const room = this.#rooms.get(roomId)
    if (room === undefined) return []
    this.#sweep(room)
    return [...room.gens.keys()]
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    this.#assertLive()
    const room = this.#rooms.get(roomId)
    if (room === undefined) return
    if (this.#liveHead(room)?.currentInc === inc) {
      throw new Error(`dropGeneration: refusing to drop the current incarnation '${inc}' of room '${roomId}'`)
    }
    const gen = room.gens.get(inc)
    if (gen === undefined) return // already dropped — the janitor is resumable
    for (const subs of gen.subs.values()) for (const sub of subs) sub.generationDropped()
    room.gens.delete(inc)
    this.#sweep(room)
  }

  // ── directory ──

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    this.#directory.set(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    if (this.#directory.get(roomId) === incTag) this.#directory.delete(roomId)
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    this.#assertLive()
    const matching = [...this.#directory.keys()].filter((roomId) => roomId.startsWith(prefix)).sort()
    const start = cursor === undefined ? 0 : matching.findIndex((roomId) => roomId > cursor)
    if (start < 0) return { entries: [] }
    const page = matching.slice(start, start + DIRECTORY_PAGE_SIZE)
    const entries = page.map((roomId) => ({ roomId, incTag: this.#directory.get(roomId) as string }))
    const last = page[page.length - 1]
    const more = last !== undefined && start + page.length < matching.length
    return more ? { entries, cursor: last } : { entries }
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    for (const room of this.#rooms.values()) {
      for (const gen of room.gens.values()) {
        for (const subs of gen.subs.values()) for (const sub of subs) sub.generationDropped()
      }
    }
    this.#rooms.clear()
    this.#directory.clear()
  }

  // ── internals ──

  #assertLive(): void {
    if (this.#disposed) throw new Error('MemoryRoomBackend: used after dispose()')
  }

  #roomFor(roomId: string): RoomRecord {
    const existing = this.#rooms.get(roomId)
    if (existing !== undefined) return existing
    const room: RoomRecord = { head: null, gens: new Map() }
    this.#rooms.set(roomId, room)
    return room
  }

  #generation(room: RoomRecord, inc: string): Generation {
    const existing = room.gens.get(inc)
    if (existing !== undefined) return existing
    const gen = newGeneration()
    room.gens.set(inc, gen)
    return gen
  }

  // Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch (I1).
  #liveHead(room: RoomRecord | undefined): StoredHead | null {
    if (room === undefined || room.head === null) return null
    if (!isExpired(room.head, this.#now())) return room.head
    room.head = null
    return null
  }

  // The reclaiming half of the TTL story, run on the janitor path. Reads already filter expired data, so
  // this only frees memory — it is never what makes an expired datum invisible.
  #sweep(room: RoomRecord): void {
    const now = this.#now()
    this.#liveHead(room)
    for (const gen of room.gens.values()) {
      for (const [key, cell] of gen.cells) if (isExpired(cell, now)) gen.cells.delete(key)
      for (const [domain, mark] of gen.order) if (isExpired(mark, now)) gen.order.delete(domain)
    }
  }
}
