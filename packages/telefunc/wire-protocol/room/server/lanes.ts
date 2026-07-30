export {
  CONTROL_LANE,
  SEMANTIC_LANE,
  SubSlot,
  commitRoomLane,
  configFromHead,
  decodeRoomText,
  encodeRoomConfig,
  encodeRoomText,
  publishCtrl,
  reportRoomError,
  withinRoomHorizon,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getBackend } from '../../backend/install.js'
import type { BackendSubscription, CommitAccepted, LaneId, RoomHead } from '../../backend/spi.js'
import type { RoomConfigRecord, RoomCtrlEnvelope } from '../protocol.js'
import { RoomError } from '../protocol.js'

const roomTextEncoder = new TextEncoder()
const roomTextDecoder = new TextDecoder()
const SEMANTIC_LANE = { kind: 'semantic' } as const satisfies LaneId
const CONTROL_LANE = { kind: 'control' } as const satisfies LaneId

function encodeRoomText(value: string): Uint8Array {
  return roomTextEncoder.encode(value)
}

function decodeRoomText(value: Uint8Array): string {
  return roomTextDecoder.decode(value)
}

function encodeRoomConfig(config: RoomConfigRecord): Uint8Array {
  return encodeRoomText(stringify(config))
}

function configFromHead(head: RoomHead): RoomConfigRecord {
  const stored = parse(decodeRoomText(head.config)) as RoomConfigRecord
  return {
    ...stored,
    ...(head.currentInc === null ? {} : { inc: head.currentInc }),
  }
}

async function commitRoomLane(
  id: string,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
): Promise<CommitAccepted | null> {
  const result = await getBackend().commitLane(id, inc, lane, payload, opts)
  if ('stale' in result) return null
  await result.delivery
  return result
}

class SubSlot {
  private _subscription: BackendSubscription | null = null
  private _subscribe: (() => BackendSubscription) | null = null
  private _unobserve: (() => void) | null = null
  private _readyPromise = Promise.resolve()
  private _resolveReady: (() => void) | null = null
  private _generationPromise = Promise.resolve()
  private _resolveGeneration: (() => void) | null = null
  private _rejectGeneration: ((error: unknown) => void) | null = null

  constructor(
    private readonly _onTerminal: (slot: SubSlot, error?: unknown) => void,
    private readonly _onRecovered: () => void,
  ) {}

  get active(): boolean {
    return this._subscription !== null && this._subscription.state() !== 'closed'
  }

  get established(): boolean {
    return this._subscription?.state() === 'ready'
  }

  get wanted(): boolean {
    return this._subscribe !== null
  }

  /** Holder-facing readiness survives raw attempt failures; Room's policy owns replacement. */
  get ready(): Promise<void> {
    return this._readyPromise
  }

  /** Internal recovery generation. Unlike holder readiness, exhaustion rejects this generation so
   *  internal planners can abandon it; a later retry creates a fresh one. */
  get generationReady(): Promise<void> {
    return this._generationPromise
  }

  /** The current raw generation, used only by Room's bounded recovery policy. */
  get attemptReady(): Promise<void> {
    return this._subscription?.ready ?? Promise.resolve()
  }

  sync(want: boolean, subscribe: () => BackendSubscription): void {
    if (!want) return this.stop()
    this._subscribe = subscribe
    if (this._subscription !== null && this._subscription.state() !== 'closed') return
    this.retry()
  }

  retry(): void {
    if (this._subscribe === null) return
    this._ensurePendingReady()
    this._ensurePendingGeneration()
    const previous = this._subscription
    this._unobserve?.()
    const subscription = this._subscribe()
    this._subscription = subscription
    let terminalNotified = false
    const notifyTerminal = (error?: unknown) => {
      if (terminalNotified) return
      terminalNotified = true
      this._onTerminal(this, error)
    }
    let wasReady = subscription.state() === 'ready'
    if (wasReady) this._settleReady()
    let lostAfterReady = false
    void subscription.ready.then(
      () => {
        if (this._subscription === subscription && subscription.state() === 'ready') {
          wasReady = true
          this._settleReady()
        }
      },
      (error: unknown) => {
        if (this._subscription !== subscription) return
        this._ensurePendingReady()
        this._ensurePendingGeneration()
        notifyTerminal(error)
      },
    )
    this._unobserve = subscription.onStateChange((state) => {
      if (this._subscription !== subscription) return
      if (state === 'lost') {
        if (wasReady) lostAfterReady = true
        this._ensurePendingReady()
        this._ensurePendingGeneration()
      } else if (state === 'ready') {
        if (lostAfterReady) this._onRecovered()
        wasReady = true
        lostAfterReady = false
        this._settleReady()
      } else if (state === 'closed') {
        this._ensurePendingReady()
        this._ensurePendingGeneration()
        notifyTerminal()
      }
    })
    if (subscription.state() === 'closed') notifyTerminal()
    if (previous) void previous.unsubscribe().catch(reportRoomError)
  }

  /** Exhausted policy keeps demand and holder readiness pending, but rejects this internal
   *  generation and drops the dead raw attempt until the next planning pass. */
  markLost(error: unknown): void {
    const subscription = this._subscription
    this._subscription = null
    this._unobserve?.()
    this._unobserve = null
    this._rejectGeneration?.(error)
    this._resolveGeneration = null
    this._rejectGeneration = null
    if (subscription) void subscription.unsubscribe().catch(reportRoomError)
  }

  stop(): void {
    const subscription = this._subscription
    this._subscription = null
    this._subscribe = null
    this._unobserve?.()
    this._unobserve = null
    this._settleReady()
    this._readyPromise = Promise.resolve()
    this._generationPromise = Promise.resolve()
    if (subscription) void subscription.unsubscribe().catch(reportRoomError)
  }

  private _ensurePendingReady(): void {
    if (this._resolveReady !== null) return
    this._readyPromise = new Promise<void>((resolve) => {
      this._resolveReady = resolve
    })
  }

  private _ensurePendingGeneration(): void {
    if (this._resolveGeneration !== null) return
    this._generationPromise = new Promise<void>((resolve, reject) => {
      this._resolveGeneration = resolve
      this._rejectGeneration = reject
    })
    // A generation can exhaust without an active internal waiter. Keep that deliberate rejection
    // from becoming an unhandled rejection while preserving it for callers that captured the promise.
    void this._generationPromise.catch(() => {})
  }

  private _settleReady(): void {
    this._resolveReady?.()
    this._resolveReady = null
    this._resolveGeneration?.()
    this._resolveGeneration = null
    this._rejectGeneration = null
  }
}

function withinRoomHorizon<T>(promise: Promise<T>, ms: number): Promise<T> {
  if (ms <= 0) return Promise.reject(new RoomError('Room subscription recovery horizon expired'))
  let timer!: ReturnType<typeof setTimeout>
  const timeout = new Promise<never>((_, reject) => {
    timer = unrefTimer(setTimeout(() => reject(new RoomError('Room subscription recovery horizon expired')), ms))
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

async function publishCtrl(roomId: string, inc: string, event: RoomCtrlEnvelope): Promise<void> {
  const committed = await commitRoomLane(roomId, inc, CONTROL_LANE, encodeRoomText(stringify(event)))
  if (committed === null) throw new RoomError(`Room is closed: ${roomId}`)
}

function reportRoomError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}
