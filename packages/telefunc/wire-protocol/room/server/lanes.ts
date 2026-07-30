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
    status: head.state,
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

  constructor(
    private readonly _onTerminal: (slot: SubSlot) => void,
    private readonly _onRecovered: () => void,
  ) {}

  get active(): boolean {
    return this._subscription !== null && this._subscription.state() !== 'closed'
  }

  get wanted(): boolean {
    return this._subscribe !== null
  }

  get ready(): Promise<void> {
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
    const previous = this._subscription
    this._unobserve?.()
    const subscription = this._subscribe()
    this._subscription = subscription
    let wasReady = subscription.state() === 'ready'
    let lostAfterReady = false
    void subscription.ready.then(
      () => {
        if (this._subscription === subscription && subscription.state() === 'ready') wasReady = true
      },
      () => {},
    )
    this._unobserve = subscription.onStateChange((state) => {
      if (this._subscription !== subscription) return
      if (state === 'lost' && wasReady) lostAfterReady = true
      else if (state === 'ready') {
        if (lostAfterReady) this._onRecovered()
        wasReady = true
        lostAfterReady = false
      } else if (state === 'closed' && wasReady) this._onTerminal(this)
    })
    if (previous) void previous.unsubscribe().catch(reportRoomError)
  }

  stop(): void {
    const subscription = this._subscription
    this._subscription = null
    this._subscribe = null
    this._unobserve?.()
    this._unobserve = null
    if (subscription) void subscription.unsubscribe().catch(reportRoomError)
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
