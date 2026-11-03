export { assertRoomTimeout, roomConfig, getRoomConfig, reportDeliveryLost }
export type { RoomConfig }

import { ROOM_DEPARTURE_TIMEOUT_MS, ROOM_EMPTY_TIMEOUT_MS } from './constants.js'
import { TIMER_DELAY_MAX_MS } from '../shared/constants.js'
import { handleCallbackBug } from 'telefunc/__internal'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { assertUsage } from '../utils/assert.js'

type RoomConfig = {
  /**
   * How long, in milliseconds, a room nobody joined stays once nothing holds it, before it closes. `Infinity` keeps it
   * until `Room.close()`. A room's own `emptyTimeout` overrides it.
   *
   * @default 300000
   */
  emptyTimeout?: number
  /**
   * How long, in milliseconds, a room a member had joined stays once nothing holds it, before it closes. `Infinity`
   * keeps it until `Room.close()`. A room's own `departureTimeout` overrides it.
   *
   * @default 20000
   */
  departureTimeout?: number
  /**
   * Called when a message, a join or a leave couldn't be handed on to a server holding the room. A join or a leave
   * still took effect, as the servers read it back, and a message isn't sent again.
   *
   * @default ({ roomId, reason }) => console.error(`Room delivery handoff of '${roomId}' lost: ${reason}`)
   */
  onDeliveryLost?: (lost: { roomId: string; reason: string }) => void
}

const globalObject = getGlobalObject<{ config: RoomConfig }>('server/config.ts', () => ({ config: {} }))

/** `Room` defaults. */
const roomConfig: RoomConfig = new Proxy({} as RoomConfig, {
  get(_target, prop) {
    return globalObject.config[prop as keyof RoomConfig]
  },
  set(_target, prop, value) {
    const configPath = `roomConfig.${String(prop)}`
    if (prop === 'onDeliveryLost') {
      assertUsage(typeof value === 'function', `\`${configPath}\` should be a function`)
      globalObject.config.onDeliveryLost = value
      return true
    }
    assertUsage(prop === 'emptyTimeout' || prop === 'departureTimeout', `Unknown ${configPath}`)
    assertRoomTimeout(value, configPath)
    globalObject.config[prop] = value
    return true
  },
})

function getRoomConfig(): Required<Pick<RoomConfig, 'emptyTimeout' | 'departureTimeout'>> {
  return {
    emptyTimeout: globalObject.config.emptyTimeout ?? ROOM_EMPTY_TIMEOUT_MS,
    departureTimeout: globalObject.config.departureTimeout ?? ROOM_DEPARTURE_TIMEOUT_MS,
  }
}

/** Tells the app of a delivery Room lost: delivery is at-most-once, so nothing else hears of it. A throw in its callback
 *  is the app's bug, and doesn't fail the commit. */
function reportDeliveryLost(roomId: string, reason: string): void {
  const { onDeliveryLost = logDeliveryLost } = globalObject.config
  try {
    onDeliveryLost({ roomId, reason })
  } catch (err) {
    handleCallbackBug(err)
  }
}

function logDeliveryLost({ roomId, reason }: { roomId: string; reason: string }): void {
  console.error(`Room delivery handoff of '${roomId}' lost: ${reason}`)
}

/** Milliseconds a timer can wait, or `Infinity` for never. */
function assertRoomTimeout(value: unknown, what: string): asserts value is number {
  assertUsage(
    value === Infinity ||
      (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 && value <= TIMER_DELAY_MAX_MS),
    `\`${what}\` should be a non-negative safe integer of milliseconds, at most ${TIMER_DELAY_MAX_MS}, the longest ` +
      `a timer waits, or Infinity`,
  )
}
