export { config, getRoomConfig }
export type { RoomConfig }

import { assertRoomTimeout } from '../model.js'
import { ROOM_DEPARTURE_TIMEOUT_MS, ROOM_EMPTY_TIMEOUT_MS } from './constants.js'
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
}

const globalObject = getGlobalObject<{ config: RoomConfig }>('server/config.ts', () => ({ config: {} }))

/** `Room` defaults. */
const config: RoomConfig = new Proxy({} as RoomConfig, {
  get(_target, prop) {
    return globalObject.config[prop as keyof RoomConfig]
  },
  set(_target, prop, value) {
    const configPath = `config.${String(prop)}`
    assertUsage(prop === 'emptyTimeout' || prop === 'departureTimeout', `Unknown ${configPath}`)
    assertRoomTimeout(value, configPath)
    globalObject.config[prop] = value
    return true
  },
})

function getRoomConfig(): Required<RoomConfig> {
  return {
    emptyTimeout: globalObject.config.emptyTimeout ?? ROOM_EMPTY_TIMEOUT_MS,
    departureTimeout: globalObject.config.departureTimeout ?? ROOM_DEPARTURE_TIMEOUT_MS,
  }
}
