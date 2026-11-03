export { loadRoomServer, loadedRoomServer }
export type { RoomServer }

import { getGlobalObject } from '../utils/getGlobalObject.js'

type RoomServer = typeof import('./roomServer.js')

// Per process: a dev server's second module graph serializes the rooms the first one made.
const globalObject = getGlobalObject<{ loading?: Promise<RoomServer>; loaded?: RoomServer }>('server/load.ts', {})

/** Room's server code, imported on first use; until then a server loads only Room's entry and Cloudflare extension. */
function loadRoomServer(): Promise<RoomServer> {
  globalObject.loading ??= import('./roomServer.js').then((server) => (globalObject.loaded = server))
  return globalObject.loading
}

/** Room's server code once it is loaded; before, no room, participant or remote participant exists. */
function loadedRoomServer(): RoomServer | undefined {
  return globalObject.loaded
}
