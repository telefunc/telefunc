export { loadRoomServer, loadedRoomServer }
export type { RoomServer }

import { getGlobalObject } from '../utils/getGlobalObject.js'

type RoomServer = typeof import('./roomServer.js')

// Per process: a dev server's second module graph serializes the rooms the first one made.
declare module '../utils/getGlobalObject.js' {
  interface GlobalObjects {
    'server/load.ts': { loading?: Promise<RoomServer>; loaded?: RoomServer }
  }
}
const globalObject = getGlobalObject('server/load.ts', () => ({}))

/** Room's server code, imported on first use; until then a server loads only Room's entry and Cloudflare extension. */
function loadRoomServer(): Promise<RoomServer> {
  globalObject.loading ??= importRoomServer()
  return globalObject.loading
}

async function importRoomServer(): Promise<RoomServer> {
  const server = await import('./roomServer.js')
  globalObject.loaded = server
  return server
}

/** Room's server code once it is loaded; before, no room, participant or remote participant exists. */
function loadedRoomServer(): RoomServer | undefined {
  return globalObject.loaded
}
