export { roomKit, setRoomKit }

import type { RoomPlaneKit } from '@telefunc/room/backend'
import { assert } from './assert.js'
import { getGlobalObject } from './getGlobalObject.js'

/** Room's lane keys and page sizes. @telefunc/room hands them in as it builds its plane on a Redis backend
 *  (`RedisBackend.planes`), before any Room call reaches it. */
const globalObject = getGlobalObject<{ kit: RoomPlaneKit | null }>('roomKit.ts', { kit: null })

function setRoomKit(roomKit: RoomPlaneKit): void {
  globalObject.kit = roomKit
}

function roomKit(): RoomPlaneKit {
  assert(globalObject.kit)
  return globalObject.kit
}
