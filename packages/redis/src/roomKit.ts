export { roomKit, setRoomKit }

import type { RoomPlaneKit } from '@telefunc/room/backend'
import { assert } from './assert.js'

/** Room's lane keys and page sizes. @telefunc/room hands them in as it builds its plane on a Redis backend
 *  (`RedisBackend.planes`), before any Room call reaches it. */
let kit: RoomPlaneKit | null = null

function setRoomKit(roomKit: RoomPlaneKit): void {
  kit = roomKit
}

function roomKit(): RoomPlaneKit {
  assert(kit)
  return kit
}
