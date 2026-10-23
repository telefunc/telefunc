export { onJoinRoom }

import { Room } from 'telefunc'

async function onJoinRoom(roomId: string) {
  return await Room.getOrCreate(`video:${roomId}`)
}
