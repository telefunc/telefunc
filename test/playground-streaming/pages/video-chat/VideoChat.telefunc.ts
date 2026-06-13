export { onJoinRoom }

import { Broadcast } from 'telefunc'

async function onJoinRoom(roomId: string) {
  const room = new Broadcast({ key: `video:${roomId}` })
  return room
}
