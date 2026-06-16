export { onJoinRoom }

import { BroadcastChannel } from 'telefunc'

async function onJoinRoom(roomId: string) {
  const room = new BroadcastChannel({ key: `video:${roomId}` })
  return room
}
