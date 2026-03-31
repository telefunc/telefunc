export { onJoinRoom }

import { pubsub } from 'telefunc'

async function onJoinRoom(room: string) {
  const ps = pubsub(`video:${room}`)
  return ps
}
