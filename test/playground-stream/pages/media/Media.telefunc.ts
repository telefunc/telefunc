export { onJoinMeeting }

import { Room } from '@telefunc/room'
import { anyoneCanJoin } from '../../anyone-can-join'

async function onJoinMeeting(roomId: string, name: string) {
  const room = anyoneCanJoin(await Room.getOrCreate<Record<string, never>, { name: string }>(`media:${roomId}`))
  const me = await room.join({ meta: { name }, identity: name, selfDelivery: false })
  return { room, me }
}
