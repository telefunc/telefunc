import { Room } from '@telefunc/room'

export async function onGetLobby() {
  const room = await Room.getOrCreate<{}, { name: string }, { text: string }>('lobby')
  Room.guard(room, { anyoneCanJoin: true })
  return room
}
