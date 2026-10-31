export { onGetCall, onJoinCall, onSwitchHere }

import { Room } from '@telefunc/room'

const callId = 'video-chat'

async function onGetCall() {
  return await Room.getOrCreate<Record<string, never>, { name: string }, { text: string }>(callId)
}

async function onJoinCall(name: string) {
  const call = await onGetCall()
  return await call.join({ meta: { name }, identity: name })
}

async function onSwitchHere(name: string) {
  await Room.removeParticipant(callId, { identity: name, reason: 'moved to another tab' })
  return await onJoinCall(name)
}
