export { publishMine }

import type { LocalParticipant } from '@telefunc/room'

/** `me`'s message, frame on `cam` and lossy frame on `cursor`, each of which `onWatchRoomTrack()`'s room receives. */
async function publishMine(me: Pick<LocalParticipant, 'publish' | 'publishBinary'>) {
  await me.publish('mine')
  await me.publishBinary(new Uint8Array([7]), { track: 'cam' })
  await me.publishBinary(new Uint8Array([7]), { track: 'cursor', lossy: true })
}
