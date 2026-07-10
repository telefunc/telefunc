export { onRefIdentity }

import { BroadcastChannel, Channel } from 'telefunc'

type RoomMsg = { start: true } | { n: number }

/** Returns the same live values under several keys — the client asserts each revives
 *  as one object, with exactly one wire subscription behind it. */
async function onRefIdentity() {
  // Unique key per call so broadcast seq state doesn't leak across page loads.
  const room = new BroadcastChannel<RoomMsg>({ key: `ref-identity:${crypto.randomUUID()}` })
  room.subscribe(async (msg) => {
    // Reacts to the client's `{ start: true }`; guards against its own `{ n }` self-echo.
    if ('start' in msg) {
      for (let n = 1; n <= 3; n++) await room.publish({ n })
    }
  })

  const ch = new Channel<(msg: string) => void, (msg: string) => void>()
  ch.listen((msg) => {
    if (msg === 'ping') ch.send('pong')
  })

  const gen = (async function* () {
    yield 1
    yield 2
  })()

  return { room, roomDupe: room, list: [room], ch, chDupe: ch, gen, genDupe: gen }
}
