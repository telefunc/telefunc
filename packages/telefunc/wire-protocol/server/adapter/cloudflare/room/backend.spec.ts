import { expect, test } from 'vitest'
import '../../../../../node/server/async_hooks.js'
import { CloudflareRoomBackend, CloudflareRoomSessionManager, type CloudflareRoomNamespace } from './backend.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { withCloudflareSession } from '../session.js'

test("a session's commits to a room reach its authority in the order Room sent them", async () => {
  const arrived: string[] = []
  let opened = 0
  // Like Cloudflare's stubs: calls through one stub arrive in call order, calls through different stubs race; the
  // first stub opened is the slowest.
  const rooms = {
    idFromName: (name: string) => name,
    get: () => {
      const latency = opened++ === 0 ? 20 : 0
      let arrival = Promise.resolve()
      return {
        commitLane(_inc: string, _lane: unknown, payload: Uint8Array) {
          arrival = arrival.then(() => new Promise((resolve) => setTimeout(resolve, latency)))
          return arrival.then(() => {
            arrived.push(new TextDecoder().decode(payload))
            return { seq: arrived.length, timestamp: 1, receivers: 0, deliveryToken: 'token' }
          })
        },
        awaitDelivery: async () => {},
      }
    },
  } as unknown as CloudflareRoomNamespace
  const noBroadcast = () => {
    throw new Error('this spec uses no Broadcast')
  }
  const backend = new CloudflareRoomBackend({
    rooms: () => rooms,
    broadcast: new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', namespace: noBroadcast }),
  })
  const manager = new CloudflareRoomSessionManager('session')
  const commit = (text: string) =>
    backend.commitLane('room', 'inc', { kind: 'semantic' }, new TextEncoder().encode(text))
  await withCloudflareSession({ room: () => manager, broadcast: noBroadcast }, () =>
    Promise.all([commit('first'), commit('second')]),
  )
  expect(arrived).toEqual(['first', 'second'])
})
