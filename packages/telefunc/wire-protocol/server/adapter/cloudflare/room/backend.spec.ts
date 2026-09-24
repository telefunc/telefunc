import { expect, test, vi } from 'vitest'
import '../../../../../node/server/async_hooks.js'
import {
  CloudflareRoomBackend,
  CloudflareRoomSessionManager,
  type CloudflareRoomAuthorityStub,
  type CloudflareRoomNamespace,
} from './backend.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { withCloudflareSession } from '../session.js'

const noBroadcast = () => {
  throw new Error('this spec uses no Broadcast')
}
const broadcast = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', namespace: noBroadcast })

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
  const backend = new CloudflareRoomBackend({ rooms: () => rooms, broadcast })
  const manager = new CloudflareRoomSessionManager('session')
  const commit = (text: string) =>
    backend.commitLane('room', 'inc', { kind: 'semantic' }, new TextEncoder().encode(text))
  await withCloudflareSession({ room: () => manager, broadcast: noBroadcast }, () =>
    Promise.all([commit('first'), commit('second')]),
  )
  expect(arrived).toEqual(['first', 'second'])
})

test('outside a session, a commit goes through a stub of its own, and a stale answer comes back as it is', async () => {
  const stale = { stale: 'head' }
  const rooms = { idFromName: (name: string) => name, get: () => ({ commitLane: async () => stale }) }
  const backend = new CloudflareRoomBackend({ rooms: () => rooms as unknown as CloudflareRoomNamespace, broadcast })
  await expect(backend.commitLane('room', 'inc', { kind: 'semantic' }, new Uint8Array())).resolves.toBe(stale)
})

test('a session delivers a frame for the lease its subscription holds, and drops one for another lease', async () => {
  const manager = new CloudflareRoomSessionManager('session')
  const received: number[] = []
  const authority = { registerRoute: async () => ({ ok: true }), unsubscribeRoute: async () => {} }
  const attempt = manager.openSubscription(
    { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } },
    authority as unknown as CloudflareRoomAuthorityStub,
    (payload) => void received.push(payload[0]!),
  )
  await vi.waitFor(() => expect(attempt.state()).toBe('ready'))
  const route = { roomId: 'room', inc: 'inc', laneKey: encodeLaneKey({ kind: 'semantic' }), sessionDoId: 'session' }
  const frame = (byte: number, leaseId: string) => ({
    ...route,
    leaseId,
    payload: new Uint8Array([byte]),
    seq: byte,
    timestamp: 1,
  })
  await manager.deliver(frame(1, attempt.leaseId))
  // A lease this session held before a restart.
  await manager.deliver(frame(2, 'stale-lease'))
  expect(received).toEqual([1])
  await attempt.unsubscribe()
})
