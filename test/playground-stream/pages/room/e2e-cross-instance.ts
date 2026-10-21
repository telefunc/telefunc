export {
  testRedisRoomCrossInstance,
  testRedisBroadcastCrossInstance,
  testRedisOutages,
  testRedisAcrossServers,
  testServerCrash,
  testRedisClusterReshard,
}

import { autoRetry, expect, expectLog, getServerUrl, test } from '@brillout/test-e2e'
import {
  dropRedisConnections,
  getRedisChannels,
  getRedisKeys,
  killInstance,
  moveRedisSlot,
  restartRedis,
  rewindRedis,
  snapshotRedis,
  startInstance,
} from '../../e2e-utils'

type Instance = 'a' | 'b' | 'c' | 'd'
type RoomView = {
  count: number
  members: Record<string, Record<string, unknown>>
  stored: Record<string, Record<string, unknown>>
  meta: Record<string, unknown>
  frames: string[]
  events: string[]
}

function testRedisRoomCrossInstance() {
  test('room: installRedis alone delivers a publish from instance A to a joined instance B', async () => {
    const roomId = `redis-cross-instance-${Date.now()}-${Math.random().toString(36).slice(2)}`
    const message = { kind: 'from-a', nonce: Math.random().toString(36).slice(2) }
    try {
      await retry(30_000, async () => {
        expect(await request('a', '/api/room-cross-instance/join', roomId, 'POST')).to.deep.equal({
          ok: true,
          instance: 'A',
        })
      })
      await retry(30_000, async () => {
        expect(await request('b', '/api/room-cross-instance/join', roomId, 'POST')).to.deep.equal({
          ok: true,
          instance: 'B',
        })
      })
      expect(await request('a', '/api/room-cross-instance/publish', roomId, 'POST', message)).to.deep.equal({
        ok: true,
        instance: 'A',
      })

      await retry(10_000, async () => {
        const observed = await request('b', '/api/room-cross-instance/received', roomId)
        expect(observed).to.deep.equal({ ok: true, instance: 'B', received: [message] })
      })
    } finally {
      await Promise.allSettled([
        request('a', '/api/room-cross-instance/leave', roomId, 'DELETE'),
        request('b', '/api/room-cross-instance/leave', roomId, 'DELETE'),
      ])
    }
  })
}

function testRedisBroadcastCrossInstance() {
  test('broadcast: Redis Cluster delivers a generic publish from instance A to instance B', async () => {
    const key = `redis-cluster-broadcast-${Date.now()}-${Math.random().toString(36).slice(2)}`
    const message = { kind: 'generic-from-a', nonce: Math.random().toString(36).slice(2) }
    try {
      await retry(30_000, async () => {
        expect(await broadcastRequest('b', '/api/broadcast-cross-instance/subscribe', key, 'POST')).to.deep.equal({
          ok: true,
          instance: 'B',
        })
      })
      expect(
        await broadcastRequest('a', '/api/broadcast-cross-instance/publish', key, 'POST', message),
      ).to.deep.include({
        ok: true,
        instance: 'A',
      })

      await retry(10_000, async () => {
        const observed = await broadcastRequest('b', '/api/broadcast-cross-instance/received', key)
        expect(observed).to.deep.equal({ ok: true, instance: 'B', received: [message] })
      })
    } finally {
      await broadcastRequest('b', '/api/broadcast-cross-instance/unsubscribe', key, 'DELETE').catch(() => {})
    }
  })
}

function testRedisOutages() {
  test("room: after a failover rewinds Redis, members on two servers get each other's messages again, in order, and see the next join and meta change", async () => {
    const roomId = newId('redis-rewind')
    try {
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')
      await publish('a', roomId, 'm1')
      await retry(10_000, async () => {
        expect(await received('b', roomId)).deep.equal(['m1'])
      })

      const snapshot = await snapshotRedis()
      // What the failover loses.
      await joinRoom('c', roomId, 'Carol')
      await setMeta('b', roomId, { score: 1 })
      await publish('a', roomId, 'lost')
      await rewindRedis(snapshot)

      await expectDeliveryResumes(roomId)
      await joinRoom('d', roomId, 'Dave')
      await setMeta('b', roomId, { score: 2 })
      await retry(30_000, async () => {
        for (const instance of ['a', 'b', 'd'] as const) {
          const { members } = await view(instance, roomId)
          expect(Object.keys(members)).to.include.members(['Alice', 'Bob', 'Dave'])
          expect(members.Bob!.score).toBe(2)
        }
      })
      // Each server reports the outage of its subscriber connection.
      expectLog('Redis subscriber connection closed', { filter: (log) => log.logSource === 'stderr' })
    } finally {
      await leaveRoom(['a', 'b', 'c', 'd'], roomId)
    }
  })

  test('room: when Redis restarts without its data, the servers holding the room are told it closed, and a room created again under its id delivers', async () => {
    const roomId = newId('redis-restart')
    try {
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')

      restartRedis()
      await retry(30_000, async () => {
        for (const instance of ['a', 'b'] as const) expect((await view(instance, roomId)).events).contains('close')
      })
      // Each server reports the room's subscriptions ending with its data.
      expectLog('Backend subscription closed', { filter: (log) => log.logSource === 'stderr' })

      await leaveRoom(['a', 'b'], roomId)
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')
      await publish('a', roomId, 'after')
      await retry(10_000, async () => {
        expect(await received('b', roomId)).deep.equal(['after'])
      })
      // Each server reports the outage of its subscriber connection.
      expectLog('Redis subscriber connection closed', { filter: (log) => log.logSource === 'stderr' })
    } finally {
      await leaveRoom(['a', 'b'], roomId)
    }
  })

  test("room and broadcast: after every Redis connection drops, servers get each other's messages again, in order, and a key nobody subscribes to any more holds no Redis subscription", async () => {
    const roomId = newId('redis-drop')
    const key = newId('redis-drop-key')
    try {
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')
      await broadcastRequest('b', '/api/broadcast-cross-instance/subscribe', key, 'POST')

      await dropRedisConnections()
      await expectDeliveryResumes(roomId)
      // Delivery is at-most-once while the connection comes back.
      await retry(30_000, async () => {
        await broadcastRequest('a', '/api/broadcast-cross-instance/publish', key, 'POST', 'probe')
        expect(await broadcastReceived('b', key)).contains('probe')
      })
      for (const message of ['k1', 'k2', 'k3']) {
        await broadcastRequest('a', '/api/broadcast-cross-instance/publish', key, 'POST', message)
      }
      await retry(10_000, async () => {
        expect((await broadcastReceived('b', key)).filter((message) => message !== 'probe')).deep.equal([
          'k1',
          'k2',
          'k3',
        ])
      })

      await broadcastRequest('b', '/api/broadcast-cross-instance/unsubscribe', key, 'DELETE')
      await retry(10_000, async () => {
        expect(await getRedisChannels(`*${encodeURIComponent(key)}*`)).deep.equal([])
      })
      // Each server reports the outage of its subscriber connection.
      expectLog('Redis subscriber connection closed', { filter: (log) => log.logSource === 'stderr' })
    } finally {
      await leaveRoom(['a', 'b'], roomId)
      await broadcastRequest('b', '/api/broadcast-cross-instance/unsubscribe', key, 'DELETE').catch(() => {})
    }
  })
}

function testRedisAcrossServers() {
  test("broadcast: a key's binary frames reach another server at once after a text publish on it", async () => {
    const key = newId('redis-kinds')
    try {
      await broadcastRequest('b', '/api/broadcast-cross-instance/subscribe', key, 'POST')
      const start = Date.now()
      await broadcastRequest('a', '/api/broadcast-cross-instance/publish', key, 'POST', 'text')
      for (const n of [1, 2, 3, 4, 5]) {
        await broadcastRequest('a', '/api/broadcast-cross-instance/publish?kind=binary', key, 'POST', n)
      }
      await retry(10_000, async () => {
        expect(await broadcastReceived('b', key)).deep.equal(['text', ...[1, 2, 3, 4, 5].map((n) => `binary:${n}`)])
      })
      // A frame held for a missing predecessor waits one second.
      expect(Date.now() - start).lessThan(800)
    } finally {
      await broadcastRequest('b', '/api/broadcast-cross-instance/unsubscribe', key, 'DELETE').catch(() => {})
    }
  })

  test('room: 1,000 members joining at once through four servers all get in, and every server counts them', async () => {
    const roomId = newId('redis-joins')
    const instances = ['a', 'b', 'c', 'd'] as const
    try {
      for (const instance of instances) await joinRoom(instance, roomId, instance.toUpperCase())
      const joined = await Promise.all(
        instances.map((instance) =>
          request(instance, `/api/room-cross-instance/join-many?count=249&name=${instance}`, roomId, 'POST'),
        ),
      )
      expect(joined.map((result) => result.joined)).deep.equal([249, 249, 249, 249])
      await retry(30_000, async () => {
        for (const instance of instances) expect((await view(instance, roomId)).count).toBe(1000)
      })
    } finally {
      await leaveRoom([...instances], roomId)
    }
  })

  test("room: members on two servers see each other's meta, the room's meta, frames and wants alike", async () => {
    const roomId = newId('redis-agree')
    try {
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')
      // Both servers write the room's meta and their member's at once.
      await Promise.all([
        ...Array.from({ length: 20 }, (_, i) => setMeta(i % 2 ? 'a' : 'b', roomId, { [`k${i}`]: i }, 'room')),
        ...Array.from({ length: 10 }, (_, i) => setMeta('a', roomId, { step: i })),
        ...Array.from({ length: 10 }, (_, i) => setMeta('b', roomId, { step: i })),
      ])
      await retry(30_000, async () => {
        const [onA, onB] = await Promise.all([view('a', roomId), view('b', roomId)])
        expect(Object.entries(onB.meta)).deep.equal(Object.entries(onA.meta))
        for (let i = 0; i < 20; i++) expect(onA.meta[`k${i}`]).toBe(i)
        expect(onB.members).deep.equal(onA.members)
        expect(onA.members).deep.equal(onA.stored)
      })

      // Bob's server watches the cam track of every member: Alice's server learns it is wanted.
      await request('b', '/api/room-cross-instance/watch?track=cam', roomId, 'POST')
      await retry(30_000, async () => {
        expect((await view('a', roomId)).events).contains('demand:cam:true')
      })
      await request('a', '/api/room-cross-instance/watch?track=cam', roomId, 'POST')
      for (const [instance, n] of [
        ['a', 1],
        ['b', 2],
        ['a', 3],
        ['b', 4],
      ] as const) {
        await request(instance, '/api/room-cross-instance/publish-binary?track=cam', roomId, 'POST', n)
      }
      await retry(10_000, async () => {
        for (const instance of ['a', 'b'] as const) {
          const { frames } = await view(instance, roomId)
          expect(frames).deep.equal(['cam:Alice:1', 'cam:Bob:2', 'cam:Alice:3', 'cam:Bob:4'])
        }
      })
    } finally {
      await leaveRoom(['a', 'b'], roomId)
    }
  })

  test('room: once a room closes, every key it kept in Redis expires', async () => {
    const roomId = newId('redis-close')
    try {
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')
      await publish('a', roomId, 'kept', true)
      await request('b', '/api/room-cross-instance/watch?track=cam', roomId, 'POST')
      await request('a', '/api/room-cross-instance/publish-binary?track=cam&retain=true', roomId, 'POST', 1)
      await request('b', '/api/room-cross-instance/join-many?count=20&name=guest', roomId, 'POST')
      await setMeta('a', roomId, { topic: 'closing' }, 'room')
      await leaveRoom(['b'], roomId)
      await request('a', '/api/room-cross-instance/close', roomId, 'POST')
      const keys = await getRedisKeys(`*{${encodeURIComponent(roomId)}}*`)
      expect(keys.length).greaterThan(0)
      expect(keys.filter(({ ttl }) => ttl < 0).map(({ key }) => key)).deep.equal([])
    } finally {
      await leaveRoom(['a', 'b'], roomId)
    }
  })
}

function testRedisClusterReshard() {
  test("room: a member's messages and frames reach another server in the order it published them while the room moves to another Cluster node", async () => {
    const roomId = newId('redis-reshard')
    try {
      await joinRoom('a', roomId, 'Alice')
      await joinRoom('b', roomId, 'Bob')
      await request('b', '/api/room-cross-instance/watch?track=cam', roomId, 'POST')
      await retry(30_000, async () => {
        expect((await view('a', roomId)).events).contains('demand:cam:true')
      })
      let moved = false
      // Every key of a room shares its id's hash tag, so this moves the whole room.
      const moving = moveRedisSlot(`{${encodeURIComponent(roomId)}}`).finally(() => {
        moved = true
      })
      let published = 0
      do {
        await request(
          'a',
          `/api/room-cross-instance/publish-many?from=${published + 1}&count=20&track=cam`,
          roomId,
          'POST',
        )
        published += 20
      } while (!moved)
      expect((await moving).moved).greaterThan(0)

      const calls = Array.from({ length: published }, (_, i) => i + 1)
      await retry(30_000, async () => {
        expect(await received('b', roomId)).deep.equal(calls)
        expect((await view('b', roomId)).frames).deep.equal(calls.map((n) => `cam:Alice:${n % 256}`))
      })
    } finally {
      await leaveRoom(['a', 'b'], roomId)
    }
  })
}

function testServerCrash() {
  test("room: within three minutes of a server crashing, its member leaves the other servers' view as 'disconnected', and the room it alone held closes", async () => {
    const roomId = newId('redis-crash')
    try {
      await joinRoom('a', roomId, 'Alice', 1000)
      await joinRoom('b', roomId, 'Bob')
      // Alice's server keeps watching the room Bob alone holds.
      await request('a', '/api/room-cross-instance/member-leave', roomId, 'POST')
      killInstance('b')
      await retry(3 * 60_000 + 10_000, async () => {
        expect((await view('a', roomId)).events).to.include.members(['leave:Bob:disconnected', 'close'])
      })
    } finally {
      startInstance('b')
      await retry(30_000, async () => {
        await request('b', '/api/room-cross-instance/leave', roomId, 'DELETE')
      })
    }
  })
}

/** Once Redis is back, a member's publishes reach the other server again, then arrive there in order, once.
 *  Delivery is at-most-once until then, so the publisher repeats a probe. */
async function expectDeliveryResumes(roomId: string) {
  await retry(30_000, async () => {
    await publish('a', roomId, 'probe')
    expect(await received('b', roomId)).contains('probe')
  })
  for (const message of ['m2', 'm3', 'm4']) await publish('a', roomId, message)
  await retry(10_000, async () => {
    const after = await received('b', roomId)
    expect(after.slice(after.lastIndexOf('probe') + 1)).deep.equal(['m2', 'm3', 'm4'])
  })
}

function newId(label: string) {
  return `${label}-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

async function joinRoom(instance: Instance, roomId: string, name: string, departureTimeout?: number) {
  const query = `&name=${name}${departureTimeout === undefined ? '' : `&departureTimeout=${departureTimeout}`}`
  await retry(30_000, async () => {
    expect(await request(instance, `/api/room-cross-instance/join?${query}`, roomId, 'POST')).deep.include({ ok: true })
  })
}

async function publish(instance: Instance, roomId: string, message: unknown, retain = false) {
  await request(instance, `/api/room-cross-instance/publish?retain=${retain}`, roomId, 'POST', message)
}

async function setMeta(
  instance: Instance,
  roomId: string,
  attributes: Record<string, unknown>,
  target: 'member' | 'room' = 'member',
) {
  await request(instance, `/api/room-cross-instance/meta?target=${target}`, roomId, 'POST', attributes)
}

async function received(instance: Instance, roomId: string): Promise<unknown[]> {
  return (await request(instance, '/api/room-cross-instance/received', roomId)).received as unknown[]
}

async function broadcastReceived(instance: Instance, key: string): Promise<unknown[]> {
  return (await broadcastRequest(instance, '/api/broadcast-cross-instance/received', key)).received as unknown[]
}

async function view(instance: Instance, roomId: string): Promise<RoomView> {
  return (await request(instance, '/api/room-cross-instance/view', roomId)) as unknown as RoomView
}

async function leaveRoom(instances: Instance[], roomId: string) {
  await Promise.allSettled(
    instances.map((instance) => request(instance, '/api/room-cross-instance/leave', roomId, 'DELETE')),
  )
}

function retry(timeout: number, assertion: () => Promise<void>): Promise<void> {
  return autoRetry(assertion, { timeout })
}

async function request(
  instance: Instance,
  pathname: string,
  resourceId: string,
  method: 'GET' | 'POST' | 'DELETE' = 'GET',
  body?: unknown,
  queryName: 'roomId' | 'key' = 'roomId',
): Promise<Record<string, unknown>> {
  const url = new URL(pathname, getServerUrl())
  url.searchParams.set('bench_instance', instance)
  url.searchParams.set(queryName, resourceId)
  const response = await fetch(url, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const payload = (await response.json()) as Record<string, unknown>
  if (!response.ok) throw new Error(`${method} ${pathname} failed (${response.status}): ${JSON.stringify(payload)}`)
  return payload
}

async function broadcastRequest(
  instance: Instance,
  pathname: string,
  key: string,
  method: 'GET' | 'POST' | 'DELETE' = 'GET',
  body?: unknown,
): Promise<Record<string, unknown>> {
  return request(instance, pathname, key, method, body, 'key')
}
