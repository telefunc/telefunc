export { testRedisRoomCrossInstance, testRedisBroadcastCrossInstance }

import { autoRetry, expect, getServerUrl, test } from '@brillout/test-e2e'

function testRedisRoomCrossInstance() {
  test('room: installRedis alone delivers a publish from instance A to a joined instance B', async () => {
    const roomId = `redis-cross-instance-${Date.now()}-${Math.random().toString(36).slice(2)}`
    const message = { kind: 'from-a', nonce: Math.random().toString(36).slice(2) }
    try {
      await autoRetry(
        async () => {
          expect(await request('a', '/api/room-cross-instance/join', roomId, 'POST')).to.deep.equal({
            ok: true,
            instance: 'A',
          })
        },
        { timeout: 30_000 },
      )
      await autoRetry(
        async () => {
          expect(await request('b', '/api/room-cross-instance/join', roomId, 'POST')).to.deep.equal({
            ok: true,
            instance: 'B',
          })
        },
        { timeout: 30_000 },
      )
      expect(await request('a', '/api/room-cross-instance/publish', roomId, 'POST', message)).to.deep.equal({
        ok: true,
        instance: 'A',
      })

      await autoRetry(
        async () => {
          const observed = await request('b', '/api/room-cross-instance/received', roomId)
          expect(observed).to.deep.equal({ ok: true, instance: 'B', received: [message] })
        },
        { timeout: 10_000 },
      )
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
      await autoRetry(
        async () => {
          expect(await broadcastRequest('b', '/api/broadcast-cross-instance/subscribe', key, 'POST')).to.deep.equal({
            ok: true,
            instance: 'B',
          })
        },
        { timeout: 30_000 },
      )
      expect(
        await broadcastRequest('a', '/api/broadcast-cross-instance/publish', key, 'POST', message),
      ).to.deep.include({
        ok: true,
        instance: 'A',
      })

      await autoRetry(
        async () => {
          const observed = await broadcastRequest('b', '/api/broadcast-cross-instance/received', key)
          expect(observed).to.deep.equal({ ok: true, instance: 'B', received: [message] })
        },
        { timeout: 10_000 },
      )
    } finally {
      await broadcastRequest('b', '/api/broadcast-cross-instance/unsubscribe', key, 'DELETE').catch(() => {})
    }
  })
}

async function request(
  instance: 'a' | 'b',
  pathname: string,
  roomId: string,
  method: 'GET' | 'POST' | 'DELETE' = 'GET',
  body?: unknown,
): Promise<Record<string, unknown>> {
  const url = new URL(pathname, getServerUrl())
  url.searchParams.set('bench_instance', instance)
  url.searchParams.set('roomId', roomId)
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
  instance: 'a' | 'b',
  pathname: string,
  key: string,
  method: 'GET' | 'POST' | 'DELETE' = 'GET',
  body?: unknown,
): Promise<Record<string, unknown>> {
  const url = new URL(pathname, getServerUrl())
  url.searchParams.set('bench_instance', instance)
  url.searchParams.set('key', key)
  const response = await fetch(url, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const payload = (await response.json()) as Record<string, unknown>
  if (!response.ok) throw new Error(`${method} ${pathname} failed (${response.status}): ${JSON.stringify(payload)}`)
  return payload
}
