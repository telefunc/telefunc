// Real-Redis certification for the released generic broadcast transport. The fake-backed unit spec
// proves the Lua and frame codec; this proves a real SUBSCRIBE acknowledgement and the unchanged
// PUBLISH_CMD path across separate Redis connections. Gated because it flushes the target database:
//
//   docker run --rm -p 6379:6379 redis
//   TELEFUNC_TEST_REAL_REDIS=redis://localhost:6379 \
//     pnpm exec vitest run packages/redis/src/index.integration.spec.ts
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { Redis } from 'ioredis'
import { DefaultBroadcastAdapter, type BroadcastAdapter } from 'telefunc'
import { Worker } from 'node:worker_threads'
import { RedisTransport } from './index.js'

const REDIS_URL = process.env.TELEFUNC_TEST_REAL_REDIS

type Received = { payload: string; seq: number; timestamp: number }

describe.skipIf(!REDIS_URL)('Redis broadcast transport against a live server', () => {
  let redis: Redis
  let adapter: BroadcastAdapter

  beforeAll(() => {
    redis = new Redis(REDIS_URL!)
    adapter = new DefaultBroadcastAdapter(new RedisTransport({ redis }))
  })
  afterEach(async () => {
    await redis.flushdb()
  })
  afterAll(async () => {
    await redis.quit()
  })

  it('delivers generic publishes across Redis with the order assigned by PUBLISH_CMD', async () => {
    const received: Received[] = []
    const unsub = adapter.subscribe('itest:broadcast:a', (payload, info) => received.push({ payload, ...info }))
    expect(unsub.ready).toBeInstanceOf(Promise)
    await unsub.ready

    const first = await adapter.publish('itest:broadcast:a', 'one')
    const second = await adapter.publish('itest:broadcast:a', 'two')

    await waitFor(() => received.length === 2)
    expect(received.map((entry) => entry.payload)).toEqual(['one', 'two'])
    expect(received[0]).toMatchObject({ seq: first.seq, timestamp: first.timestamp })
    expect(received[1]).toMatchObject({ seq: second.seq, timestamp: second.timestamp })
    expect(second.seq).toBe(first.seq + 1)
    unsub()
  })

  it('runs the public Room lifecycle across two installRedis-only Node instances', async () => {
    const roomId = `itest-public-room-${Date.now()}-${Math.random().toString(36).slice(2)}`
    const prefix = `itest-public:${roomId}:`
    const instanceA = spawnPublicRoomInstance('A', roomId, prefix)
    let instanceB: Worker | undefined

    try {
      const readyA = await waitForWorkerMessage(instanceA, (message) => message.type === 'ready')
      expect(readyA).toMatchObject({ role: 'A', backend: 'RedisRoomBackend' })

      const joinedOnA = waitForWorkerMessage(instanceA, (message) => message.type === 'join' && message.name === 'B')
      instanceB = spawnPublicRoomInstance('B', roomId, prefix)
      const readyB = await waitForWorkerMessage(instanceB, (message) => message.type === 'ready')
      expect(readyB).toMatchObject({ role: 'B', backend: 'RedisRoomBackend' })
      await expect(joinedOnA).resolves.toMatchObject({ participantId: readyB.participantId })

      const message = { kind: 'public-room-cross-instance', nonce: Math.random().toString(36).slice(2) }
      const receivedOnB = waitForWorkerMessage(instanceB, (event) => event.type === 'message' && event.from === 'A')
      const publishedOnA = waitForWorkerMessage(instanceA, (event) => event.type === 'published')
      instanceA.postMessage({ type: 'publish', data: message })
      await expect(publishedOnA).resolves.toMatchObject({ role: 'A' })
      await expect(receivedOnB).resolves.toMatchObject({ role: 'B', data: message })

      const leftOnB = waitForWorkerMessage(
        instanceB,
        (event) => event.type === 'leave' && event.participantId === readyB.participantId,
      )
      const kickedOnA = waitForWorkerMessage(instanceA, (event) => event.type === 'kicked')
      instanceA.postMessage({ type: 'kick', participantId: readyB.participantId })
      await expect(kickedOnA).resolves.toMatchObject({ role: 'A' })
      await expect(leftOnB).resolves.toMatchObject({ role: 'B' })
    } finally {
      await Promise.all([stopPublicRoomInstance(instanceA), instanceB ? stopPublicRoomInstance(instanceB) : undefined])
    }
  })
})

type PublicRoomWorkerMessage = {
  type: 'ready' | 'join' | 'leave' | 'message' | 'published' | 'kicked' | 'stopped' | 'error'
  role?: 'A' | 'B'
  backend?: string
  participantId?: string
  name?: string
  from?: string
  data?: unknown
  error?: string
}

const PUBLIC_ROOM_WORKER_SOURCE = String.raw`
const { parentPort, workerData } = require('node:worker_threads')

void (async () => {
  const { Redis } = await import('ioredis')
  const { installRedis } = await import('@telefunc/redis')
  const { Room } = await import('telefunc')
  const { disposeRoomBackend, getRoomBackend } = await import('telefunc/backend')
  const redis = new Redis(workerData.redisUrl)
  await redis.ping()
  installRedis(redis, { prefix: workerData.prefix })

  const room =
    workerData.role === 'A'
      ? await Room.getOrCreate(workerData.roomId, { meta: { purpose: 'public-cross-instance-integration' } })
      : await Room.get(workerData.roomId)
  if (!room) throw new Error('public Room was not visible from the second Node instance')

  room.onJoin((participant) =>
    parentPort.postMessage({
      type: 'join',
      role: workerData.role,
      participantId: participant.id,
      name: participant.meta.name,
    }),
  )
  room.onLeave((participant) =>
    parentPort.postMessage({
      type: 'leave',
      role: workerData.role,
      participantId: participant.id,
      name: participant.meta.name,
    }),
  )
  const unsubscribe = room.subscribe((data, _info, from) =>
    parentPort.postMessage({ type: 'message', role: workerData.role, data, from: from.meta.name }),
  )
  const participant = await room.join({ meta: { name: workerData.role } })
  parentPort.postMessage({
    type: 'ready',
    role: workerData.role,
    backend: getRoomBackend().constructor.name,
    participantId: participant.id,
  })

  parentPort.on('message', async (command) => {
    try {
      if (command.type === 'publish') {
        await participant.publish(command.data)
        parentPort.postMessage({ type: 'published', role: workerData.role })
      } else if (command.type === 'kick') {
        await Room.removeParticipant(workerData.roomId, { id: command.participantId })
        parentPort.postMessage({ type: 'kicked', role: workerData.role })
      } else if (command.type === 'stop') {
        unsubscribe()
        await participant.leave().catch(() => {})
        await disposeRoomBackend().catch(() => {})
        await redis.quit().catch(() => {})
        parentPort.postMessage({ type: 'stopped', role: workerData.role })
      }
    } catch (error) {
      parentPort.postMessage({ type: 'error', role: workerData.role, error: error && error.stack ? error.stack : String(error) })
    }
  })
})().catch((error) => {
  parentPort.postMessage({ type: 'error', role: workerData.role, error: error && error.stack ? error.stack : String(error) })
})
`

function spawnPublicRoomInstance(role: 'A' | 'B', roomId: string, prefix: string): Worker {
  return new Worker(PUBLIC_ROOM_WORKER_SOURCE, {
    eval: true,
    workerData: { role, roomId, prefix, redisUrl: REDIS_URL },
  })
}

function waitForWorkerMessage(
  worker: Worker,
  matches: (message: PublicRoomWorkerMessage) => boolean,
  timeoutMs = 10_000,
): Promise<PublicRoomWorkerMessage> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => finish(new Error('timed out waiting for public Room worker event')), timeoutMs)
    const onMessage = (message: PublicRoomWorkerMessage) => {
      if (message.type === 'error') {
        finish(new Error(message.error ?? 'public Room worker failed'))
      } else if (matches(message)) {
        finish(undefined, message)
      }
    }
    const onError = (error: Error) => finish(error)
    const onExit = (code: number) => {
      if (code !== 0) finish(new Error(`public Room worker exited with code ${code}`))
    }
    function finish(error?: Error, message?: PublicRoomWorkerMessage): void {
      clearTimeout(timer)
      worker.off('message', onMessage)
      worker.off('error', onError)
      worker.off('exit', onExit)
      if (error) reject(error)
      else resolve(message!)
    }
    worker.on('message', onMessage)
    worker.on('error', onError)
    worker.on('exit', onExit)
  })
}

async function stopPublicRoomInstance(worker: Worker): Promise<void> {
  if (worker.threadId === -1) return
  const stopped = waitForWorkerMessage(worker, (message) => message.type === 'stopped', 3_000)
  worker.postMessage({ type: 'stop' })
  await stopped.catch(() => {})
  await worker.terminate()
}

async function waitFor(cond: () => boolean, timeoutMs = 2_000): Promise<void> {
  const start = Date.now()
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('timed out waiting for delivery')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}
