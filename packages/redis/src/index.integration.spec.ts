// Real-Redis certification harness for the `commitFrame` + subscribe-readiness realizations.
//
// The unit spec (`index.spec.ts`) proves the Lua and the wire codec against a fake ioredis; this proves
// the same contract against a live Redis, where the parts a fake can't model are the real ones: the
// commit runs as one atomic EVAL inside Redis (fences + clock from `TIME` + retain + `PUBLISH`), and
// `SUBSCRIBE` has a genuine ack the readiness handoff waits on before a publish can reach a connection.
//
// A fast, deterministic companion to the `test/playground-stream` `.test-docker` jobs, which run the
// whole Room path across real (DragonflyDB) instances in CI: this one pins the transport's low-level
// contract — stale-fence result, framed retain, cross-connection order — that the e2e doesn't assert.
//
// Gated on `TELEFUNC_TEST_REAL_REDIS` so it skips in the default `test:units` job (no Redis there). Run
// it against a throwaway server — it FLUSHES the database between tests:
//
//   docker run --rm -p 6379:6379 redis
//   TELEFUNC_TEST_REAL_REDIS=redis://localhost:6379 \
//     pnpm --filter @telefunc/redis exec vitest run src/index.integration.spec.ts
//
// Certified green against redis:7-alpine. The Cluster hash-slot co-location noted in `COMMIT_FRAME_LUA`
// still needs a real Cluster and is certified separately.
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { Redis } from 'ioredis'
import { DefaultBroadcastAdapter, type BroadcastAdapter, type RoomFrameCommit } from 'telefunc'
import { RedisTransport } from './index.js'

const REDIS_URL = process.env.TELEFUNC_TEST_REAL_REDIS

type Received = { payload: string; seq: number; timestamp: number }

describe.skipIf(!REDIS_URL)('Redis realization against a live server', () => {
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
    // Best-effort teardown: quit the client we own. The transport duplicates a subscriber connection
    // internally (no public close), which vitest reaps when the worker exits.
    await redis.quit()
  })

  function frame(channelKey: string, payload: string, extra?: Partial<RoomFrameCommit>): RoomFrameCommit {
    return { partitionKey: channelKey, fences: [], orderKey: `${channelKey}:o`, channelKey, payload, ...extra }
  }

  it('delivers a committed frame across the pub/sub broker with the order the commit assigned', async () => {
    const received: Received[] = []
    const unsub = adapter.subscribe('itest:room:a', (payload, info) => received.push({ payload, ...info }))
    // The readiness handoff, live: the real SUBSCRIBE must ack before a publish can reach this connection.
    expect(unsub.ready).toBeInstanceOf(Promise)
    await unsub.ready

    const first = await adapter.commitFrame!(frame('itest:room:a', 'one'))
    const second = await adapter.commitFrame!(frame('itest:room:a', 'two'))
    expect(first.ok && second.ok).toBe(true)

    await waitFor(() => received.length === 2)
    expect(received.map((r) => r.payload)).toEqual(['one', 'two'])
    if (first.ok && second.ok) {
      // Every receiver reads the frame's committed place in the order, and the room clock strictly advances.
      expect(received[0]).toMatchObject({ seq: first.seq, timestamp: first.timestamp })
      expect(received[1]).toMatchObject({ seq: second.seq, timestamp: second.timestamp })
      const advanced =
        second.timestamp > first.timestamp || (second.timestamp === first.timestamp && second.seq > first.seq)
      expect(advanced).toBe(true)
    }
    unsub()
  })

  it('rejects a commit whose fence no longer matches, and admits it once the fence is restored', async () => {
    await adapter.set!('itest:fence', 'inc-1')
    const stale = await adapter.commitFrame!(
      frame('itest:room:b', 'x', { fences: [{ key: 'itest:fence', expected: 'inc-2' }] }),
    )
    expect(stale).toEqual({ ok: false, reason: 'stale-fence' })

    const fresh = await adapter.commitFrame!(
      frame('itest:room:b', 'x', { fences: [{ key: 'itest:fence', expected: 'inc-1' }] }),
    )
    expect(fresh.ok).toBe(true)
  })

  it('retains the committed frame under retainKey as self-framed bytes', async () => {
    const res = await adapter.commitFrame!(frame('itest:room:c', 'pinned', { retainKey: 'itest:room:c:rt' }))
    expect(res.ok).toBe(true)
    // The retained copy is the payload framed with its assigned order (`seq,ts\npayload`), so a replay
    // re-emits it in its real place — the room decodes it back on the retained read.
    expect(await adapter.get!('itest:room:c:rt')).toMatch(/^\d+,\d+\npinned$/)
  })
})

async function waitFor(cond: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('timed out waiting for delivery')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}
