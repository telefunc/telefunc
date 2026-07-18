// Cross-instance write transport (T4 slice 3). A committed batch is published to each touched table's topic
// with a batch id; a subscriber feeds the WHOLE batch into its graphs via router.ingest, deduped by id so a
// batch spanning several of its topics applies ONCE, and a local write's own round-trip is dropped. Uses the
// in-memory Broadcast adapter; registryFor is mocked so router.ingest is observable.
//
// ISOLATION: telefunc's Broadcast subscriptions are process-global and never torn down here, so a topic used
// by one test would keep firing that test's subscriber in later tests. Each test therefore uses UNIQUE table
// names (uniqueTables) so its subscribers live on their own topics and never cross-fire — the dedupe under
// test is then the ONLY thing collapsing multi-topic delivery, which is what the red-proof must target.

import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { TableChange } from '../router/events.js'

const engine = vi.hoisted(() => ({ ingest: vi.fn() }))
vi.mock('./dbRuntime.js', () => ({
  registryFor: () => ({ router: { ingest: engine.ingest, register: vi.fn(), unregister: vi.fn() }, acquire: vi.fn() }),
  ingestWrite: vi.fn(),
}))

import { Broadcast } from 'telefunc'
import { batchTopic, ensureSubscribed, publishBatch } from './writeTransport.js'

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })
let counter = 0
const remoteId = () => `remote-${counter++}`
const uniqueTables = (...names: string[]) => names.map((n) => `t${counter++}_${n}`)

beforeEach(() => engine.ingest.mockClear())

describe('write transport — per-table topics + batch-ID dedupe', () => {
  it('a batch spanning TWO of a subscriber’s topics applies exactly ONCE (dedupe by id)', async () => {
    const [users, posts] = uniqueTables('users', 'posts')
    const changes = [change(users), change(posts)]
    ensureSubscribed({}, [users, posts])
    // A REMOTE instance's publish: the same batch (one id) on each touched table's topic.
    const message = { id: remoteId(), changes }
    void Broadcast.publish(batchTopic(users), message)
    void Broadcast.publish(batchTopic(posts), message)
    await tick()
    expect(engine.ingest).toHaveBeenCalledTimes(1) // the second topic's delivery is deduped
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('a single-topic subscriber receives the WHOLE batch (router slices per table)', async () => {
    const [users, posts] = uniqueTables('users', 'posts')
    const changes = [change(users), change(posts)]
    ensureSubscribed({}, [users]) // only subscribed to one of the two touched topics
    void Broadcast.publish(batchTopic(users), { id: remoteId(), changes })
    await tick()
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes }) // the whole batch, not just this table's slice
  })

  it('a LOCAL write is NOT double-applied: its own round-trip is deduped (fed directly, not via transport)', async () => {
    const [users] = uniqueTables('users')
    ensureSubscribed({}, [users])
    // publishBatch pre-marks its id seen, then publishes — the local subscriber must drop the round-trip.
    publishBatch({ changes: [change(users)] })
    await tick()
    expect(engine.ingest).not.toHaveBeenCalled() // the local instance already fed its graphs directly
  })

  it('a genuinely remote batch (unseen id) IS applied', async () => {
    const [users] = uniqueTables('users')
    const changes = [change(users)]
    ensureSubscribed({}, [users])
    void Broadcast.publish(batchTopic(users), { id: remoteId(), changes })
    await tick()
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })
})
