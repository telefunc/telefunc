// Cross-instance write transport (T4 slice 3, rehomed onto the injected changeTransport). A committed batch
// is published to each touched table's topic with a batch id; a subscriber feeds the WHOLE batch into its
// graphs via router.ingest, deduped by id so a batch spanning several of its topics applies ONCE and a local
// write's own round-trip is dropped. Each test INJECTS its own in-process transport (setChangeTransport), so
// subscribers never leak across tests; registryFor is mocked so router.ingest is observable.

import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { TableChange } from '../router/events.js'

const engine = vi.hoisted(() => ({ ingest: vi.fn() }))
vi.mock('./dbRuntime.js', () => ({
  registryFor: () => ({ router: { ingest: engine.ingest, register: vi.fn(), unregister: vi.fn() }, acquire: vi.fn() }),
  ingestWrite: vi.fn(),
}))

import { createInMemoryChangeTransport, setChangeTransport } from './changeTransport.js'
import { batchTopic, ensureSubscribed, publishBatch } from './writeTransport.js'

const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })
let counter = 0
const remoteId = () => `remote-${counter++}`

// A fresh db + its own injected transport per test → no cross-test subscriber leakage.
function freshDb() {
  const db = {}
  const transport = createInMemoryChangeTransport()
  setChangeTransport(db, transport)
  return { db, transport }
}

beforeEach(() => engine.ingest.mockClear())

describe('write transport — per-table topics + batch-ID dedupe', () => {
  it('a batch spanning TWO of a subscriber’s topics applies exactly ONCE (dedupe by id)', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users', 'posts'])
    // A REMOTE instance's publish: the same batch (one id) on each touched table's topic.
    const message = { id: remoteId(), changes }
    transport.publish(batchTopic('users'), message)
    transport.publish(batchTopic('posts'), message)
    expect(engine.ingest).toHaveBeenCalledTimes(1) // the second topic's delivery is deduped
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('a single-topic subscriber receives the WHOLE batch (router slices per table)', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users']) // only subscribed to one of the two touched topics
    transport.publish(batchTopic('users'), { id: remoteId(), changes })
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes }) // the whole batch, not just this table's slice
  })

  it('a LOCAL write is NOT double-applied: its own round-trip is deduped (fed directly, not via transport)', async () => {
    const { db } = freshDb()
    await ensureSubscribed(db, ['users'])
    // publishBatch pre-marks its id seen, then publishes — the local subscriber must drop the round-trip.
    publishBatch(db, { changes: [change('users')] })
    expect(engine.ingest).not.toHaveBeenCalled() // the local instance already fed its graphs directly
  })

  it('a genuinely remote batch (unseen id) IS applied', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users')]
    await ensureSubscribed(db, ['users'])
    transport.publish(batchTopic('users'), { id: remoteId(), changes })
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })
})
