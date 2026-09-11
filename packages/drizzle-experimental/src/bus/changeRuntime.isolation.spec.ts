// LOGICAL-DATABASE ISOLATION.
//
// The default bus is one process-wide object and the topic used to be one constant, so EVERY reactive db
// shared a stream. Two unrelated databases that both have a `users` table then applied each other's row
// deltas — a wrong row into a precise graph, not a harmless over-fire. Identity now rides the topic and the
// envelope: derived from the connection by default, or named explicitly for a shared/injected transport.
//
// Two guards, and each is pinned ALONE below, because either one on its own would make the pair look
// sufficient: the topic keeps a foreign publication from being delivered at all, and the envelope's namespace
// rejects one that an over-broad adapter delivers anyway.

import { beforeEach, describe, expect, it, vi } from 'vitest'
vi.mock('./dbRuntime.js', async () => (await import('./changeRuntime.registryMock.js')).dbRuntimeMock())

import { CHANGE_CODEC_VERSION } from './changeCodec.js'
import { createInMemoryChangeTransport } from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import {
  acquireSubscription,
  changeTopicFor,
  publishBatch,
  setChangeNamespace,
  setChangeTransport,
} from './changeRuntime.js'
import { change, engine, flush, resetEngine, twoInstances, watching } from './changeRuntime.testKit.js'

beforeEach(resetEngine)

describe('write transport — unrelated databases never see each other', () => {
  it('two DIFFERENT databases on one transport do NOT cross-apply, even with the same table name', async () => {
    const transport = createInMemoryChangeTransport()
    const writer = { $client: { connection: 'db-one' } } // two genuinely different connections…
    const bystander = { $client: { connection: 'db-two' } }
    setChangeTransport(writer, transport)
    setChangeTransport(bystander, transport)
    await acquireSubscription(writer)
    await acquireSubscription(bystander)
    registryFor(bystander).router.register(watching('users'))

    publishBatch(writer, { changes: [change('users')] })
    await flush()

    expect(engine.ingest).not.toHaveBeenCalled() // …so a `users` row from one is not a `users` row in the other
  })

  it('…while two runtimes over the SAME database still do — so the isolation is not just deafness', async () => {
    const { dbA } = await twoInstances()
    publishBatch(dbA, { changes: [change('users')] })
    await flush()
    expect(engine.ingest).toHaveBeenCalledTimes(1)
  })

  it('an explicit namespace joins runtimes that share NO connection object — the cross-process case', async () => {
    // Different processes cannot share a `$client`, so a shared transport is paired with a stable name.
    const transport = createInMemoryChangeTransport()
    const serverA = {}
    const serverB = {}
    setChangeTransport(serverA, transport)
    setChangeTransport(serverB, transport)
    setChangeNamespace(serverA, 'orders-db')
    setChangeNamespace(serverB, 'orders-db')
    await acquireSubscription(serverA)
    await acquireSubscription(serverB)

    publishBatch(serverA, { changes: [change('users')] })
    await flush()

    expect(engine.ingest).toHaveBeenCalledTimes(1) // same name, same database, changes flow
  })
})

describe('write transport — each isolation guard, on its own', () => {
  it('TOPIC: a different database publishes on a topic this one is not subscribed to', async () => {
    const transport = createInMemoryChangeTransport()
    const listener = { $client: { connection: 'db-one' } }
    const stranger = { $client: { connection: 'db-two' } }
    setChangeTransport(listener, transport)
    setChangeTransport(stranger, transport)
    await acquireSubscription(listener)

    const heard: string[] = []
    await transport.subscribe(changeTopicFor(listener), (payload) => heard.push(payload))
    publishBatch(stranger, { changes: [change('users')] })
    await flush()

    expect(heard).toEqual([]) // nothing was even delivered — the topics genuinely differ
  })

  it('ENVELOPE: a foreign-namespace payload delivered ONTO our topic is ignored', async () => {
    // The case the topic cannot cover: an adapter that fans out more widely than the topic it was handed.
    const transport = createInMemoryChangeTransport()
    const listener = { $client: { connection: 'db-one' } }
    setChangeTransport(listener, transport)
    await acquireSubscription(listener)
    registryFor(listener).router.register(watching('users'))

    const foreign = JSON.stringify({
      version: CHANGE_CODEC_VERSION,
      namespace: 'some-other-database',
      origin: 'elsewhere',
      seq: 1,
      changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }],
    })
    transport.publish(changeTopicFor(listener), foreign)

    // Not applied — and not coarsened either: another database's rows say nothing about ours, so a refetch
    // would be spurious rather than merely cautious.
    expect(engine.ingest).not.toHaveBeenCalled()
  })
})
