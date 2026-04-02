import { afterEach, describe, expect, it } from 'vitest'
import { channel } from './channel.js'
import { pubsub } from './server-pubsub.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { TAG, decode } from '../shared-ws.js'
import { IndexedPeer } from './IndexedPeer.js'
import { getPubSubAdapter, setPubSubAdapter, DefaultPubSubAdapter } from './pubsub.js'
import type { PubSubAdapter } from './pubsub.js'

const previousPubSubAdapter = getPubSubAdapter()

afterEach(() => {
  setPubSubAdapter(previousPubSubAdapter)
})

describe('keyed channel pubsub', () => {
  it('delivers published messages to other channels with the same key', () => {
    const sender = pubsub<{ text: string }>('room:test:deliver')
    const receiver = pubsub<{ text: string }>('room:test:deliver')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('delivers published messages back to the source channel', () => {
    const ps = pubsub<{ text: string }>('room:test:self')
    ;(ps as any)._registerChannel()

    const received: Array<{ text: string }> = []
    ps.subscribe((message) => {
      received.push(message)
    })

    ps.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('keeps listen() separate from subscribe()', () => {
    const sender = pubsub<{ text: string }>('room:test:separate')
    const receiver = pubsub<{ text: string }>('room:test:separate')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const listened: Array<{ text: string }> = []
    const subscribed: Array<{ text: string }> = []

    receiver.listen((message) => {
      listened.push(message)
    })
    receiver.subscribe((message) => {
      subscribed.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(subscribed).toEqual([{ text: 'hello' }])
    expect(listened).toEqual([])
  })

  it('channel() does not have publish or subscribe', () => {
    const ch = channel<{ text: string }, { text: string }>()

    expect((ch as any).publish).toBeUndefined()
    expect((ch as any).subscribe).toBeUndefined()
  })

  it('buffers keyed publishes until the channel is registered', () => {
    const sender = pubsub<{ text: string }>('room:test:late-register')
    const receiver = pubsub<{ text: string }>('room:test:late-register')

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('buffers delivered pubsub frames until a peer attaches', () => {
    const sender = pubsub<{ text: string }>('room:test:buffered-delivery')
    const receiver = pubsub<{ text: string }>('room:test:buffered-delivery')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    // Simulate client-initiated subscribe so frames are forwarded to peer
    ;(receiver as any)._onPeerPubSubSubscribe(false)

    sender.publish({ text: 'hello' })

    const frames: Uint8Array[] = []
    ;(receiver as any)._attachPeer(
      new IndexedPeer(
        {
          send(frame) {
            frames.push(frame)
          },
        },
        7,
        new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024),
      ),
    )

    expect(frames).toHaveLength(1)
    const decoded = decode(frames[0]!)
    expect(decoded.tag).toBe(TAG.PUBLISH)
    if (decoded.tag !== TAG.PUBLISH) throw new Error('Expected publish frame')
    expect(decoded.text).toBe('{"text":"hello"}')
  })

  it('returns a publish receipt when ack is requested', async () => {
    const sender = pubsub<{ text: string }>('room:test:ack')
    const receiver = pubsub<{ text: string }>('room:test:ack')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    const firstReceipt = await sender.publish({ text: 'hello' })
    const secondReceipt = await sender.publish({ text: 'again' })

    expect(received).toEqual([{ text: 'hello' }, { text: 'again' }])
    expect(firstReceipt).toMatchObject({
      key: 'room:test:ack',
      seq: 1,
      meta: {
        delivered: 1,
        transport: 'in-memory',
      },
    })
    expect(firstReceipt.ts).toEqual(expect.any(Number))
    expect(secondReceipt).toMatchObject({
      key: 'room:test:ack',
      seq: 2,
    })
    expect(secondReceipt.ts).toEqual(expect.any(Number))
  })

  it('uses channel ack mode as the default for keyed publish', async () => {
    const sender = pubsub<{ text: string }>('room:test:ack-default')
    const receiver = pubsub<{ text: string }>('room:test:ack-default')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    const receipt = await sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
    expect(receipt).toMatchObject({
      key: 'room:test:ack-default',
      seq: 1,
    })
    expect(receipt.ts).toEqual(expect.any(Number))
  })
})

describe('DefaultPubSubAdapter with adapter', () => {
  /** Simulates a multi-node message bus with synchronous delivery. */
  function createMockAdapter(): PubSubAdapter & {
    _listeners: Map<string, Set<(message: string, info: { seq: number; ts: number }) => void>>
  } {
    const listeners = new Map<string, Set<(message: string, info: { seq: number; ts: number }) => void>>()
    const seqs = new Map<string, number>()
    return {
      _listeners: listeners,
      subscribe(key, onMessage) {
        let set = listeners.get(key)
        if (!set) {
          set = new Set()
          listeners.set(key, set)
        }
        set.add(onMessage)
        return () => {
          listeners.delete(key)
        }
      },
      publish(key, message) {
        const seq = (seqs.get(key) ?? 0) + 1
        seqs.set(key, seq)
        const ts = Date.now()
        const set = listeners.get(key)
        if (set) {
          for (const cb of set) cb(message, { seq, ts })
        }
        return { seq, ts }
      },
      subscribeBinary() {
        return () => {}
      },
      publishBinary() {
        return { seq: 0, ts: Date.now() }
      },
    }
  }

  it('delivers locally and returns adapter seq', async () => {
    const adapter = createMockAdapter()
    const registry = new DefaultPubSubAdapter(adapter)
    setPubSubAdapter(registry)

    const sender = pubsub<{ text: string }>('room:adapted:basic')
    const receiver = pubsub<{ text: string }>('room:adapted:basic')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))

    const receipt = await sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
    expect(receipt.seq).toBe(1)
    expect(receipt.ts).toEqual(expect.any(Number))
  })

  it('does not double-deliver from same node echo', () => {
    const adapter = createMockAdapter()
    const registry = new DefaultPubSubAdapter(adapter)
    setPubSubAdapter(registry)

    const sender = pubsub<{ text: string }>('room:adapted:echo')
    const receiver = pubsub<{ text: string }>('room:adapted:echo')
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))

    sender.publish({ text: 'hello' })

    // Receiver should get the message exactly once, not twice
    // (once from local delivery, adapter echo should be filtered by nodeId)
    expect(received).toEqual([{ text: 'hello' }])
  })

  it('delivers from a different node (different adapter instance)', () => {
    const adapter = createMockAdapter()
    const node1 = new DefaultPubSubAdapter(adapter)
    const node2 = new DefaultPubSubAdapter(adapter)

    // node2 has a subscriber
    const received: string[] = []
    node2.subscribe('room:cross-node', (serialized: string) => received.push(serialized))

    // node1 publishes
    node1.publish('room:cross-node', '{"text":"from-node1"}')

    expect(received).toEqual(['{"text":"from-node1"}'])
  })

  it('unsubscribes from adapter when last local subscriber leaves', () => {
    const adapter = createMockAdapter()
    const registry = new DefaultPubSubAdapter(adapter)

    const unsub1 = registry.subscribe('room:unsub', () => {})
    const unsub2 = registry.subscribe('room:unsub', () => {})
    expect(adapter._listeners.get('room:unsub')?.size).toBe(1) // one adapter subscription

    unsub1()
    expect(adapter._listeners.has('room:unsub')).toBe(true) // still subscribed

    unsub2()
    expect(adapter._listeners.has('room:unsub')).toBe(false) // adapter unsubscribed
  })

  it('returns seq: 0 when adapter omits seq', async () => {
    const adapter = {
      subscribe() {
        return () => {}
      },
      publish() {
        return {} // no seq, no ts
      },
      subscribeBinary() {
        return () => {}
      },
      publishBinary() {
        return {}
      },
    } as unknown as PubSubAdapter
    const registry = new DefaultPubSubAdapter(adapter)
    setPubSubAdapter(registry)

    const ps = pubsub<{ text: string }>('room:no-seq')
    ;(ps as any)._registerChannel()

    const receipt = await ps.publish({ text: 'hello' })
    expect(receipt.seq).toBe(0)
    expect(receipt.ts).toEqual(expect.any(Number))
  })
})
