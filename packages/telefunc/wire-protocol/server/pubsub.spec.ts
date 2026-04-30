import { afterEach, describe, expect, it } from 'vitest'
import { channel } from './channel.js'
import { ServerPubSub } from './server-pubsub.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { TAG, decode } from '../shared-ws.js'
import { IndexedPeer } from './IndexedPeer.js'
import { getPubSubAdapter, _resetPubSubAdapterForTesting, DefaultPubSubAdapter } from './pubsub.js'
import type { PubSubTransport } from './pubsub.js'

const previousPubSubAdapter = getPubSubAdapter()

afterEach(() => {
  _resetPubSubAdapterForTesting(previousPubSubAdapter)
})

describe('keyed channel pubsub', () => {
  it('delivers published messages to other channels with the same key', () => {
    const sender = new ServerPubSub<{ text: string }>({ key: 'room:test:deliver' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:test:deliver' })
    sender._registerChannel()
    receiver._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('delivers published messages back to the source channel', () => {
    const ps = new ServerPubSub<{ text: string }>({ key: 'room:test:self' })
    ps._registerChannel()

    const received: Array<{ text: string }> = []
    ps.subscribe((message) => {
      received.push(message)
    })

    ps.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('throws on Channel methods that do not apply to pubsub', () => {
    const ps = new ServerPubSub<{ text: string }>({ key: 'room:test:disallowed' })

    expect(() => ps.listen()).toThrow(/`listen\(\)` is not available/)
    expect(() => ps.listenBinary()).toThrow(/`listenBinary\(\)` is not available/)
    expect(() => ps.send()).toThrow(/`send\(\)` is not available/)
    expect(() => ps.sendBinary()).toThrow(/`sendBinary\(\)` is not available/)
  })

  it('channel() does not have publish or subscribe', () => {
    const ch = channel<{ text: string }, { text: string }>()

    expect((ch as { publish?: unknown }).publish).toBeUndefined()
    expect((ch as { subscribe?: unknown }).subscribe).toBeUndefined()
  })

  it('buffers keyed publishes until the channel is registered', () => {
    const sender = new ServerPubSub<{ text: string }>({ key: 'room:test:late-register' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:test:late-register' })

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('buffers delivered pubsub frames until a peer attaches', () => {
    const sender = new ServerPubSub<{ text: string }>({ key: 'room:test:buffered-delivery' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:test:buffered-delivery' })
    sender._registerChannel()
    receiver._registerChannel()

    // Simulate client-initiated subscribe so frames are forwarded to peer
    receiver._onPeerPubSubSubscribe(false)

    sender.publish({ text: 'hello' })

    const frames: Uint8Array[] = []
    receiver._attachPeer(
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
    const decoded = decode(frames[0]! as Uint8Array<ArrayBuffer>)
    expect(decoded.tag).toBe(TAG.PUBLISH)
    if (decoded.tag !== TAG.PUBLISH) throw new Error('Expected publish frame')
    expect(decoded.text).toBe('{"text":"hello"}')
  })

  it('returns a publish receipt when ack is requested', async () => {
    const sender = new ServerPubSub<{ text: string }>({ key: 'room:test:ack' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:test:ack' })
    sender._registerChannel()
    receiver._registerChannel()

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
    const sender = new ServerPubSub<{ text: string }>({ key: 'room:test:ack-default' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:test:ack-default' })
    sender._registerChannel()
    receiver._registerChannel()

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

describe('DefaultPubSubAdapter with transport', () => {
  type TextListener = (payload: string, info: { seq: number; ts: number }) => void

  /** Simulates a multi-node message bus with synchronous delivery. */
  function createMockTransport(): PubSubTransport & {
    _listeners: Map<string, Set<TextListener>>
  } {
    const listeners = new Map<string, Set<TextListener>>()
    const seqs = new Map<string, number>()
    return {
      _listeners: listeners,
      send(key, payload) {
        const seq = (seqs.get(key) ?? 0) + 1
        seqs.set(key, seq)
        const ts = Date.now()
        const set = listeners.get(key)
        if (set) {
          for (const cb of set) cb(payload, { seq, ts })
        }
        return { seq, ts }
      },
      listen(key, onMessage) {
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
      sendBinary(_key, _payload) {
        return { seq: 0, ts: Date.now() }
      },
      listenBinary() {
        return () => {}
      },
    }
  }

  it('delivers locally and returns transport seq', async () => {
    const transport = createMockTransport()
    const registry = new DefaultPubSubAdapter(transport)
    _resetPubSubAdapterForTesting(registry)

    const sender = new ServerPubSub<{ text: string }>({ key: 'room:adapted:basic' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:adapted:basic' })
    sender._registerChannel()
    receiver._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))

    const receipt = await sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
    expect(receipt.seq).toBe(1)
    expect(receipt.ts).toEqual(expect.any(Number))
  })

  it('does not double-deliver from same node echo', () => {
    const transport = createMockTransport()
    const registry = new DefaultPubSubAdapter(transport)
    _resetPubSubAdapterForTesting(registry)

    const sender = new ServerPubSub<{ text: string }>({ key: 'room:adapted:echo' })
    const receiver = new ServerPubSub<{ text: string }>({ key: 'room:adapted:echo' })
    sender._registerChannel()
    receiver._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))

    sender.publish({ text: 'hello' })

    // Receiver should get the message exactly once, not twice
    expect(received).toEqual([{ text: 'hello' }])
  })

  it('delivers from a different node (different adapter instance)', () => {
    const transport = createMockTransport()
    const node1 = new DefaultPubSubAdapter(transport)
    const node2 = new DefaultPubSubAdapter(transport)

    const received: string[] = []
    node2.subscribe('room:cross-node', (serialized: string) => received.push(serialized))

    node1.publish('room:cross-node', '{"text":"from-node1"}')

    expect(received).toEqual(['{"text":"from-node1"}'])
  })

  it('unsubscribes from transport when last local subscriber leaves', () => {
    const transport = createMockTransport()
    const registry = new DefaultPubSubAdapter(transport)

    const unsub1 = registry.subscribe('room:unsub', () => {})
    const unsub2 = registry.subscribe('room:unsub', () => {})
    expect(transport._listeners.get('room:unsub')?.size).toBe(1) // one transport subscription

    unsub1()
    expect(transport._listeners.has('room:unsub')).toBe(true) // still subscribed

    unsub2()
    expect(transport._listeners.has('room:unsub')).toBe(false) // transport unsubscribed
  })
})
