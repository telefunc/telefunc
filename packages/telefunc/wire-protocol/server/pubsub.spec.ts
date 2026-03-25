import { afterEach, describe, expect, it } from 'vitest'
import { createChannel } from './channel.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { TAG, decode } from '../shared-ws.js'
import { IndexedPeer } from './IndexedPeer.js'
import { getPubSubRegistry, setPubSubRegistry, DefaultPubSubRegistry } from './pubsub.js'
import type { PubSubAdapter } from './pubsub.js'

const previousPubSubRegistry = getPubSubRegistry()

afterEach(() => {
  setPubSubRegistry(previousPubSubRegistry)
})

describe('keyed channel pubsub', () => {
  it('delivers published messages to other channels with the same key', () => {
    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:test:deliver' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:test:deliver' })
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('delivers published messages back to the source channel by default (selfDelivery: true)', () => {
    const channel = createChannel<{ text: string }, { text: string }>({ key: 'room:test:self' })
    ;(channel as any)._registerChannel()

    const received: Array<{ text: string }> = []
    channel.subscribe((message) => {
      received.push(message)
    })

    channel.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('does not loop published messages back to the source channel when selfDelivery is false', () => {
    const channel = createChannel<{ text: string }, { text: string }>({
      key: 'room:test:self-off',
      selfDelivery: false,
    })
    ;(channel as any)._registerChannel()

    const received: Array<{ text: string }> = []
    channel.subscribe((message) => {
      received.push(message)
    })

    channel.publish({ text: 'hello' })

    expect(received).toEqual([])
  })

  it('keeps listen() separate from subscribe()', () => {
    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:test:separate' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:test:separate' })
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

  it('requires a key for publish() and subscribe()', () => {
    const channel = createChannel<{ text: string }, { text: string }>()
    const untypedChannel = channel as any

    expect(() => untypedChannel.publish({ text: 'hello' })).toThrow('Channel.publish() requires createChannel({ key })')
    expect(() => untypedChannel.subscribe(() => undefined)).toThrow(
      'Channel.subscribe() requires createChannel({ key })',
    )
  })

  it('buffers keyed publishes until the channel is registered', () => {
    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:test:late-register' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:test:late-register' })

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('buffers delivered pubsub frames until a peer attaches', () => {
    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:test:buffered-delivery' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:test:buffered-delivery' })
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    // subscribe() registers with the transport so the receiver gets pub/sub messages
    receiver.subscribe(() => {})

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
        new ReplayBuffer(1024 * 1024, 60_000),
      ),
    )

    expect(frames).toHaveLength(1)
    const decoded = decode(frames[0]!)
    expect(decoded.tag).toBe(TAG.PUBLISH)
    if (decoded.tag !== TAG.PUBLISH) throw new Error('Expected publish frame')
    expect(decoded.text).toBe('{"text":"hello"}')
  })

  it('returns a publish receipt when ack is requested', async () => {
    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:test:ack' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:test:ack' })
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((message) => {
      received.push(message)
    })

    const firstReceipt = await sender.publish({ text: 'hello' }, { ack: true })
    const secondReceipt = await sender.publish({ text: 'again' }, { ack: true })

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
    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:test:ack-default', ack: true })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:test:ack-default' })
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

describe('DefaultPubSubRegistry with adapter', () => {
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
      },
      unsubscribe(key) {
        listeners.delete(key)
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
    }
  }

  it('delivers locally and returns adapter seq', async () => {
    const adapter = createMockAdapter()
    const registry = new DefaultPubSubRegistry(adapter)
    setPubSubRegistry(registry)

    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:adapted:basic' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:adapted:basic' })
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
    const registry = new DefaultPubSubRegistry(adapter)
    setPubSubRegistry(registry)

    const sender = createChannel<{ text: string }, { text: string }>({ key: 'room:adapted:echo' })
    const receiver = createChannel<{ text: string }, { text: string }>({ key: 'room:adapted:echo' })
    ;(sender as any)._registerChannel()
    ;(receiver as any)._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))

    sender.publish({ text: 'hello' })

    // Receiver should get the message exactly once, not twice
    // (once from local delivery, adapter echo should be filtered by nodeId)
    expect(received).toEqual([{ text: 'hello' }])
  })

  it('delivers from a different node (different registry instance)', () => {
    const adapter = createMockAdapter()
    const node1 = new DefaultPubSubRegistry(adapter)
    const node2 = new DefaultPubSubRegistry(adapter)

    // node2 has a subscriber
    const received: string[] = []
    node2.subscribe({
      id: 'receiver-1',
      key: 'room:cross-node',
      selfDelivery: true,
      onMessage: (serialized: string) => received.push(serialized),
    })

    // node1 publishes
    node1.publish({ id: 'sender-1', key: 'room:cross-node', selfDelivery: true, serialized: '{"text":"from-node1"}' })

    expect(received).toEqual(['{"text":"from-node1"}'])
  })

  it('respects selfDelivery: false', () => {
    const adapter = createMockAdapter()
    const registry = new DefaultPubSubRegistry(adapter)
    setPubSubRegistry(registry)

    const channel = createChannel<{ text: string }, { text: string }>({
      key: 'room:adapted:self-off',
      selfDelivery: false,
    })
    ;(channel as any)._registerChannel()

    const received: Array<{ text: string }> = []
    channel.subscribe((msg) => received.push(msg))

    channel.publish({ text: 'hello' })

    expect(received).toEqual([])
  })

  it('unsubscribes from adapter when last local subscriber leaves', () => {
    const adapter = createMockAdapter()
    const registry = new DefaultPubSubRegistry(adapter)

    registry.subscribe({ id: 'sub-1', key: 'room:unsub', selfDelivery: true, onMessage() {} })
    registry.subscribe({ id: 'sub-2', key: 'room:unsub', selfDelivery: true, onMessage() {} })
    expect(adapter._listeners.get('room:unsub')?.size).toBe(1) // one adapter subscription

    registry.unsubscribe('sub-1', 'room:unsub')
    expect(adapter._listeners.has('room:unsub')).toBe(true) // still subscribed

    registry.unsubscribe('sub-2', 'room:unsub')
    expect(adapter._listeners.has('room:unsub')).toBe(false) // adapter unsubscribed
  })

  it('returns seq: 0 when adapter omits seq', async () => {
    const adapter: PubSubAdapter = {
      subscribe() {},
      unsubscribe() {},
      publish() {
        return {} // no seq, no ts
      },
    }
    const registry = new DefaultPubSubRegistry(adapter)
    setPubSubRegistry(registry)

    const channel = createChannel<{ text: string }, { text: string }>({ key: 'room:no-seq' })
    ;(channel as any)._registerChannel()

    const receipt = await channel.publish({ text: 'hello' })
    expect(receipt.seq).toBe(0)
    expect(receipt.ts).toEqual(expect.any(Number))
  })
})
