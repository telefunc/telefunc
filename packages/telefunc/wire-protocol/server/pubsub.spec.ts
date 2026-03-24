import { afterEach, describe, expect, it } from 'vitest'
import { createChannel } from './channel.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { TAG, decode } from '../shared-ws.js'
import { IndexedPeer } from './IndexedPeer.js'
import { getPubSubTransport, setPubSubTransport } from './pubsub.js'

const previousPubSubTransport = getPubSubTransport()

afterEach(() => {
  setPubSubTransport(previousPubSubTransport)
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
    expect(secondReceipt).toMatchObject({
      key: 'room:test:ack',
      seq: 2,
    })
    expect(firstReceipt.ts).toEqual(expect.any(Number))
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
