import { afterEach, expect, test, vi } from 'vitest'
import { ServerChannel } from './channel.js'
import { ERROR_REASON, ProtocolViolationError, TAG } from '../shared-ws.js'
import { ChannelOverflowError } from '../channel-errors.js'
import { CHANNEL_BUFFER_LIMIT_BINARY_BYTES, CHANNEL_BUFFER_LIMIT_BYTES } from '../constants.js'
import { config } from '../../client/clientConfig.js'
import { page, register } from '../test/page-server.js'

afterEach(() => {
  delete config.fetch
  vi.restoreAllMocks()
})

test("a channel listener that stops listening itself doesn't make the next one miss the message", () => {
  const channel = new ServerChannel<string, never>()
  const seen: string[] = []
  const unlisten = channel.listen((message) => {
    seen.push(`once:${message}`)
    unlisten()
  })
  channel.listen((message) => void seen.push(`other:${message}`))
  channel._onPeerMessage(JSON.stringify('one'), 5)
  channel._onPeerMessage(JSON.stringify('two'), 5)
  expect(seen).toEqual(['once:one', 'other:one', 'other:two'])
  channel.abort()
})

test("a page's ERROR is a protocol violation unless its reason is a replay loss", () => {
  const channel = new ServerChannel()
  const error = (seq: number, reason: number) => ({ tag: TAG.ERROR, index: 0, seq, reason }) as const
  expect(() => channel._dispatchFrame(error(1, ERROR_REASON.BUG))).toThrow(ProtocolViolationError)
  expect(channel.isClosed).toBe(false)
  channel._dispatchFrame(error(2, ERROR_REASON.LOST))
  expect(channel.isClosed).toBe(true)
})

test.each(['text', 'binary'] as const)(
  'a flood of empty %s sends to a channel no page attached to holds its budget, each send with what it retains: the oldest reject with ChannelOverflowError',
  async (kind) => {
    const channel = new ServerChannel<never, string>()
    let overflowed = 0
    const count = 100_000
    for (let i = 0; i < count; i++) {
      const sent = kind === 'text' ? channel.send('') : channel.sendBinary(new Uint8Array(0))
      sent.catch((err: unknown) => void (err instanceof ChannelOverflowError && overflowed++))
    }
    await Promise.resolve()
    // Each counts 512 bytes besides its own, which for text is the 2 of '""'.
    const held =
      kind === 'text' ? Math.floor(CHANNEL_BUFFER_LIMIT_BYTES / 514) : CHANNEL_BUFFER_LIMIT_BINARY_BYTES / 512
    expect(count - overflowed).toBe(held)
    channel.abort()
  },
)

test.each([false, true])(
  'a binary send with ack %s larger than the buffer of a channel no page attached to is refused before its bytes are copied, and one that fits is copied once',
  async (ack) => {
    const channel = new ServerChannel<never, string>()
    const refused = new Uint8Array(CHANNEL_BUFFER_LIMIT_BINARY_BYTES + 1)
    const held = new Uint8Array(1)
    const copies: string[] = []
    const send = (data: Uint8Array) => (ack ? channel.sendBinary(data, { ack: true }) : channel.sendBinary(data))
    const Bytes = globalThis.Uint8Array
    globalThis.Uint8Array = new Proxy(Bytes, {
      construct(target, args, newTarget) {
        if (args[0] === refused) copies.push('refused')
        if (args[0] === held) copies.push('held')
        return Reflect.construct(target, args, newTarget)
      },
    })
    let refusal: Promise<unknown>
    try {
      refusal = send(refused).catch((err: unknown) => err)
      send(held).catch(() => {})
    } finally {
      globalThis.Uint8Array = Bytes
    }
    expect(await refusal).toBeInstanceOf(ChannelOverflowError)
    expect(copies).toEqual(['held'])
    channel.abort()
  },
)

/** What the page's send of a `kind` message with an ack settles with, and both ends' isClosed after it. */
async function ackAnswer(kind: 'text' | 'binary', listen: (server: ServerChannel) => void = () => {}) {
  vi.useFakeTimers()
  try {
    const { channel } = page('sse')
    const server = register()
    const client = channel(server.id)
    listen(server)
    await vi.advanceTimersByTimeAsync(100)
    const sent =
      kind === 'text' ? client.send('question', { ack: true }) : client.sendBinary(new Uint8Array(1), { ack: true })
    const answered = sent.then(
      () => 'resolved',
      (err: Error) => String(err),
    )
    await vi.advanceTimersByTimeAsync(10_000)
    const closed = [client.isClosed, server.isClosed]
    client.abort()
    return { answer: await answered, closed }
  } finally {
    vi.useRealTimers()
  }
}

test.each(['text', 'binary'] as const)(
  "an ack answer to a %s message that can't be serialized rejects the page's send as a listener's throw does, and the channel stays open",
  async (kind) => {
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
    const cyclic: { self?: unknown } = {}
    cyclic.self = cyclic
    const { answer, closed } = await ackAnswer(kind, (server) => {
      server.listen(() => cyclic)
      server.listenBinary(() => cyclic)
    })
    expect(answer).toBe('Error: Internal Server Error — see server logs')
    expect(reported).toHaveBeenCalledOnce()
    expect(closed).toEqual([false, false])
  },
)

test.each(['text', 'binary'] as const)(
  "a %s message with an ack the server has no listener for rejects the page's send, and the channel stays open",
  async (kind) => {
    const { answer, closed } = await ackAnswer(kind)
    expect(answer).toBe('Error: No listener registered for ack request')
    expect(closed).toEqual([false, false])
  },
)

test("a data shield refusing a text message with an ack rejects the page's send with its verdict, and checks no binary one", async () => {
  const shielded = (server: ServerChannel) => {
    server._validators.set('data', () => 'not a question')
    server.listen(() => 'answer')
    server.listenBinary(() => 'answer')
  }
  expect((await ackAnswer('text', shielded)).answer).toBe('ShieldValidationError: not a question')
  expect((await ackAnswer('binary', shielded)).answer).toBe('resolved')
})
