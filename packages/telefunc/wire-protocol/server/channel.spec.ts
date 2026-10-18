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

test.each(['text', 'binary'] as const)(
  "an ack answer to a %s message that can't be serialized rejects the page's send as a listener's throw does, and the channel stays open",
  async (kind) => {
    vi.useFakeTimers()
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const { channel } = page('sse')
      const server = register()
      const client = channel(server.id)
      const cyclic: { self?: unknown } = {}
      cyclic.self = cyclic
      server.listen(() => cyclic)
      server.listenBinary(() => cyclic)
      await vi.advanceTimersByTimeAsync(100)
      const sent =
        kind === 'text' ? client.send('question', { ack: true }) : client.sendBinary(new Uint8Array(1), { ack: true })
      const answered = sent.then(
        () => 'resolved',
        (err: Error) => err.message,
      )
      await vi.advanceTimersByTimeAsync(10_000)
      expect(await answered).toBe('Internal Server Error — see server logs')
      expect(reported).toHaveBeenCalledOnce()
      expect([client.isClosed, server.isClosed]).toEqual([false, false])
      client.abort()
    } finally {
      vi.useRealTimers()
    }
  },
)
