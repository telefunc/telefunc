import { assert, describe, expect, onTestFinished, test, vi } from 'vitest'
import { defined } from '../../utils/defined.fixture.js'
import { channelLedger } from '../../wire-protocol/channel/server/channelMemory.fixture.js'

import { serve } from './telefunc.js'
import { config } from './serverConfig.js'
import { shield } from './shield.js'
import { Abort } from './Abort.js'
import { Channel, ServerChannel } from '../../wire-protocol/channel/server/ServerChannel.js'
import { IndexedPeer } from '../../wire-protocol/channel/server/IndexedPeer.js'
import { encode } from '../../wire-protocol/wire/frame.js'
import { decodeChannelFrame } from '../../wire-protocol/wire/decode.fixture.js'
import { holdsAtMost } from '../../wire-protocol/channel/server/channelMemory.js'

/** Each request's lifecycle, as `runTelefunc` creates it: how often its `onClose()` callbacks ran, and its `signal`. */
const requests = vi.hoisted((): { closed: number; signal: AbortSignal }[] => [])
vi.mock('./context/requestContext.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('./context/requestContext.js')>()
  return {
    ...original,
    createRequestContext(request: Request) {
      const context = original.createRequestContext(request)
      const lifecycle = { closed: 0, signal: context.signal }
      context.onClose(() => lifecycle.closed++)
      requests.push(lifecycle)
      return context
    },
  }
})

// The telefunctions below are this app's only file.
vi.mock('./runTelefunc/loadTelefuncFiles.js', () => ({
  loadTelefuncFiles: async () => ({ telefuncFilesLoaded: { [FILE]: telefunctions }, telefuncFilesAll: [FILE] }),
}))

const FILE = '/requestLifecycle.telefunc.ts'
config.disableNamingConvention = true
// One channel may hold 8 MiB, so a row can send past that quickly.
config.channel = { maxPendingBytes: 8 * 1024 * 1024 }
/** What this server's channels hold, on the ledger every channel shares. */
const ledgerTotal = () => channelLedger().total
/** What the sends waiting for room reserve, on that ledger. */
const ledgerReserved = () => channelLedger().reserved
/** The last channel a telefunction made, how often a stream it returned was cancelled, and what lets a waiting one return. */
const opened: { channel: Channel<never, string> | null; cancelled: number; letReturn: (() => void) | null } = {
  channel: null,
  cancelled: 0,
  letReturn: null,
}
/** A channel holding a 64 KiB send. */
function heldChannel() {
  opened.channel = new Channel<never, string>()
  void opened.channel.send('x'.repeat(64 * 1024), { lossy: true })
  return opened.channel
}
function circular() {
  const value: Record<string, unknown> = {}
  value.self = value
  return value
}
const telefunctions: Record<string, (...args: never[]) => Promise<unknown>> = {
  onSuccess: async () => 42,
  onThrow: async () => {
    throw new Error('the telefunction fails, as the test means it to')
  },
  onSendsPastItsChannelThenThrows: async () => {
    const channel = new Channel<never, string>()
    // Each over half what one channel may hold: the second waits for room.
    const half = 'x'.repeat(holdsAtMost() / 2)
    void channel.send(half)
    void channel.send(half)
    throw new Error('the telefunction fails, as the test means it to')
  },
  onCircular: async () => circular(),
  onChannelThenCircular: async () => ({ channel: heldChannel(), bad: circular() }),
  onSameChannelThenCircular: async () => ({ channel: opened.channel, bad: circular() }),
  onSameChannel: async () => ({ channel: opened.channel }),
  onChannelThenThrowingGetter: async () => ({
    channel: heldChannel(),
    get broken() {
      throw new Error('the getter fails, as the test means it to')
    },
  }),
  onStreamThenCircular: async () => ({
    stream: new ReadableStream({
      cancel() {
        opened.cancelled++
      },
    }),
    bad: circular(),
  }),
  onStreamThenUnreadableStream: async () => {
    const unreadable = new ReadableStream()
    unreadable.getReader()
    return {
      channel: heldChannel(),
      stream: new ReadableStream({
        cancel() {
          opened.cancelled++
        },
      }),
      unreadable,
    }
  },
  onAbortWhileStreamWaits: async () => {
    const control = new Channel()
    control.listen(() => {
      throw Abort()
    })
    return {
      control: control.client,
      stream: new ReadableStream({
        pull(controller) {
          controller.enqueue(new Uint8Array(2 * 1024 * 1024))
        },
        cancel() {
          opened.cancelled++
        },
      }),
    }
  },
  onAbortWithStream: async () => {
    throw Abort({
      stream: new ReadableStream({
        cancel() {
          opened.cancelled++
        },
      }),
    })
  },
  onStreamOnceLetReturn: async () => {
    await new Promise<void>((resolve) => (opened.letReturn = resolve))
    return new ReadableStream({
      cancel() {
        opened.cancelled++
      },
    })
  },
  onChannelOnceLetReturn: async () => {
    await new Promise<void>((resolve) => (opened.letReturn = resolve))
    return heldChannel()
  },
  onChannel: async () => heldChannel(),
  onText: shield(async (_text: string) => 42, [shield.type.string]),
  onStream: async () =>
    new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array([1]))
        controller.close()
      },
    }),
}

/** A page attached to `channel` on `index` that reads nothing more of it: what's sent past its first credit waits. */
function pageThatStopsReading(channel: ServerChannel, index: number) {
  return new IndexedPeer({
    sender: {
      sendPublish() {},
      send() {},
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => Number.POSITIVE_INFINITY,
      deliveredBefore: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      queue: { waitingPublishes: () => 0, dropUngiven: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
    },
    index,
    replay: defined(channel._replayBuffer),
  })
}

function call(
  name: string,
  args: unknown[] = [],
  { signal, transport }: { signal?: AbortSignal; transport?: string } = {},
) {
  return serve({
    request: new Request('https://example.test/_telefunc', {
      method: 'POST',
      headers: { 'x-telefunc-request': 'txt' },
      body: JSON.stringify({ file: FILE, name, args, stream: transport && { transport } }),
      signal,
    }),
  })
}

// A page may hold 4,096 channels on each of its connections (`connectionKey`), and a call carries the wants of all.
test('a call carrying the wants of more channels than one connection holds runs', async () => {
  const wants = Array.from({ length: 4_097 }, () => ({
    channel: crypto.randomUUID(),
    version: 1,
    text: true,
    binary: false,
  }))
  const response = await serve({
    request: new Request('https://example.test/_telefunc', {
      method: 'POST',
      headers: { 'x-telefunc-request': 'txt' },
      body: JSON.stringify({ file: FILE, name: 'onSuccess', args: [], wants }),
    }),
  })
  expect(response.statusCode).toBe(200)
})

describe('a request ends its lifecycle once, however the call ends', () => {
  for (const [name, args, statusCode] of [
    ['onSuccess', [], 200],
    ['onThrow', [], 500],
    ['onCircular', [], 500],
    ['onText', [1], 422],
    ['onMissing', [], 400],
  ] as const) {
    test(`${name}(): ${statusCode}`, async () => {
      const before = requests.length
      const response = await call(name, [...args])
      await response.getBody()
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(response.statusCode).toBe(statusCode)
      expect(requests.length).toBe(before + 1)
      const request = defined(requests.at(-1))
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
    })
  }

  test('onSendsPastItsChannelThenThrows(): 500, and the send its channel had waiting for room reserves nothing', async () => {
    const before = ledgerReserved()
    const response = await call('onSendsPastItsChannelThenThrows')
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(response.statusCode).toBe(500)
    expect(ledgerReserved()).toBe(before)
  })

  for (const name of ['onChannelThenCircular', 'onChannelThenThrowingGetter']) {
    test(`${name}(): 500, and the channel it returned before the failure closes with it`, async () => {
      const before = ledgerTotal()
      const response = await call(name)
      await response.getBody()
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(response.statusCode).toBe(500)
      const request = defined(requests.at(-1))
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
      expect(defined(opened.channel).isClosed).toBe(true)
      expect(ledgerTotal()).toBe(before)
    })
  }

  for (const name of ['onSameChannelThenCircular', 'onSameChannel']) {
    test(`${name}(): 500, and the channel an earlier call returned stays open`, async () => {
      await (await call('onChannel')).getBody()
      const earlier = defined(requests.at(-1))
      const channel = defined(opened.channel)
      const response = await call(name)
      await response.getBody()
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(response.statusCode).toBe(500)
      const request = defined(requests.at(-1))
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
      expect(channel.isClosed).toBe(false)
      expect(earlier.closed).toBe(0)
      await channel.close({ timeout: 0 })
      expect(earlier.closed).toBe(1)
    })
  }

  test('onStreamThenCircular(): 500, and the stream it returned before the failure is cancelled', async () => {
    const response = await call('onStreamThenCircular')
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(response.statusCode).toBe(500)
    const request = defined(requests.at(-1))
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
    expect(opened.cancelled).toBe(1)
  })

  for (const transport of ['binary-inline', 'sse-inline', 'channel']) {
    test(`onStreamThenUnreadableStream() over ${transport}: 500, and what it returned before the failure ends with it`, async () => {
      const cancelledBefore = opened.cancelled
      const response = await call('onStreamThenUnreadableStream', [], { transport })
      await response.getBody()
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(response.statusCode).toBe(500)
      const request = defined(requests.at(-1))
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
      expect(opened.cancelled).toBe(cancelledBefore + 1)
      expect(defined(opened.channel).isClosed).toBe(true)
    })
  }

  test('onAbortWithStream(): 403, and the stream in its abort value is cancelled', async () => {
    const cancelledBefore = opened.cancelled
    const response = await call('onAbortWithStream')
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(response.statusCode).toBe(403)
    const request = defined(requests.at(-1))
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
    expect(opened.cancelled).toBe(cancelledBefore + 1)
  })

  for (const [transport, waitsFor] of [
    ['channel', 'credit'],
    ['channel', 'its page'],
    ['binary-inline', 'its page'],
    ['sse-inline', 'its page'],
  ] as const) {
    test(`a response aborted while a stream it returned over ${transport} waits for ${waitsFor} ends at once`, async () => {
      const registered = vi.spyOn(ServerChannel.prototype, '_registerChannel')
      onTestFinished(() => registered.mockRestore())
      const cancelledBefore = opened.cancelled
      const response = await call('onAbortWhileStreamWaits', [], { transport })
      // An inline response is the stream's body, which the page doesn't read.
      if (transport === 'channel') await response.getBody()
      else onTestFinished(() => response.getReadableWebStream().cancel())
      const request = defined(requests.at(-1))
      const [control, stream] = registered.mock.contexts.filter((context) => context instanceof ServerChannel)
      onTestFinished(() => stream?.abort())
      assert(control)
      control._attachPeer(pageThatStopsReading(control, 1))
      if (waitsFor === 'credit') {
        assert(stream)
        stream._attachPeer(pageThatStopsReading(stream, 2))
      }
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(opened.cancelled).toBe(cancelledBefore)
      // The page's message makes the control channel's listener throw `Abort()`, which aborts the response.
      control._dispatchFrame(decodeChannelFrame(encode.text(1, 'null', 1)))
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(opened.cancelled).toBe(cancelledBefore + 1)
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
    })
  }

  test('a channel it returned holds the request till it closes', async () => {
    const response = await call('onChannel')
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    const request = defined(requests.at(-1))
    expect(request.closed).toBe(0)
    expect(request.signal.aborted).toBe(false)
    await defined(opened.channel).close({ timeout: 0 })
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
  })

  /** Calls `name` and lets it return once its page left. */
  async function callThatReturnsAfterItsPageLeft(name: string, transport?: string) {
    opened.letReturn = null
    const page = new AbortController()
    const response = call(name, [], { signal: page.signal, transport })
    const letReturn = await vi.waitFor(() => defined(opened.letReturn, 'a telefunction waiting to return'))
    page.abort()
    const request = defined(requests.at(-1))
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
    letReturn()
    await response
    await new Promise((resolve) => setTimeout(resolve, 10))
  }

  for (const transport of ['binary-inline', 'sse-inline', 'channel']) {
    test(`a stream it returns over ${transport} after its page left is cancelled`, async () => {
      const cancelledBefore = opened.cancelled
      await callThatReturnsAfterItsPageLeft('onStreamOnceLetReturn', transport)
      expect(opened.cancelled).toBe(cancelledBefore + 1)
    })
  }

  test('a channel it returns after its page left closes at once', async () => {
    const before = ledgerTotal()
    await callThatReturnsAfterItsPageLeft('onChannelOnceLetReturn')
    expect(defined(opened.channel).isClosed).toBe(true)
    expect(ledgerTotal()).toBe(before)
  })

  test('a channel it returned stays open when its request aborts after the response', async () => {
    const page = new AbortController()
    const response = await call('onChannel', [], { signal: page.signal })
    await response.getBody()
    page.abort()
    await new Promise((resolve) => setTimeout(resolve, 10))
    const request = defined(requests.at(-1))
    expect(defined(opened.channel).isClosed).toBe(false)
    expect(request.closed).toBe(0)
    await defined(opened.channel).close({ timeout: 0 })
    expect(request.closed).toBe(1)
  })

  test('a stream it returned holds the request till the page read it', async () => {
    const response = await call('onStream')
    const request = defined(requests.at(-1))
    expect(request.closed).toBe(0)
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
  })

  test('onText() with a function argument: 422, and starts closing the function it was passed', async () => {
    const close = vi.spyOn(ServerChannel.prototype, 'close')
    onTestFinished(() => close.mockRestore())
    const channelId = crypto.randomUUID()
    const response = await call('onText', [1, `!TelefuncFunction:${JSON.stringify({ channelId })}`])
    expect(response.statusCode).toBe(422)
    expect(
      close.mock.contexts.filter((context) => context instanceof ServerChannel).map((channel) => channel.id),
    ).toEqual([channelId])
  })
})
