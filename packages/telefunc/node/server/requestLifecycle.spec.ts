import { describe, expect, test, vi } from 'vitest'

import { serve } from './telefunc.js'
import { config } from './serverConfig.js'
import { shield } from './shield.js'
import { Channel } from '../../wire-protocol/channel/server/ServerChannel.js'

/** Each request's lifecycle, as `runTelefunc` creates it: how often its `onClose()` callbacks ran, and its `signal`. */
const requests = vi.hoisted(() => [] as { closed: number; signal: AbortSignal }[])
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
/** What this server's channels hold, on the ledger every channel shares. */
const ledgerTotal = () => (globalThis as any)._telefunc['channelMemory.ts'].total as number
/** The last channel a telefunction made, and how often a stream it returned was cancelled. */
const opened = { channel: null as Channel<never, string> | null, cancelled: 0 }
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
const telefunctions: Record<string, (...args: any[]) => Promise<unknown>> = {
  onSuccess: async () => 42,
  onThrow: async () => {
    throw new Error('the telefunction fails, as the test means it to')
  },
  onCircular: async () => circular(),
  onChannelThenCircular: async () => ({ channel: heldChannel(), bad: circular() }),
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

function call(name: string, args: unknown[] = []) {
  return serve({
    url: 'https://example.test/_telefunc',
    method: 'POST',
    headers: new Headers({ 'x-telefunc-request': 'txt' }),
    body: JSON.stringify({ file: FILE, name, args }),
  })
}

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
      const request = requests.at(-1)!
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
    })
  }

  for (const name of ['onChannelThenCircular', 'onChannelThenThrowingGetter']) {
    test(`${name}(): 500, and the channel it returned before the failure closes with it`, async () => {
      const before = ledgerTotal()
      const response = await call(name)
      await response.getBody()
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(response.statusCode).toBe(500)
      const request = requests.at(-1)!
      expect(request.closed).toBe(1)
      expect(request.signal.aborted).toBe(true)
      expect(opened.channel!.isClosed).toBe(true)
      expect(ledgerTotal()).toBe(before)
    })
  }

  test('onStreamThenCircular(): 500, and the stream it returned before the failure is cancelled', async () => {
    const response = await call('onStreamThenCircular')
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(response.statusCode).toBe(500)
    const request = requests.at(-1)!
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
    expect(opened.cancelled).toBe(1)
  })

  test('a channel it returned holds the request till it closes', async () => {
    const response = await call('onChannel')
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    const request = requests.at(-1)!
    expect(request.closed).toBe(0)
    expect(request.signal.aborted).toBe(false)
    await opened.channel!.close({ timeout: 0 })
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
  })

  test('a stream it returned holds the request till the page read it', async () => {
    const response = await call('onStream')
    const request = requests.at(-1)!
    expect(request.closed).toBe(0)
    await response.getBody()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(request.closed).toBe(1)
    expect(request.signal.aborted).toBe(true)
  })
})
