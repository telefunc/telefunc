import { Readable } from 'node:stream'
import { beforeAll, describe, expect, test, vi } from 'vitest'
import { loadStreamNodeModuleOnce } from '../../utils/loadStreamNodeModule.js'
import { STREAM_TRANSPORT } from '../../wire-protocol/constants.js'
import { ServerChannel } from '../../wire-protocol/server/channel.js'
import { getContext } from './context/getContext.js'
import { createRequestContext, REQUEST_CONTEXT } from './context/requestContext.js'
import { serializeTelefunctionResult } from './runTelefunc/serializeTelefunctionResult.js'

beforeAll(loadStreamNodeModuleOnce)

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => (resolve = r))
  return { promise, resolve }
}

function createResponse(useNodeStream: boolean, wakeOn: 'onClose' | 'signal' = 'signal') {
  const controller = new AbortController()
  const requestContext = createRequestContext(new Request('http://localhost/_telefunc', { signal: controller.signal }))
  const onClose = vi.fn()
  requestContext.onClose(onClose)
  const waiting = deferred()
  const event = deferred()
  const finished = deferred()
  const watchers = new Set<() => void>()
  async function* snapshots(initial = 'snapshot') {
    const context = getContext()
    watchers.add(event.resolve)
    if (wakeOn === 'onClose') context.onClose(event.resolve)
    else context.signal.addEventListener('abort', event.resolve, { once: true })
    try {
      yield initial
      waiting.resolve()
      await event.promise
    } finally {
      watchers.delete(event.resolve)
      context.signal.removeEventListener('abort', event.resolve)
      finished.resolve()
    }
  }
  function serialize(value: unknown) {
    const result = serializeTelefunctionResult({
      telefunctionReturn: value,
      telefunctionName: 'snapshots',
      telefuncFilePath: '/snapshots.telefunc.ts',
      telefunctionAborted: false,
      context: { [REQUEST_CONTEXT]: requestContext },
      requestContext,
      abortSignal: controller.signal,
      streamTransport: STREAM_TRANSPORT.BINARY_INLINE,
      useNodeStream,
      serverConfig: { log: { shieldErrors: { dev: false, prod: false } } },
    })
    if (result.type !== 'streaming') throw new Error('Expected an inline response')
    const body = result.body
    const reader = body instanceof Readable ? null : body.getReader()
    return {
      body,
      cancel: async () => {
        if (body instanceof Readable) body.destroy()
        else await reader!.cancel()
      },
      drain: async () => {
        const chunks: Uint8Array[] = []
        if (body instanceof Readable) {
          // Flowing mode allows destroy() without an async iterator's AbortError.
          await new Promise<void>((resolve) => {
            body.on('data', (chunk: Uint8Array) => chunks.push(chunk))
            body.once('close', resolve)
          })
        } else {
          while (true) {
            const { value, done } = await reader!.read()
            if (done) break
            chunks.push(value)
          }
        }
        return Buffer.concat(chunks)
      },
    }
  }
  return { controller, requestContext, onClose, waiting, event, finished, watchers, snapshots, serialize }
}

describe.each([true, false])('inline cancellation (useNodeStream=%s)', (useNodeStream) => {
  test.each(['onClose', 'signal'] as const)(
    'cancelling an idle generator wakes %s and releases resources',
    async (wakeOn) => {
      const fixture = createResponse(useNodeStream, wakeOn)
      const response = fixture.serialize(fixture.snapshots())
      const drained = response.drain()
      try {
        await fixture.waiting.promise
        expect(fixture.watchers.size).toBe(1)
        await response.cancel()
        expect(fixture.requestContext.signal.aborted).toBe(true)
        await fixture.finished.promise
        expect(fixture.watchers.size).toBe(0)
        expect(fixture.onClose).toHaveBeenCalledTimes(1)
      } finally {
        fixture.event.resolve()
        await response.cancel()
        await drained
      }
    },
  )

  test('request abort wakes an idle generator', async () => {
    const fixture = createResponse(useNodeStream)
    const response = fixture.serialize(fixture.snapshots())
    const drained = response.drain()
    try {
      await fixture.waiting.promise
      fixture.controller.abort()
      expect(fixture.requestContext.signal.aborted).toBe(true)
      await fixture.finished.promise
      expect(fixture.watchers.size).toBe(0)
      expect(fixture.onClose).toHaveBeenCalledTimes(1)
    } finally {
      fixture.event.resolve()
      await response.cancel()
      await drained
    }
  })

  test('cancellation and later completion preserve independently active channels', async () => {
    const fixture = createResponse(useNodeStream)
    const channels = [new ServerChannel(), new ServerChannel()]
    const response = fixture.serialize({ snapshots: fixture.snapshots(), channels })
    const drained = response.drain()
    try {
      await fixture.waiting.promise
      await response.cancel()
      expect(fixture.requestContext.signal.aborted).toBe(false)
      expect(channels.every((channel) => !channel.isClosed)).toBe(true)
      // Let the merge loop's finally release the same hold a second time.
      fixture.event.resolve()
      await fixture.finished.promise
      await new Promise<void>((resolve) => setTimeout(resolve, 0))
      await channels[0]!.close({ timeout: 0 })
      expect(fixture.requestContext.signal.aborted).toBe(false)
      expect(channels[1]!.isClosed).toBe(false)
      expect(fixture.onClose).not.toHaveBeenCalled()
      await channels[1]!.close({ timeout: 0 })
      expect(fixture.requestContext.signal.aborted).toBe(true)
      expect(fixture.onClose).toHaveBeenCalledTimes(1)
    } finally {
      fixture.event.resolve()
      await response.cancel()
      await Promise.all(channels.map((channel) => channel.close({ timeout: 0 })))
      await drained
    }
  })

  test('closing the last channel wakes the cancelled inline generator', async () => {
    const fixture = createResponse(useNodeStream)
    const channel = new ServerChannel()
    const response = fixture.serialize({ snapshots: fixture.snapshots(), channel })
    const drained = response.drain()
    try {
      await fixture.waiting.promise
      await response.cancel()
      expect(fixture.requestContext.signal.aborted).toBe(false)
      await channel.close({ timeout: 0 })
      expect(fixture.requestContext.signal.aborted).toBe(true)
      await fixture.finished.promise
      expect(fixture.watchers.size).toBe(0)
      expect(fixture.onClose).toHaveBeenCalledTimes(1)
    } finally {
      fixture.event.resolve()
      await response.cancel()
      await channel.close({ timeout: 0 })
      await drained
    }
  })

  test.each(['immediate', 'backpressure'])('%s cancellation releases the lifecycle hold', async (timing) => {
    const fixture = createResponse(useNodeStream)
    const response = fixture.serialize(fixture.snapshots('x'.repeat(256 * 1024)))
    if (timing === 'backpressure') {
      // No reads: the Node payload / Web header fills the sink's buffer.
      await new Promise<void>((resolve) => setTimeout(resolve, 0))
    }
    try {
      await response.cancel()
      expect(fixture.requestContext.signal.aborted).toBe(true)
      await fixture.finished.promise
      expect(fixture.watchers.size).toBe(0)
      expect(fixture.onClose).toHaveBeenCalledTimes(1)
    } finally {
      fixture.event.resolve()
      await response.cancel()
    }
  })

  test('normal completion preserves response bytes and closes once', async () => {
    const fixture = createResponse(useNodeStream)
    const response = fixture.serialize(fixture.snapshots())
    const drained = response.drain()
    await fixture.waiting.promise
    expect(fixture.requestContext.signal.aborted).toBe(false)
    fixture.event.resolve()
    const bytes = await drained
    // Captured from the unmodified inline response implementation.
    expect(bytes.toString('hex')).toBe(
      '0000002c7b22726574223a222154656c6566756e6347656e657261746f723a7b5c225f5f696e6465785c223a307d227d0000000b0022736e617073686f7422000000010000000000',
    )
    expect(fixture.watchers.size).toBe(0)
    expect(fixture.requestContext.signal.aborted).toBe(true)
    expect(fixture.onClose).toHaveBeenCalledTimes(1)
  })
})
