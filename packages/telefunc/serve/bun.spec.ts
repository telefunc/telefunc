import { afterEach, expect, test, vi } from 'vitest'
import { Telefunc } from './bun.js'
import { getChannelMux } from '../wire-protocol/server/mux.js'
import { ServerChannel } from '../wire-protocol/server/channel.js'
import { ChannelOverflowError } from '../wire-protocol/channel-errors.js'
import { encode } from '../wire-protocol/shared-ws.js'
import { CHANNEL_BUFFER_LIMIT_BYTES, CREDIT_WINDOW_INITIAL_BYTES } from '../wire-protocol/constants.js'

afterEach(() => {
  vi.unstubAllGlobals()
})

test("Bun's WebSocket handler queues every frame instead of dropping past its default 16 MiB limit", () => {
  vi.stubGlobal('Bun', {}) // crossws's Bun adapter checks it runs on Bun
  expect(new Telefunc().websocket).toMatchObject({ backpressureLimit: 0 })
})

/** A channel attached through a Bun socket whose page reads nothing, and whose getBufferedAmount() is `buffered`. */
async function attach(buffered: (written: number) => number) {
  vi.stubGlobal('Bun', {})
  const { websocket } = new Telefunc()
  let written = 0
  const socket = {
    data: { context: {}, namespace: '/_telefunc', request: new Request('http://localhost/_telefunc') },
    send(frame: Uint8Array) {
      written += frame.byteLength
      return -1
    },
    // Bun's own methods refuse any `this` but the socket.
    getBufferedAmount() {
      if (this !== socket) throw new TypeError('Expected this to be instanceof ServerWebSocket')
      return buffered(written)
    },
    terminate() {},
  }
  const channel = new ServerChannel<unknown, string>()
  let opened = false
  channel.onOpen(() => void (opened = true))
  getChannelMux().registerChannel(channel)
  websocket.open!(socket as never)
  websocket.message!(
    socket as never,
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  await vi.waitFor(() => expect(opened).toBe(true))
  return { channel, written: () => written }
}

/** 16 KiB sends, none awaited, until one rejects. */
async function sendUntilRejected(channel: ServerChannel<unknown, string>, maxSends = 2_000) {
  let error: unknown
  for (let n = 0; error === undefined && n < maxSends; n++) {
    channel.send(String(n).padEnd(16 * 1024)).catch((err: unknown) => (error = err))
    await Promise.resolve()
  }
  return error
}

test("a page that stops reading its Bun socket holds what a channel sends nobody awaits to the page's window and bufferLimit: the next send rejects with ChannelOverflowError", async () => {
  const { channel, written } = await attach((written) => written)
  expect(await sendUntilRejected(channel)).toBeInstanceOf(ChannelOverflowError)
  expect(written()).toBeLessThanOrEqual(CREDIT_WINDOW_INITIAL_BYTES + CHANNEL_BUFFER_LIMIT_BYTES + 32 * 1024)
})

test('a Bun socket that writes everything out at once holds nothing for the page, so a channel refuses no send nobody awaits', async () => {
  const { channel } = await attach(() => 0)
  expect(await sendUntilRejected(channel, 400)).toBeUndefined()
})
