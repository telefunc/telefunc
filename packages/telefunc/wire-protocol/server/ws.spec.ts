import { afterEach, expect, test, vi } from 'vitest'
import type { Peer } from 'crossws'
import { getTelefuncChannelHooks } from './ws.js'
import { getChannelMux } from './mux.js'
import { ServerChannel } from './channel.js'
import { encode } from '../shared-ws.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { ChannelOverflowError } from '../channel-errors.js'
import { CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_MAX_BYTES } from '../constants.js'

afterEach(() => {
  vi.useRealTimers()
})

test("a client that goes silent is detached at its ping deadline, though the peer's terminate() never closes", async () => {
  vi.useFakeTimers()
  const hooks = getTelefuncChannelHooks()
  // A Durable Object peer: terminate() starts a close handshake the vanished client never answers, and its socket
  // reports no bufferedAmount.
  const peer = { context: {}, websocket: {}, send() {}, terminate() {} } as unknown as Peer
  const channel = new ServerChannel<string, string>()
  let closed = false
  channel.onClose(() => {
    closed = true
  })
  getChannelMux().registerChannel(channel)
  await hooks.open!(peer)
  const reconcile = encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] })
  await hooks.message!(peer, { uint8Array: () => reconcile } as never)
  const { pingInterval, reconnectTimeout } = getServerConfig().channel
  await vi.advanceTimersByTimeAsync(2 * pingInterval + reconnectTimeout + 1_000)
  expect(closed).toBe(true)
})

/** A channel attached to a page that reads nothing, through a peer whose `websocket` is the runtime's socket. */
async function attachToStalledPage(websocket: (written: () => number) => object) {
  const hooks = getTelefuncChannelHooks()
  let written = 0
  const peer = {
    context: {},
    websocket: websocket(() => written),
    send(frame: Uint8Array) {
      written += frame.byteLength
    },
    terminate() {},
  } as unknown as Peer
  const channel = new ServerChannel<unknown, string>()
  getChannelMux().registerChannel(channel)
  await hooks.open!(peer)
  const reconcile = encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] })
  await hooks.message!(peer, { uint8Array: () => reconcile } as never)
  return { channel, written: () => written }
}

/** 16 KiB sends, none awaited, until one rejects. */
async function sendUntilRejected(channel: ServerChannel<unknown, string>, maxSends = 6_000) {
  let error: unknown
  let sends = 0
  while (error === undefined && sends < maxSends) {
    channel.send(String(sends++).padEnd(16 * 1024)).catch((err: unknown) => (error = err))
    await Promise.resolve()
  }
  return { error, sends }
}

/** One message past the page's window and the largest window a page grants, and the headers of each message and of
 *  its pieces. */
const BOUND = CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES + 192 * 1024

test("a page that stops reading its Node or Deno socket holds what a channel sends nobody awaits to the page's window and the largest window a page grants: the next send rejects with ChannelOverflowError", async () => {
  // What is written stays in the socket.
  const { channel, written } = await attachToStalledPage((written) => ({
    get bufferedAmount() {
      return written()
    },
  }))
  const { error } = await sendUntilRejected(channel)
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(written()).toBeLessThanOrEqual(BOUND)
  expect(channel.isClosed).toBe(false)
})

test("a Durable Object's socket reports no bufferedAmount, so the page's window and the largest window a page grants alone bound what a channel sends nobody awaits", async () => {
  const { channel, written } = await attachToStalledPage(() => ({}))
  const { error } = await sendUntilRejected(channel)
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(written()).toBeLessThanOrEqual(BOUND)
})

test('a socket that writes everything out at once holds nothing for the page, so a channel refuses no send nobody awaits', async () => {
  const { channel, written } = await attachToStalledPage(() => ({ bufferedAmount: 0 }))
  const { error } = await sendUntilRejected(channel, (3 * BOUND) / (16 * 1024))
  expect(error).toBeUndefined()
  expect(written()).toBeGreaterThan(2 * BOUND)
})
