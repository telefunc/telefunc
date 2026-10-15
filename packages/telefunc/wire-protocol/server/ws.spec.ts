import { afterEach, expect, test, vi } from 'vitest'
import type { Peer } from 'crossws'
import { getTelefuncChannelHooks } from './ws.js'
import { getChannelMux } from './mux.js'
import { ServerChannel } from './channel.js'
import { LENGTH_BYTES, TAG, decode, encode, isOneFrame, withTook } from '../shared-ws.js'
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

test('the server hands a socket it closed nothing more, though it answered a frame in the same turn: workerd throws on a send after close', async () => {
  const hooks = getTelefuncChannelHooks()
  let closed = false
  let sentAfterClose = 0
  const peer = {
    context: {},
    websocket: {},
    send() {
      if (closed) sentAfterClose++
    },
    terminate() {
      closed = true
    },
  } as unknown as Peer
  await hooks.open!(peer)
  await hooks.message!(peer, { uint8Array: () => encode.reconcile({ open: [] }) } as never)
  // The PONG the PING is answered with is gathered for a message the turn sends as it ends.
  void hooks.message!(peer, { uint8Array: () => withTook(encode.ping(), 0) } as never)
  // A piece of a frame larger than any the server takes ends the wire as it arrives.
  void hooks.message!(peer, { uint8Array: () => encode.piece(0xffffffff, new Uint8Array(1)) } as never)
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(closed).toBe(true)
  expect(sentAfterClose).toBe(0)
})

test("a listener that transfers its data's buffer leaves the frames behind that data in its message intact", async () => {
  const hooks = getTelefuncChannelHooks()
  let terminated = false
  const peer = {
    context: {},
    websocket: {},
    send() {},
    terminate() {
      terminated = true
    },
  } as unknown as Peer
  const channel = new ServerChannel()
  getChannelMux().registerChannel(channel)
  await hooks.open!(peer)
  const reconcile = encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] })
  await hooks.message!(peer, { uint8Array: () => reconcile } as never)
  const got: number[] = []
  channel.listenBinary((data) => {
    got.push(data.byteLength)
    structuredClone(data.buffer, { transfer: [data.buffer] })
  })
  const frames = [encode.binary(0, new Uint8Array(100), 1), encode.window(0, 1 << 20, 0)]
  const message = encode.message(frames, LENGTH_BYTES + frames.reduce((bytes, frame) => bytes + frame.byteLength, 0))
  // Deno, Bun and workerd hand the server each message in an ArrayBuffer of its own.
  await hooks.message!(peer, { uint8Array: () => message } as never)
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(got).toEqual([100])
  expect(terminated).toBe(false)
})

test("on Node.js, a listener's data from a message of several frames has a buffer of its own frame, which it may transfer", async () => {
  const hooks = getTelefuncChannelHooks()
  let terminated = false
  const peer = {
    context: {},
    websocket: {},
    send() {},
    terminate() {
      terminated = true
    },
  } as unknown as Peer
  const channel = new ServerChannel()
  getChannelMux().registerChannel(channel)
  await hooks.open!(peer)
  const reconcile = encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] })
  await hooks.message!(peer, { uint8Array: () => reconcile } as never)
  const buffers: number[] = []
  channel.listenBinary((data) => {
    buffers.push(data.buffer.byteLength)
    structuredClone(data.buffer, { transfer: [data.buffer] })
  })
  const binary = encode.binary(0, new Uint8Array(100), 1)
  const frames = [binary, encode.window(0, 1 << 20, 0), encode.window(0, 1 << 20, 0)]
  const encoded = encode.message(frames, LENGTH_BYTES + frames.reduce((bytes, frame) => bytes + frame.byteLength, 0))
  // ws hands a Node.js server each message as a `Buffer`.
  const message = Buffer.alloc(encoded.byteLength)
  message.set(encoded)
  await hooks.message!(peer, { uint8Array: () => message } as never)
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(buffers).toEqual([binary.byteLength])
  expect(terminated).toBe(false)
})

/** A channel attached to a page that reads nothing, or, `acknowledging`, only what tells the server what arrived,
 *  through a peer whose `websocket` is the runtime's socket. */
async function attachToStalledPage(websocket: (written: () => number) => object, { acknowledging = false } = {}) {
  const hooks = getTelefuncChannelHooks()
  let written = 0
  let took = 0
  const peer = {
    context: {},
    websocket: websocket(() => written),
    send(message: Uint8Array) {
      written += message.byteLength
      if (!acknowledging) return
      // A message of several frames holds whole ones.
      const frame = isOneFrame(message) ? decode(message) : null
      if (frame?.tag === TAG.ANNOUNCE) return
      const bytes = frame?.tag === TAG.PIECE ? frame.piece.byteLength : message.byteLength
      if (bytes === 0) return
      took += bytes
      const ack = encode.piecesAck(took, 0, 0)
      queueMicrotask(() => void hooks.message!(peer, { uint8Array: () => ack } as never))
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

test('a socket that writes everything out at once to a page that acknowledges what arrives holds nothing for it, so a channel refuses no send nobody awaits', async () => {
  const { channel, written } = await attachToStalledPage(() => ({ bufferedAmount: 0 }), { acknowledging: true })
  const { error } = await sendUntilRejected(channel, (3 * BOUND) / (16 * 1024))
  expect(error).toBeUndefined()
  expect(written()).toBeGreaterThan(2 * BOUND)
})
