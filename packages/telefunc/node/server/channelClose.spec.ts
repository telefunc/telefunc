import { describe, expect, test, vi } from 'vitest'

import { ServerReplay } from '../../wire-protocol/channel/server/ServerReplay.js'
import {
  ACK_STATUS,
  ERROR_REASON,
  decode,
  encode,
  type AckResultStatus,
  type ChannelFrame,
} from '../../wire-protocol/wire/frame.js'
import { TAG } from '../../wire-protocol/wire/tags.js'
import { ProtocolViolationError } from '../../wire-protocol/wire/protocolViolation.js'
import { IndexedPeer } from '../../wire-protocol/channel/server/IndexedPeer.js'
import { ServerChannel } from '../../wire-protocol/channel/server/ServerChannel.js'
import { Broadcast, ServerBroadcast } from '../../wire-protocol/channel/server/ServerBroadcast.js'
import { createDeferred } from '../../utils/createDeferred.js'
import {
  ChannelClosedError,
  ChannelCloseUnknownError,
  ChannelOverflowError,
  ChannelPageLeftError,
  rejectedFrameError,
} from '../../wire-protocol/channel/channelErrors.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { isAbort } from '../../shared/Abort.js'
import { CREDIT_MSG_WINDOW_INITIAL, TIMER_DELAY_MAX_MS } from '../../wire-protocol/constants.js'
import { getChannelConfig, reconnectWindow } from './serverConfig.js'
import { onBug } from './runTelefunc/onBug.js'

function createPeer(frames: Uint8Array[]) {
  return new IndexedPeer({
    sender: {
      send(frame) {
        frames.push(frame)
      },
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => 700,
      deliveredBefore: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      queue: { waitingPublishes: () => 0, dropUngiven: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
    },
    index: 7,
    replay: new ServerReplay({ heldBytes: 0 }),
  })
}

/** What the page sends `channel`, through the entry its mux hands frames to; each frame takes the page's next seq. */
function fromPage(channel: Pick<ServerChannel, '_dispatchFrame'>, frames: Uint8Array[]) {
  let seq = 0
  const dispatch = (frame: Uint8Array<ArrayBuffer>) => channel._dispatchFrame(decode(frame) as ChannelFrame)
  const askAck = (text: string) => {
    const ackedSeq = ++seq
    dispatch(encode.textAckReq(7, text, ackedSeq))
    return ackedSeq
  }
  return {
    text(text: string) {
      dispatch(encode.text(7, text, ++seq))
    },
    ackRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK) {
      dispatch(encode.ackRes(7, ++seq, ackedSeq, result, status))
    },
    closeRequest(timeoutMs: number) {
      dispatch(encode.close(7, timeoutMs, ++seq))
    },
    closeAck() {
      dispatch(encode.closeAck(7, ++seq))
    },
    /** The page's side of the close is done; from the page that decides, the channel closed. */
    closeDone() {
      dispatch(encode.closeDone(7, ++seq))
    },
    closeTimedOut() {
      dispatch(encode.error(7, ERROR_REASON.CLOSE_TIMED_OUT, ++seq))
    },
    replayLost() {
      dispatch(encode.error(7, ERROR_REASON.LOST, ++seq))
    },
    abort(abortValue: string) {
      dispatch(encode.abort(7, JSON.stringify(abortValue), ++seq))
    },
    binary(bytes: number) {
      dispatch(encode.binary(7, new Uint8Array(bytes), ++seq))
    },
    /** Asks for an acknowledgement, without waiting for the answer. */
    askAck(text: string): void {
      askAck(text)
    },
    /** Resolves once the channel sent its answer. */
    async ackReq(text: string): Promise<void> {
      const ackedSeq = askAck(text)
      const isAnswer = (frame: Uint8Array) => {
        const decoded = decode(frame)
        return decoded.tag === TAG.ACK_RES && decoded.ackedSeq === ackedSeq
      }
      while (!frames.some(isAnswer)) await new Promise((resolve) => setTimeout(resolve, 1))
    },
  }
}

function expectCloseFrame(frame: Uint8Array) {
  const decoded = decode(frame)
  expect(decoded.tag).toBe(TAG.CLOSE)
  if (decoded.tag !== TAG.CLOSE) throw new Error('Expected CLOSE frame')
  return decoded
}

/** The tags of the frames the channel sent its page. */
function tagsOf(frames: Uint8Array[]): number[] {
  return frames.map((frame) => decode(frame).tag)
}

function expectCloseAckFrame(frame: Uint8Array) {
  const decoded = decode(frame)
  expect(decoded.tag).toBe(TAG.CLOSE_ACK)
  if (decoded.tag !== TAG.CLOSE_ACK) throw new Error('Expected CLOSE_ACK frame')
  expect(decoded.index).toBe(7)
}

// ── Self-initiated close ──

describe('self-initiated close', () => {
  test.each([ACK_STATUS.OK, ACK_STATUS.ABORT])('malformed ack status %s rejects its waiter', async (status) => {
    const channel = new ServerChannel<string, string>({ ack: true })
    const frames: Uint8Array[] = []
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const pending = channel.send('hello', { ack: true })
    const frame = decode(frames[0]!)
    if (frame.tag !== TAG.TEXT_ACK_REQ) throw new Error('Expected TEXT_ACK_REQ')

    expect(() => page.ackRes(frame.seq, '{', status)).toThrow(ProtocolViolationError)

    await expect(pending).rejects.toBeInstanceOf(ProtocolViolationError)
  })

  test('isClosed=true on close(); onClose fires after close-ack roundtrip', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let didFireClose = false
    let closeErr: Error | undefined | null = null

    channel.onClose((err) => {
      didFireClose = true
      closeErr = err
    })

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const closePromise = channel.close()

    expect(channel.isClosed).toBe(true)
    expect(didFireClose).toBe(false)
    expect(channel._phase).not.toBe('ended')

    page.closeAck()
    expect(didFireClose).toBe(true)
    // Its side is done; the page's is not yet.
    expect(channel._phase).not.toBe('ended')
    page.closeDone()
    await expect(closePromise).resolves.toBeNull()
    expect(closeErr).toBeNull()
    expect(channel._phase).toBe('ended')
  })

  test('close() sends close frame and resolves null after ack', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const closePromise = channel.close()

    expect(frames).toHaveLength(1)
    const ctrl = expectCloseFrame(frames[0]!)
    expect(ctrl.index).toBe(7)
    expect(ctrl.timeoutMs).toBeGreaterThanOrEqual(0)

    page.closeAck()
    page.closeDone()
    await expect(closePromise).resolves.toBeNull()
    expect(tagsOf(frames)).toEqual([TAG.CLOSE, TAG.CLOSE_DONE])
    expect(channel._phase).toBe('ended')
  })

  test('close() resolves the timeout error when peer never acks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const closePromise = channel.close({ timeout: 10 })

    expect(channel.isClosed).toBe(true)
    expect(frames).toHaveLength(1)
    expectCloseFrame(frames[0]!)

    await expect(closePromise).resolves.toBeInstanceOf(Error)
    expect(tagsOf(frames)).toEqual([TAG.CLOSE, TAG.ERROR])
    expect(channel._phase).toBe('ended')
  })

  test('the page done before the server: close() resolves null once its own cleanup settled', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    const cleanup = createDeferred<void>()
    channel.onClose(() => cleanup.promise)
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const closePromise = channel.close({ timeout: 1_000 })
    page.closeAck()
    page.closeDone()
    await new Promise((resolve) => setTimeout(resolve, 5))
    expect(channel._phase).not.toBe('ended')
    cleanup.resolve()
    await expect(closePromise).resolves.toBeNull()
    expect(tagsOf(frames)).toEqual([TAG.CLOSE, TAG.CLOSE_DONE])
  })

  test('the page never done: the close times out on both ends, and close() resolves the timeout error', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const closePromise = channel.close({ timeout: 10 })
    page.closeAck()
    await expect(closePromise).resolves.toBeInstanceOf(Error)
    await expect(channel.closed).resolves.toHaveProperty('message', expect.stringContaining('Channel close timed out'))
    const end = frames.map((frame) => decode(frame))[1]
    expect(end?.tag === TAG.ERROR && end.reason).toBe(ERROR_REASON.CLOSE_TIMED_OUT)
  })

  test('close() waits for inflight inbound ack before resolving', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const work = createDeferred<string>()
    channel.listen(() => work.promise)

    // Start inbound ack-req processing
    const inboundPending = page.ackReq('"work"')
    // Its deadline is past any run's: the close ends on the ack.
    const closePromise = channel.close({ timeout: 60_000 })

    expect(channel.isClosed).toBe(true)
    page.closeAck()

    // close() hasn't resolved yet — inflight inbound ack still in progress
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._phase).not.toBe('ended')

    // Inbound work finishes → close resolves
    work.resolve('done')
    await expect(inboundPending).resolves.toBeUndefined()
    page.closeDone()
    await expect(closePromise).resolves.toBeNull()
    expect(channel._phase).toBe('ended')
  })

  test('close() waits for inflight outbound ack before resolving', async () => {
    const channel = new ServerChannel<never, (v: string) => string>({ ack: true, id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)

    const sendPromise = channel.send('hello', { ack: true })
    const closePromise = channel.close({ timeout: 100 })

    expect(channel.isClosed).toBe(true)
    page.closeAck()

    // close() hasn't resolved yet — outbound ack still pending
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._phase).not.toBe('ended')

    // Ack arrives → close resolves
    const ackReqFrame = decode(frames[0]!)
    if (ackReqFrame.tag !== TAG.TEXT_ACK_REQ) throw new Error('Expected TEXT_ACK_REQ')
    page.ackRes(ackReqFrame.seq, '"received"')
    page.closeDone()

    await expect(sendPromise).resolves.toBe('received')
    await expect(closePromise).resolves.toBeNull()
    expect(channel._phase).toBe('ended')
  })

  test('close() waits for async onClose callbacks before resolving', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let didFinishOnClose = false

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.onClose(async () => {
      await new Promise((resolve) => setTimeout(resolve, 30))
      didFinishOnClose = true
    })

    const closePromise = channel.close({ timeout: 100 })
    expect(channel.isClosed).toBe(true)
    page.closeAck()
    page.closeDone()

    // close() hasn't resolved — async onClose still running
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(didFinishOnClose).toBe(false)
    expect(channel._phase).not.toBe('ended')

    await expect(closePromise).resolves.toBeNull()
    expect(didFinishOnClose).toBe(true)
    expect(channel._phase).toBe('ended')
  })

  const UNRETURNED = {
    'a Channel': () => new ServerChannel<never, number>(),
    'a BroadcastChannel used only on the server': () => new ServerBroadcast<number>({ key: crypto.randomUUID() }),
  }
  for (const [kind, create] of Object.entries(UNRETURNED)) {
    test(`close() of ${kind} no telefunction has returned ends it without waiting for a page, and onClose() receives null`, async () => {
      const channel = create()
      const closes: unknown[] = []
      channel.onClose((err) => void closes.push(err))

      await expect(channel.close({ timeout: 1_000 })).resolves.toBeNull()
      expect(closes).toEqual([null])
      await expect(channel.closed).resolves.toBeNull()
    })
  }

  test('buffered send flushes before close request on attachPeer', async () => {
    const channel = new ServerChannel<never, number>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.send(1)
    // No response carried it yet: it closes with no page to wait for.
    await expect(channel.close()).resolves.toBeNull()

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    await Promise.resolve()

    // The page that attaches later reads what was sent, then the close and its verdict.
    expect(tagsOf(frames)).toEqual([TAG.TEXT, TAG.CLOSE, TAG.CLOSE_DONE])
    const dataFrame = decode(frames[0]!)
    expect(dataFrame.tag).toBe(TAG.TEXT)
    if (dataFrame.tag !== TAG.TEXT) throw new Error('Expected TEXT frame')
    expect(dataFrame.text).toBe('1')
    expectCloseFrame(frames[1]!)
  })

  test('send({ack:true}) resolves and close() resolves null when both acks arrive', async () => {
    const channel = new ServerChannel<never, (v: string) => string>({ ack: true, id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)

    const sendPromise = channel.send('hello', { ack: true })
    const closePromise = channel.close({ timeout: 100 })

    const ackReqFrame = decode(frames[0]!)
    if (ackReqFrame.tag !== TAG.TEXT_ACK_REQ) throw new Error('Expected TEXT_ACK_REQ')
    page.ackRes(ackReqFrame.seq, '"ok"')
    page.closeAck()
    page.closeDone()

    await expect(sendPromise).resolves.toBe('ok')
    await expect(closePromise).resolves.toBeNull()
  })

  test('outbound ack that arrives after timeout rejects with close error', async () => {
    const channel = new ServerChannel<never, (v: string) => string>({ ack: true, id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)

    const sendPromise = channel.send('hello', { ack: true })
    const sendOutcome = sendPromise.then(
      (value) => ({ status: 'fulfilled' as const, value }),
      (error) => ({ status: 'rejected' as const, error }),
    )
    channel.close({ timeout: 10 })

    // Timeout fires, shutdown happens, ack arrives too late
    await new Promise((resolve) => setTimeout(resolve, 20))

    const ackReqFrame = decode(frames[0]!)
    if (ackReqFrame.tag !== TAG.TEXT_ACK_REQ) throw new Error('Expected TEXT_ACK_REQ')
    page.ackRes(ackReqFrame.seq, '"received"')

    const result = await sendOutcome
    expect(result.status).toBe('rejected')
    if (result.status !== 'rejected') throw new Error('Expected rejection')
    expect(result.error).toBeInstanceOf(Error)
  })
})

// ── Peer-initiated close ──

describe('peer-initiated close', () => {
  test('isClosed=true, onClose, and close-ack all fire immediately', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let didFireClose = false
    let closeErr: Error | undefined | null = null

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.onClose((err) => {
      didFireClose = true
      closeErr = err
    })

    page.closeRequest(100)

    // All synchronous: its stop, its cleanup, and with nothing to wait for, its side done.
    expect(channel.isClosed).toBe(true)
    expect(didFireClose).toBe(true)
    expect(closeErr).toBeNull()
    expect(tagsOf(frames)).toEqual([TAG.CLOSE_ACK, TAG.CLOSE_DONE])
    expectCloseAckFrame(frames[0]!)
    // The page decides: the server ends on its verdict.
    expect(channel._phase).not.toBe('ended')
    page.closeDone()
    await expect(channel.closed).resolves.toBeNull()
  })

  test('a close() during a close the page began asks for a shorter deadline and returns its outcome', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.onClose(() => new Promise<void>(() => {}))
    page.closeRequest(5_000)
    const result = channel.close({ timeout: 0 })
    const close = frames.map((frame) => decode(frame))[1]
    expect(tagsOf(frames)).toEqual([TAG.CLOSE_ACK, TAG.CLOSE])
    expect(close?.tag === TAG.CLOSE && close.timeoutMs).toBe(0)
    page.closeTimedOut()
    await expect(result).resolves.toBeInstanceOf(Error)
    await expect(channel.closed).resolves.toHaveProperty('message', expect.stringContaining('Channel close timed out'))
  })

  test('shutdown waits for inflight inbound ack', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const work = createDeferred<string>()
    channel.listen(() => work.promise)

    const pending = page.ackReq('"work"')
    // Its deadline is past any run's: the close ends on the ack.
    page.closeRequest(60_000)

    expect(channel.isClosed).toBe(true)

    // Not terminated yet — inflight work
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._phase).not.toBe('ended')

    work.resolve('done')
    await expect(pending).resolves.toBeUndefined()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(tagsOf(frames).slice(-2)).toEqual([TAG.ACK_RES, TAG.CLOSE_DONE])
    page.closeDone()
    expect(channel._phase).toBe('ended')
  })

  test('peer timeout cuts short long-running inflight work', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.listen(
      () =>
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('done'), 50)
        }),
    )

    // The ended channel drops its answer: nothing waits for one.
    page.askAck('"work"')
    page.closeRequest(10)

    expect(channel.isClosed).toBe(true)
    expectCloseAckFrame(frames[0]!)

    // The page's deadline passes: its verdict ends the server's end.
    await new Promise((resolve) => setTimeout(resolve, 10))
    page.closeTimedOut()
    expect(channel._phase).toBe('ended')
    await expect(channel.closed).resolves.toHaveProperty('message', expect.stringContaining('Channel close timed out'))
  })

  test('shutdown waits for async onClose callbacks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    let didFinishOnClose = false

    const frames: Uint8Array[] = []
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.onClose(async () => {
      await new Promise((resolve) => setTimeout(resolve, 25))
      didFinishOnClose = true
    })

    page.closeRequest(100)

    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(didFinishOnClose).toBe(false)
    expect(channel._phase).not.toBe('ended')

    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(didFinishOnClose).toBe(true)
    expect(tagsOf(frames)).toEqual([TAG.CLOSE_ACK, TAG.CLOSE_DONE])
    page.closeDone()
    expect(channel._phase).toBe('ended')
  })
})

describe('a close whose verdict never crosses', () => {
  test("its verdict never sent: the server ends at the close's timeout and the reconnect window past it, with the outcome unknown", async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    try {
      const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
      const frames: Uint8Array[] = []
      const closes: (Error | null)[] = []
      channel._registerChannel()
      channel._attachPeer(createPeer(frames))
      channel.onClose((err) => void closes.push(err))
      fromPage(channel, frames).closeRequest(100)
      expect(tagsOf(frames)).toEqual([TAG.CLOSE_ACK, TAG.CLOSE_DONE])
      await vi.advanceTimersByTimeAsync(100 + reconnectWindow() - 1)
      expect(channel._phase).not.toBe('ended')
      await vi.advanceTimersByTimeAsync(1)
      expect(channel._phase).toBe('ended')
      await expect(channel.closed).resolves.toBeInstanceOf(ChannelCloseUnknownError)
      // Its cleanup ran at the page's CLOSE, gracefully as far as it knew.
      expect(closes).toEqual([null])
      // It sends nothing in the verdict's place: the page decides.
      expect(tagsOf(frames)).toEqual([TAG.CLOSE_ACK, TAG.CLOSE_DONE])
    } finally {
      vi.useRealTimers()
    }
  })

  test('a close timeout as long as a timer holds: the server waits it and the reconnect window past it, through timers no longer than that', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    const timers = vi.spyOn(globalThis, 'setTimeout')
    try {
      const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
      const frames: Uint8Array[] = []
      channel._registerChannel()
      channel._attachPeer(createPeer(frames))
      fromPage(channel, frames).closeRequest(TIMER_DELAY_MAX_MS)
      const longest = () => Math.max(...timers.mock.calls.map(([, delay]) => delay ?? 0))
      expect(longest()).toBeLessThanOrEqual(TIMER_DELAY_MAX_MS)
      await vi.advanceTimersByTimeAsync(TIMER_DELAY_MAX_MS + reconnectWindow() - 1)
      expect(channel._phase).not.toBe('ended')
      await vi.advanceTimersByTimeAsync(1)
      await expect(channel.closed).resolves.toBeInstanceOf(ChannelCloseUnknownError)
      expect(longest()).toBeLessThanOrEqual(TIMER_DELAY_MAX_MS)
    } finally {
      timers.mockRestore()
      vi.useRealTimers()
    }
  })

  test("its page gone past the reconnect timeout: the server ends with the outcome unknown, the page's loss its cause", async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    try {
      const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
      const frames: Uint8Array[] = []
      const peer = createPeer(frames)
      channel._registerChannel()
      channel._attachPeer(peer)
      fromPage(channel, frames).closeRequest(60_000)
      channel._onPeerDisconnect(peer, 1_000)
      await vi.advanceTimersByTimeAsync(1_000)
      const outcome = await channel.closed
      expect(outcome).toBeInstanceOf(ChannelCloseUnknownError)
      expect(outcome?.cause).toBeInstanceOf(NetworkError)
    } finally {
      vi.useRealTimers()
    }
  })

  test("the server that decides, its page gone past the reconnect timeout: it ends with the outcome unknown too, the page's loss its cause", async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    try {
      const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
      const frames: Uint8Array[] = []
      const peer = createPeer(frames)
      channel._registerChannel()
      channel._attachPeer(peer)
      const result = channel.close({ timeout: 60_000 })
      fromPage(channel, frames).closeAck()
      channel._onPeerDisconnect(peer, 1_000)
      await vi.advanceTimersByTimeAsync(1_000)
      const outcome = await result
      expect(outcome).toBeInstanceOf(ChannelCloseUnknownError)
      expect(outcome).toBeInstanceOf(NetworkError)
      expect(outcome?.cause).toBeInstanceOf(NetworkError)
    } finally {
      vi.useRealTimers()
    }
  })
})

describe("an onClose callback that waits for its own channel's outcome", () => {
  for (const reads of ['closed', 'close()'] as const) {
    test(`awaiting \`${reads}\` doesn't hold the close: it ends gracefully, and the callback gets null`, async () => {
      const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
      const frames: Uint8Array[] = []
      channel._registerChannel()
      channel._attachPeer(createPeer(frames))
      const page = fromPage(channel, frames)
      const seen = createDeferred<Error | null>()
      channel.onClose(async () => {
        seen.resolve(await (reads === 'closed' ? channel.closed : channel.close()))
      })
      const result = channel.close({ timeout: 60_000 })
      page.closeAck()
      page.closeDone()
      await expect(result).resolves.toBeNull()
      await expect(seen.promise).resolves.toBeNull()
    })
  }
})

// ── Cross-close (both sides simultaneously) ──

describe('cross-close', () => {
  test('both sides close: close() resolves null when peer acks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)

    // Server calls close
    const closePromise = channel.close({ timeout: 100 })
    expect(channel.isClosed).toBe(true)
    expect(frames).toHaveLength(1)
    expectCloseFrame(frames[0]!)

    // The page's CLOSE, sent before it had the server's: a cross, which the server decides. The page's CLOSE is its stop,
    // so the server sends no CLOSE_ACK.
    page.closeRequest(100)
    expect(tagsOf(frames)).toEqual([TAG.CLOSE])

    page.closeDone()
    await expect(closePromise).resolves.toBeNull()
    expect(tagsOf(frames)).toEqual([TAG.CLOSE, TAG.CLOSE_DONE])
    expect(channel._phase).toBe('ended')
  })

  test('both sides close: close() resolves the timeout error on timeout without ack', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)

    const closePromise = channel.close({ timeout: 10 })

    // The page's CLOSE crossed it, and the page never says it is done.
    page.closeRequest(10)

    await expect(closePromise).resolves.toBeInstanceOf(Error)
    expect(channel._phase).toBe('ended')
  })

  test("a crossed close the server decides ends at the page's shortening, on both ends with the timeout", async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.onClose(() => new Promise<void>(() => {}))
    const closePromise = channel.close({ timeout: 5_000 })
    // The page's CLOSE crossed the server's; the page follows, and its app's close({ timeout: 0 }) asks for less.
    page.closeRequest(5_000)
    page.closeRequest(0)
    await expect(closePromise).resolves.toBeInstanceOf(Error)
    await expect(channel.closed).resolves.toHaveProperty('message', expect.stringContaining('Channel close timed out'))
    const end = frames.map((frame) => decode(frame))[1]
    expect(end?.tag === TAG.ERROR && end.reason).toBe(ERROR_REASON.CLOSE_TIMED_OUT)
  })

  test("the server's cleanup starts at the page's CLOSE_ACK, while the page's runs, and a cleanup past the timeout ends both with it", async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    let cleanupStarted = false
    channel.onClose(() => {
      cleanupStarted = true
      return new Promise<void>((resolve) => setTimeout(resolve, 200))
    })
    const closePromise = channel.close({ timeout: 20 })
    expect(cleanupStarted).toBe(false)
    // The page's cleanup runs from here on: it sends its CLOSE_DONE only once it settles.
    page.closeAck()
    expect(cleanupStarted).toBe(true)
    await expect(closePromise).resolves.toBeInstanceOf(Error)
    expect(tagsOf(frames)).toEqual([TAG.CLOSE, TAG.ERROR])
  })
})

// ── Stopping an onOpen or onClose callback ──

describe('stopping an onOpen or onClose callback', () => {
  test('a stopped onOpen is not called on open', () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    let didFireOpen = false

    const stop = channel.onOpen(() => {
      didFireOpen = true
    })
    stop()

    channel._registerChannel()
    channel._attachPeer(createPeer([]))
    expect(didFireOpen).toBe(false)
  })

  test('a stopped onClose is not called, and close() does not wait for it', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let didFireClose = false

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const stop = channel.onClose(() => {
      didFireClose = true
      return new Promise<void>(() => {})
    })
    stop()

    const closePromise = channel.close({ timeout: 100 })
    page.closeAck()
    page.closeDone()

    await expect(closePromise).resolves.toBeNull()
    expect(didFireClose).toBe(false)
  })

  test('stopping after the event, or twice, does nothing', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let opens = 0
    let closes = 0
    const onClose = () => {
      closes++
    }

    const stopOpen = channel.onOpen(() => {
      opens++
    })
    const stopFirstClose = channel.onClose(onClose)
    const stopSecondClose = channel.onClose(onClose)
    stopFirstClose()
    stopFirstClose()

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    stopOpen()
    stopOpen()
    fromPage(channel, frames).closeRequest(100)
    stopSecondClose()

    expect(opens).toBe(1)
    expect(closes).toBe(1)
  })

  test('aborting { signal } removes the callback', () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    const controller = new AbortController()
    let didFire = false
    const fire = () => {
      didFire = true
    }

    channel.onOpen(fire, { signal: controller.signal })
    channel.onClose(fire, { signal: controller.signal })
    controller.abort()

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    fromPage(channel, frames).closeRequest(100)
    expect(didFire).toBe(false)
  })
})

// ── A listen() callback that an earlier one stopped ──

describe('a listen() callback that an earlier one stopped during the same message', () => {
  test('is not called with it', () => {
    const channel = new ServerChannel<string, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    const got: string[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    channel.listen(() => stopSecond())
    const stopSecond = channel.listen((value) => {
      got.push(value)
    })
    fromPage(channel, frames).text('"hi"')

    expect(got).toEqual([])
  })

  test('is not asked for the answer to an acknowledged send', async () => {
    const channel = new ServerChannel<(v: string) => string, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    const asked: string[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    channel.listen(() => stopSecond())
    const stopSecond = channel.listen((value) => {
      asked.push(value)
      return 'answer'
    })
    await fromPage(channel, frames).ackReq('"q"')

    expect(asked).toEqual([])
  })
})

// The channel page's chat example announces a leave from `onClose()`: on the server, a publish goes to the key.
describe("a server BroadcastChannel's publish from its onClose()", () => {
  const ENDS = {
    'the server closed it': (channel: ServerBroadcast<string>, frames: Uint8Array[]) => {
      const closing = channel.close()
      const page = fromPage(channel, frames)
      page.closeAck()
      page.closeDone()
      return closing
    },
    'its page left': (channel: ServerBroadcast<string>) => channel._onPeerGone(new ChannelPageLeftError()),
  }
  for (const [end, endIt] of Object.entries(ENDS))
    for (const awaitsFirst of [false, true])
      for (const binary of [false, true])
        test(`${binary ? 'publishBinary()' : 'publish()'} reaches the key's other subscribers once ${end}${awaitsFirst ? ', from a callback that awaits first' : ''}`, async () => {
          const key = crypto.randomUUID()
          const channel = new ServerBroadcast<string>({ key })
          const frames: Uint8Array[] = []
          channel._registerChannel()
          channel._attachPeer(createPeer(frames))
          const sibling = new ServerBroadcast<string>({ key })
          const got: { by: string; data: unknown }[] = []
          const unsubscribes = binary
            ? [
                Broadcast.subscribeBinary(key, (data) => void got.push({ by: 'Broadcast', data: [...data] })),
                sibling.subscribeBinary((data) => void got.push({ by: 'sibling', data: [...data] })),
              ]
            : [
                Broadcast.subscribe<string>(key, (data) => void got.push({ by: 'Broadcast', data })),
                sibling.subscribe((data) => void got.push({ by: 'sibling', data })),
              ]
          let failed: unknown
          channel.onClose(async () => {
            try {
              if (awaitsFirst) await new Promise((resolve) => setTimeout(resolve, 10))
              if (binary) await channel.publishBinary(new Uint8Array([7]))
              else await channel.publish('left')
            } catch (err) {
              failed = err
            }
          })
          await endIt(channel, frames)
          while (got.length < 2 && failed === undefined) await new Promise((resolve) => setTimeout(resolve, 1))
          for (const unsubscribe of unsubscribes) unsubscribe()
          const data = binary ? [7] : 'left'
          expect({ failed, got }).toEqual({
            failed: undefined,
            got: expect.arrayContaining([
              { by: 'Broadcast', data },
              { by: 'sibling', data },
            ]),
          })
        })
})

// ── A send still waiting as its channel ends ──

/** How each ending settles what a server listener awaits: a send waiting for credit, whose frame already went, and an
 *  acknowledged send's answer. The waiting send resolves only where its frame is ahead of an end the page reads; else it
 *  rejects with the channel's outcome, and the listener that rethrows it reports nothing. */
const ENDINGS: {
  ending: string
  end: (end: {
    channel: ServerChannel<string, string>
    page: ReturnType<typeof fromPage>
    peer: IndexedPeer
  }) => unknown
  outcome: (outcome: Error | null) => boolean
  waiting: 'resolves' | 'rejects'
}[] = [
  {
    ending: "this end's abort()",
    end: ({ channel }) => channel.abort('stop'),
    outcome: isAbort,
    waiting: 'resolves',
  },
  {
    ending: "the page's abort()",
    end: ({ page }) => page.abort('stop'),
    outcome: isAbort,
    waiting: 'rejects',
  },
  {
    ending: 'a graceful close',
    end: async ({ channel, page }) => {
      const closed = channel.close({ timeout: 60_000 })
      page.ackRes(1, '"answered"')
      page.closeAck()
      page.closeDone()
      await closed
    },
    outcome: (outcome) => outcome === null,
    waiting: 'resolves',
  },
  {
    ending: 'the timeout of a close the server decides',
    end: async ({ channel, page }) => {
      const closed = channel.close({ timeout: 10 })
      page.closeAck()
      await closed
    },
    outcome: (outcome) => outcome instanceof ChannelClosedError,
    waiting: 'rejects',
  },
  {
    ending: 'the timeout of a close the page decides',
    end: ({ page }) => {
      page.closeRequest(60_000)
      page.closeTimedOut()
    },
    outcome: (outcome) => outcome instanceof ChannelClosedError,
    waiting: 'rejects',
  },
  {
    ending: 'a reconnect that needs what the replay buffer dropped',
    end: ({ page }) => page.replayLost(),
    outcome: (outcome) => outcome instanceof NetworkError && !(outcome instanceof ChannelCloseUnknownError),
    waiting: 'rejects',
  },
  {
    ending: 'the connection lost outside a close',
    end: ({ channel, peer }) => channel._onPeerDisconnect(peer, 10),
    outcome: (outcome) => outcome?.constructor === NetworkError,
    waiting: 'rejects',
  },
  {
    ending: 'the connection lost during a close the page began',
    end: ({ channel, page, peer }) => {
      page.closeRequest(60_000)
      channel._onPeerDisconnect(peer, 10)
    },
    outcome: (outcome) => outcome instanceof ChannelCloseUnknownError && outcome.cause instanceof NetworkError,
    waiting: 'rejects',
  },
  {
    ending: 'the connection lost during a close the server decides',
    end: ({ channel, page, peer }) => {
      void channel.close({ timeout: 60_000 })
      page.closeAck()
      channel._onPeerDisconnect(peer, 10)
    },
    outcome: (outcome) => outcome instanceof ChannelCloseUnknownError && outcome.cause instanceof NetworkError,
    waiting: 'rejects',
  },
  {
    ending: "a reconnect that doesn't recover the channel",
    end: ({ channel }) => channel._onPeerRecoveryFailure(),
    outcome: (outcome) => outcome?.constructor === NetworkError,
    waiting: 'rejects',
  },
  {
    ending: "a reconnect that doesn't recover the channel during a close",
    end: ({ channel, page }) => {
      void channel.close({ timeout: 60_000 })
      page.closeAck()
      channel._onPeerRecoveryFailure()
    },
    outcome: (outcome) => outcome instanceof ChannelCloseUnknownError && outcome.cause instanceof NetworkError,
    waiting: 'rejects',
  },
  {
    ending: 'the page leaving outside a close',
    end: ({ channel }) => channel._onPeerGone(new ChannelPageLeftError()),
    outcome: (outcome) => outcome instanceof ChannelPageLeftError,
    waiting: 'rejects',
  },
  {
    ending: 'the page leaving during a close',
    end: async ({ channel, page }) => {
      const closed = channel.close({ timeout: 60_000 })
      page.closeAck()
      channel._onPeerGone(new ChannelPageLeftError())
      expect(await closed).toBeInstanceOf(ChannelPageLeftError)
    },
    outcome: (outcome) => outcome instanceof ChannelPageLeftError,
    waiting: 'rejects',
  },
  {
    ending: 'a frame the server refused',
    end: ({ channel }) => channel._onPeerGone(rejectedFrameError()),
    outcome: (outcome) => outcome instanceof ChannelClosedError,
    waiting: 'rejects',
  },
  {
    ending: "an overflow: the page's messages past what the server may hold",
    end: ({ page }) => {
      // Its binary listener holds each one: one more than the channel may hold.
      const bytes = 768 * 1024
      for (let i = 0; i <= getChannelConfig().maxPendingBytes / bytes + 1; i++) page.binary(bytes)
    },
    outcome: (outcome) => outcome instanceof ChannelOverflowError,
    waiting: 'rejects',
  },
]

describe('a send still waiting as its channel ends', () => {
  const bugs: unknown[] = []
  onBug((err) => bugs.push(err))

  for (const { ending, end, outcome, waiting } of ENDINGS) {
    test(`${ending}: the send waiting for credit ${waiting}, the answer awaited rejects unless given, and nothing is reported`, async () => {
      bugs.length = 0
      const printed = vi.spyOn(console, 'error').mockImplementation(() => {})
      try {
        const channel = new ServerChannel<string, string>({ id: crypto.randomUUID() })
        const frames: Uint8Array[] = []
        const peer = createPeer(frames)
        channel._registerChannel()
        channel._attachPeer(peer)
        const page = fromPage(channel, frames)
        const settled: { waiting?: unknown; answer?: unknown } = {}
        const listened = createDeferred<void>()
        channel.listenBinary(() => new Promise(() => {}))
        channel.listen(async () => {
          // The acknowledged send goes first, at seq 1, outside credit; the page answers it only where the close is
          // graceful. The 100th plain send is out of the credit the page starts with.
          const answer = channel.send('question', { ack: true })
          for (let i = 0; i < CREDIT_MSG_WINDOW_INITIAL - 1; i++) void channel.send('fill')
          const waits = channel.send('last')
          void answer.then(
            (value) => (settled.answer = value),
            (err: unknown) => (settled.answer = err),
          )
          void waits.then(
            () => (settled.waiting = 'resolved'),
            (err: unknown) => (settled.waiting = err),
          )
          listened.resolve()
          // The listener awaits both and rethrows what they reject with.
          await Promise.all([waits, answer])
        })
        page.text('"go"')
        await listened.promise
        expect(settled).toEqual({})

        await end({ channel, page, peer })
        await new Promise((resolve) => setTimeout(resolve, 30))

        const result = await channel.closed
        expect(outcome(result)).toBe(true)
        if (waiting === 'resolves') expect(settled.waiting).toBe('resolved')
        else expect(settled.waiting).toBe(result)
        expect(settled.answer).toBe(result === null ? 'answered' : result)
        expect(bugs).toEqual([])
        expect(printed).not.toHaveBeenCalled()
      } finally {
        printed.mockRestore()
      }
    })
  }

  test("a listener that rejects with an error that isn't its channel's end is reported", async () => {
    bugs.length = 0
    const printed = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const channel = new ServerChannel<string, string>({ id: crypto.randomUUID() })
      const frames: Uint8Array[] = []
      channel._registerChannel()
      channel._attachPeer(createPeer(frames))
      const mine = new NetworkError('an error of its own', true)
      channel.listen(async () => {
        await new Promise((resolve) => setTimeout(resolve, 1))
        throw mine
      })
      fromPage(channel, frames).text('"go"')
      channel.abort('stop')
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(bugs).toEqual([mine])
    } finally {
      printed.mockRestore()
    }
  })
})
