import { describe, expect, test } from 'vitest'

import { ServerReplay } from '../../wire-protocol/channel/server/ServerReplay.js'
import { ACK_STATUS, decode, encode, type AckResultStatus, type ChannelFrame } from '../../wire-protocol/wire/frame.js'
import { TAG } from '../../wire-protocol/wire/tags.js'
import { ProtocolViolationError } from '../../wire-protocol/wire/protocolViolation.js'
import { IndexedPeer } from '../../wire-protocol/channel/server/IndexedPeer.js'
import { ServerChannel } from '../../wire-protocol/channel/server/ServerChannel.js'

function createPeer(frames: Uint8Array[]) {
  return new IndexedPeer({
    sender: {
      send(frame) {
        frames.push(frame)
      },
      carry() {},
      bufferedAmount: () => 0,
      deliveredBefore: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    },
    index: 7,
    replay: new ServerReplay(1024 * 1024),
  })
}

/** What the page sends `channel`, through the entry its mux hands frames to; each frame takes the page's next seq. */
function fromPage(channel: Pick<ServerChannel, '_dispatchFrame'>, frames: Uint8Array[]) {
  let seq = 0
  const dispatch = (frame: Uint8Array<ArrayBuffer>) => channel._dispatchFrame(decode(frame) as ChannelFrame)
  return {
    ackRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK) {
      dispatch(encode.ackRes(7, ++seq, ackedSeq, result, status))
    },
    closeRequest(timeoutMs: number) {
      dispatch(encode.close(7, timeoutMs, ++seq))
    },
    closeAck() {
      dispatch(encode.closeAck(7, ++seq))
    },
    /** Resolves once the channel sent its answer. */
    async ackReq(text: string): Promise<void> {
      const ackedSeq = ++seq
      dispatch(encode.textAckReq(7, text, ackedSeq))
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
    await expect(closePromise).resolves.toBe(0)
    expect(didFireClose).toBe(true)
    expect(closeErr).toBe(undefined)
    expect(channel._phase).toBe('ended')
  })

  test('close() sends close frame and resolves 0 after ack', async () => {
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
    await expect(closePromise).resolves.toBe(0)
    expect(channel._phase).toBe('ended')
  })

  test('close() resolves 1 when peer never acks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    const closePromise = channel.close({ timeout: 10 })

    expect(channel.isClosed).toBe(true)
    expect(frames).toHaveLength(1)
    expectCloseFrame(frames[0]!)

    await expect(closePromise).resolves.toBe(1)
    expect(channel._phase).toBe('ended')
  })

  test('close() waits for inflight inbound ack before resolving', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.listen(
      () =>
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('done'), 30)
        }),
    )

    // Start inbound ack-req processing
    const inboundPending = page.ackReq('"work"')
    const closePromise = channel.close({ timeout: 100 })

    expect(channel.isClosed).toBe(true)
    page.closeAck()

    // close() hasn't resolved yet — inflight inbound ack still in progress
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._phase).not.toBe('ended')

    // Inbound work finishes → close resolves
    await expect(inboundPending).resolves.toBeUndefined()
    await expect(closePromise).resolves.toBe(0)
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

    await expect(sendPromise).resolves.toBe('received')
    await expect(closePromise).resolves.toBe(0)
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

    // close() hasn't resolved — async onClose still running
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(didFinishOnClose).toBe(false)
    expect(channel._phase).not.toBe('ended')

    await expect(closePromise).resolves.toBe(0)
    expect(didFinishOnClose).toBe(true)
    expect(channel._phase).toBe('ended')
  })

  test('buffered send flushes before close request on attachPeer', async () => {
    const channel = new ServerChannel<never, number>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.send(1)
    // No response carried it yet, so no page acknowledges its close.
    await expect(channel.close()).resolves.toBe(1)

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    await Promise.resolve()

    expect(frames).toHaveLength(2)
    const dataFrame = decode(frames[0]!)
    expect(dataFrame.tag).toBe(TAG.TEXT)
    if (dataFrame.tag !== TAG.TEXT) throw new Error('Expected TEXT frame')
    expect(dataFrame.text).toBe('1')
    expectCloseFrame(frames[1]!)
  })

  test('send({ack:true}) resolves and close() resolves 0 when both acks arrive', async () => {
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

    await expect(sendPromise).resolves.toBe('ok')
    await expect(closePromise).resolves.toBe(0)
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

    // All synchronous
    expect(channel.isClosed).toBe(true)
    expect(didFireClose).toBe(true)
    expect(closeErr).toBe(undefined)
    expect(frames).toHaveLength(1)
    expectCloseAckFrame(frames[0]!)
  })

  test('shutdown waits for inflight inbound ack', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)
    channel.listen(
      () =>
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('done'), 25)
        }),
    )

    const pending = page.ackReq('"work"')
    page.closeRequest(100)

    expect(channel.isClosed).toBe(true)

    // Not terminated yet — inflight work
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._phase).not.toBe('ended')

    await expect(pending).resolves.toBeUndefined()
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

    page.ackReq('"work"')
    page.closeRequest(10)

    expect(channel.isClosed).toBe(true)
    expectCloseAckFrame(frames[0]!)

    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(channel._phase).toBe('ended')
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
    expect(channel._phase).toBe('ended')
  })
})

// ── Cross-close (both sides simultaneously) ──

describe('cross-close', () => {
  test('both sides close: close() resolves 0 when peer acks', async () => {
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

    // Peer also sent close (arrives after server's close)
    page.closeRequest(100)

    // close-ack was sent for the peer's request
    expect(frames).toHaveLength(2)
    expectCloseAckFrame(frames[1]!)

    // Peer acks server's close
    page.closeAck()

    await expect(closePromise).resolves.toBe(0)
    expect(channel._phase).toBe('ended')
  })

  test('both sides close: close() resolves 1 on timeout without ack', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel._registerChannel()
    channel._attachPeer(createPeer(frames))
    const page = fromPage(channel, frames)

    const closePromise = channel.close({ timeout: 10 })

    // Peer also sent close — but never sends close-ack for server's request
    page.closeRequest(10)

    await expect(closePromise).resolves.toBe(1)
    expect(channel._phase).toBe('ended')
  })
})
