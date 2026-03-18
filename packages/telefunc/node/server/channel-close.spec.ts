import { describe, expect, test } from 'vitest'

import { ReplayBuffer } from '../../wire-protocol/replay-buffer.js'
import { TAG, decode } from '../../wire-protocol/shared-ws.js'
import { IndexedPeer } from '../../wire-protocol/server/IndexedPeer.js'
import { ServerChannel } from '../../wire-protocol/server/channel.js'

function createPeer(frames: Uint8Array[]) {
  return new IndexedPeer(
    {
      send(frame) {
        frames.push(frame)
      },
    },
    7,
    new ReplayBuffer(1024 * 1024, 60_000),
  )
}

function expectCloseFrame(frame: Uint8Array) {
  const decoded = decode(frame)
  expect(decoded.tag).toBe(TAG.CTRL)
  if (decoded.tag !== TAG.CTRL) throw new Error('Expected CTRL frame')
  expect(decoded.ctrl.t).toBe('close')
  if (decoded.ctrl.t !== 'close') throw new Error('Expected close ctrl')
  return decoded.ctrl
}

function expectCloseAckFrame(frame: Uint8Array) {
  const decoded = decode(frame)
  expect(decoded.tag).toBe(TAG.CTRL)
  if (decoded.tag !== TAG.CTRL) throw new Error('Expected CTRL frame')
  expect(decoded.ctrl).toEqual({ t: 'close-ack', ix: 7 })
}

// ── Self-initiated close ──

describe('self-initiated close', () => {
  test('isClosed=true and onClose fire immediately on close()', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let didFireClose = false
    let closeErr: Error | undefined | null = null

    channel.onClose((err) => {
      didFireClose = true
      closeErr = err
    })

    channel.attachPeer(createPeer(frames))
    const closePromise = channel.close()

    // Immediately synchronous — before any await
    expect(channel.isClosed).toBe(true)
    expect(didFireClose).toBe(true)
    expect(closeErr).toBe(undefined)
    expect(channel._didShutdown).toBe(false)

    channel._onPeerCloseAck()
    await expect(closePromise).resolves.toBe(0)
    expect(channel._didShutdown).toBe(true)
  })

  test('close() sends close frame and resolves 0 after ack', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))
    const closePromise = channel.close()

    expect(frames).toHaveLength(1)
    const ctrl = expectCloseFrame(frames[0]!)
    expect(ctrl.ix).toBe(7)
    expect(ctrl.timeoutMs).toBeGreaterThanOrEqual(0)

    channel._onPeerCloseAck()
    await expect(closePromise).resolves.toBe(0)
    expect(channel._didShutdown).toBe(true)
  })

  test('close() resolves 1 when peer never acks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))
    const closePromise = channel.close({ timeout: 10 })

    expect(channel.isClosed).toBe(true)
    expect(frames).toHaveLength(1)
    expectCloseFrame(frames[0]!)

    await expect(closePromise).resolves.toBe(1)
    expect(channel._didShutdown).toBe(true)
  })

  test('close() waits for inflight inbound ack before resolving', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))
    channel.listen(
      () =>
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('done'), 30)
        }),
    )

    // Start inbound ack-req processing
    const inboundPending = channel._onPeerAckReqMessage('"work"', 1)
    const closePromise = channel.close({ timeout: 100 })

    expect(channel.isClosed).toBe(true)
    channel._onPeerCloseAck()

    // close() hasn't resolved yet — inflight inbound ack still in progress
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._didShutdown).toBe(false)

    // Inbound work finishes → close resolves
    await expect(inboundPending).resolves.toBeUndefined()
    await expect(closePromise).resolves.toBe(0)
    expect(channel._didShutdown).toBe(true)
  })

  test('close() waits for inflight outbound ack before resolving', async () => {
    const channel = new ServerChannel<(v: string) => string, never>({ ackMode: true, id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))

    const sendPromise = channel.send('hello', { ack: true })
    const closePromise = channel.close({ timeout: 100 })

    expect(channel.isClosed).toBe(true)
    channel._onPeerCloseAck()

    // close() hasn't resolved yet — outbound ack still pending
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._didShutdown).toBe(false)

    // Ack arrives → close resolves
    const ackReqFrame = decode(frames[0]!)
    if (ackReqFrame.tag !== TAG.TEXT_ACK_REQ) throw new Error('Expected TEXT_ACK_REQ')
    channel._onPeerAckRes(ackReqFrame.seq, '"received"')

    await expect(sendPromise).resolves.toBe('received')
    await expect(closePromise).resolves.toBe(0)
    expect(channel._didShutdown).toBe(true)
  })

  test('close() waits for async onClose callbacks before resolving', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []
    let didFinishOnClose = false

    channel.attachPeer(createPeer(frames))
    channel.onClose(async () => {
      await new Promise((resolve) => setTimeout(resolve, 30))
      didFinishOnClose = true
    })

    const closePromise = channel.close({ timeout: 100 })
    expect(channel.isClosed).toBe(true)
    channel._onPeerCloseAck()

    // close() hasn't resolved — async onClose still running
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(didFinishOnClose).toBe(false)
    expect(channel._didShutdown).toBe(false)

    await expect(closePromise).resolves.toBe(0)
    expect(didFinishOnClose).toBe(true)
    expect(channel._didShutdown).toBe(true)
  })

  test('buffered send flushes before close request on attachPeer', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.send(1)
    const closePromise = channel.close()

    channel._registerChannel()
    channel.attachPeer(createPeer(frames))
    await Promise.resolve()

    expect(frames).toHaveLength(2)
    const dataFrame = decode(frames[0]!)
    expect(dataFrame.tag).toBe(TAG.TEXT)
    if (dataFrame.tag !== TAG.TEXT) throw new Error('Expected TEXT frame')
    expect(dataFrame.text).toBe('1')
    expectCloseFrame(frames[1]!)

    channel._onPeerCloseAck()
    await expect(closePromise).resolves.toBe(0)
  })

  test('send({ack:true}) resolves and close() resolves 0 when both acks arrive', async () => {
    const channel = new ServerChannel<(v: string) => string, never>({ ackMode: true, id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))

    const sendPromise = channel.send('hello', { ack: true })
    const closePromise = channel.close({ timeout: 100 })

    const ackReqFrame = decode(frames[0]!)
    if (ackReqFrame.tag !== TAG.TEXT_ACK_REQ) throw new Error('Expected TEXT_ACK_REQ')
    channel._onPeerAckRes(ackReqFrame.seq, '"ok"')
    channel._onPeerCloseAck()

    await expect(sendPromise).resolves.toBe('ok')
    await expect(closePromise).resolves.toBe(0)
  })

  test('outbound ack that arrives after timeout rejects with close error', async () => {
    const channel = new ServerChannel<(v: string) => string, never>({ ackMode: true, id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))

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
    channel._onPeerAckRes(ackReqFrame.seq, '"received"')

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

    channel.attachPeer(createPeer(frames))
    channel.onClose((err) => {
      didFireClose = true
      closeErr = err
    })

    channel._onPeerCloseRequest(100)

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

    channel.attachPeer(createPeer(frames))
    channel.listen(
      () =>
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('done'), 25)
        }),
    )

    const pending = channel._onPeerAckReqMessage('"work"', 1)
    channel._onPeerCloseRequest(100)

    expect(channel.isClosed).toBe(true)

    // Not terminated yet — inflight work
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(channel._didShutdown).toBe(false)

    await expect(pending).resolves.toBeUndefined()
    expect(channel._didShutdown).toBe(true)
  })

  test('peer timeout cuts short long-running inflight work', async () => {
    const channel = new ServerChannel<(v: string) => Promise<string>, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))
    channel.listen(
      () =>
        new Promise<string>((resolve) => {
          setTimeout(() => resolve('done'), 50)
        }),
    )

    channel._onPeerAckReqMessage('"work"', 1)
    channel._onPeerCloseRequest(10)

    expect(channel.isClosed).toBe(true)
    expectCloseAckFrame(frames[0]!)

    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(channel._didShutdown).toBe(true)
  })

  test('shutdown waits for async onClose callbacks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    let didFinishOnClose = false

    channel.attachPeer(createPeer([]))
    channel.onClose(async () => {
      await new Promise((resolve) => setTimeout(resolve, 25))
      didFinishOnClose = true
    })

    channel._onPeerCloseRequest(100)

    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(didFinishOnClose).toBe(false)
    expect(channel._didShutdown).toBe(false)

    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(didFinishOnClose).toBe(true)
    expect(channel._didShutdown).toBe(true)
  })
})

// ── Cross-close (both sides simultaneously) ──

describe('cross-close', () => {
  test('both sides close: close() resolves 0 when peer acks', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))

    // Server calls close
    const closePromise = channel.close({ timeout: 100 })
    expect(channel.isClosed).toBe(true)
    expect(frames).toHaveLength(1)
    expectCloseFrame(frames[0]!)

    // Peer also sent close (arrives after server's close)
    channel._onPeerCloseRequest(100)

    // close-ack was sent for the peer's request
    expect(frames).toHaveLength(2)
    expectCloseAckFrame(frames[1]!)

    // Peer acks server's close
    channel._onPeerCloseAck()

    await expect(closePromise).resolves.toBe(0)
    expect(channel._didShutdown).toBe(true)
  })

  test('both sides close: close() resolves 1 on timeout without ack', async () => {
    const channel = new ServerChannel<number, never>({ id: crypto.randomUUID() })
    const frames: Uint8Array[] = []

    channel.attachPeer(createPeer(frames))

    const closePromise = channel.close({ timeout: 10 })

    // Peer also sent close — but never sends close-ack for server's request
    channel._onPeerCloseRequest(10)

    await expect(closePromise).resolves.toBe(1)
    expect(channel._didShutdown).toBe(true)
  })
})
