import { expect, test } from 'vitest'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import { ServerChannel } from './server/channel.js'
import { decode, encode, TAG, type DecodedFrame } from './shared-ws.js'
import { WIRE_MAX_CONN_CTRL_FRAME_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from './constants.js'

function createHarness() {
  const mux = new ChannelMux()
  const wire = () => {
    const conn = {}
    const sent: DecodedFrame[] = []
    let sessionId: string | undefined
    let terminated = false
    const transport: ServerTransport<object> = {
      getSessionId: () => sessionId,
      setSessionId: (_conn, id) => (sessionId = id),
      getConnId: () => null,
      sendNow: (_conn, frame) => sent.push(decode(frame)),
      terminateConnection: () => (terminated = true),
    }
    mux.onConnectionOpen(conn, transport)
    return {
      sent,
      sessionId: () => sessionId,
      terminated: () => terminated,
      deliver: (frame: Uint8Array<ArrayBuffer>) => mux.onConnectionRawMessage(conn, frame),
    }
  }
  const channel = new ServerChannel<number, never>({ id: 'A' })
  const received: number[] = []
  channel.listen((value) => received.push(value))
  mux.registerChannel(channel)
  return { wire, received }
}

async function setupUpgrade() {
  const h = createHarness()
  const old = h.wire()
  const probe = h.wire()
  await old.deliver(encode.reconcile({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const sessionId = old.sessionId()
  expect(sessionId).toBeTypeOf('string')
  await probe.deliver(encode.prepare({ sessionId: sessionId!, upgradeId: 'upgrade-1' }))
  const barrier = encode.barrier({
    sessionId: sessionId!,
    upgradeId: 'upgrade-1',
    open: [{ id: 'A', ix: 0, lastSeq: 2 }],
  })
  return { ...h, old, probe, sessionId: sessionId!, barrier }
}

test('an old-wire frame queued before the barrier is dispatched before the session rotates', async () => {
  const h = await setupUpgrade()
  await Promise.all([
    h.old.deliver(encode.text(0, '111', 1)),
    h.old.deliver(encode.text(0, '222', 2)),
    h.old.deliver(h.barrier),
  ])
  expect(h.received).toEqual([111, 222])
  expect(h.probe.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(1)
})

test('a concurrent ordinary claim of the old session loses to the barrier commit', async () => {
  const h = await setupUpgrade()
  const claimant = h.wire()
  await Promise.all([
    h.old.deliver(h.barrier),
    claimant.deliver(encode.reconcile({ sessionId: h.sessionId, open: [{ id: 'A', ix: 0, lastSeq: 1 }] })),
  ])
  expect(h.probe.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(1)
  expect(claimant.terminated()).toBe(true)
  expect(claimant.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(0)
})

test('a control frame is bounded by what the protocol can describe, a data frame is not', async () => {
  const h = createHarness()
  const wire = h.wire()
  // A perfectly well-formed RECONCILE, just larger than a connection could legitimately need.
  // Well-formed matters: a malformed one would be refused by the parser either way, which is
  // exactly what this has to distinguish — the cap has to reject it without parsing it.
  const open = Array.from({ length: 5_200 }, (_, ix) => ({ id: 'x'.repeat(256), ix, lastSeq: 0 }))
  const oversize = encode.reconcile({ open })
  expect(oversize.byteLength).toBeGreaterThan(WIRE_MAX_CONN_CTRL_FRAME_BYTES)
  await wire.deliver(oversize)
  expect(wire.terminated()).toBe(true)
  expect(wire.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(0)

  // The same byte count on the data plane is ordinary traffic: user payloads are why that limit
  // is 64 MiB, and applying it to control frames is what let a peer make the server parse one.
  const data = h.wire()
  await data.deliver(encode.reconcile({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const big = encode.text(0, `"${'x'.repeat(oversize.byteLength)}"`, 1)
  expect(big.byteLength).toBeGreaterThan(WIRE_MAX_CONN_CTRL_FRAME_BYTES)
  expect(big.byteLength).toBeLessThan(WIRE_MAX_RAW_FRAME_BYTES)
  await data.deliver(big)
  expect(data.terminated()).toBe(false)
})
