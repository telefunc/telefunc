import { expect, test, vi } from 'vitest'
import { ChannelMux, type ServerTransport } from './server/ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './server/ServerChannel.js'
import { onBug } from '../../node/server/runTelefunc/onBug.js'
import { decode, encode, type DecodedFrame } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { CHANNEL_TRANSPORT, WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import { WIRE_MAX_CTRL_FRAME_BYTES } from '../wire/payloads.js'

function createHarness() {
  const mux = new ChannelMux()
  const wire = () => {
    const conn = {}
    const sent: DecodedFrame[] = []
    let terminated = false
    const transport: ServerTransport<object> = {
      getConnId: () => null,
      type: CHANNEL_TRANSPORT.SSE,
      wholeMessages: false,
      sendNow: (_conn, frame) => sent.push(decode(frame)),
      bufferedAmount: () => 0,
      queueOf: () => ({
        waitingPublishes: () => 0,
        dropUngiven: () => 0,
        dropWaitingPublishes() {},
        dropWaitingFrom() {},
      }),
      close: () => (terminated = true),
    }
    mux.onWireOpen(conn, transport)
    return {
      sent,
      /** The id the last RECONCILED gave this wire. */
      sessionId: () => {
        const reconciled = sent.filter((frame) => frame.tag === TAG.RECONCILED).at(-1)
        return reconciled?.tag === TAG.RECONCILED ? reconciled.payload.sessionId : undefined
      },
      terminated: () => terminated,
      deliver: (frame: Uint8Array<ArrayBuffer>) => mux.onWireMessage(conn, frame),
    }
  }
  const channel = new ServerChannel<number, never>({ id: 'A' })
  const received: number[] = []
  channel.listen((value) => received.push(value))
  mux.registerChannel(channel, ATTACHABLE_NOW)
  return { mux, wire, received }
}

async function setupUpgrade() {
  const h = createHarness()
  const old = h.wire()
  const probe = h.wire()
  await old.deliver(
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }],
    }),
  )
  const sessionId = old.sessionId()
  expect(sessionId).toBeTypeOf('string')
  await probe.deliver(encode.prepare({ attempt: 2, sessionId: sessionId!, upgradeId: 'upgrade-1' }))
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
    claimant.deliver(encode.reconcile({ sessionId: h.sessionId, attempt: 3, open: [{ id: 'A', ix: 0, lastSeq: 1 }] })),
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
  const open = Array.from({ length: Math.ceil(WIRE_MAX_CTRL_FRAME_BYTES / 256) }, (_, ix) => ({
    id: 'x'.repeat(256),
    ix,
    lastSeq: 0,
  }))
  const oversize = encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open })
  expect(oversize.byteLength).toBeGreaterThan(WIRE_MAX_CTRL_FRAME_BYTES)
  await wire.deliver(oversize)
  expect(wire.terminated()).toBe(true)
  expect(wire.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(0)

  // The same byte count on the data plane is ordinary traffic: user payloads are why that limit
  // is 64 MiB, and applying it to control frames is what let a peer make the server parse one.
  const data = h.wire()
  await data.deliver(
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }],
    }),
  )
  const big = encode.text(0, `"${'x'.repeat(oversize.byteLength)}"`, 1)
  expect(big.byteLength).toBeGreaterThan(WIRE_MAX_CTRL_FRAME_BYTES)
  expect(big.byteLength).toBeLessThan(WIRE_MAX_RAW_FRAME_BYTES)
  await data.deliver(big)
  expect(data.terminated()).toBe(false)
})

test("a RECONCILE that names a channel at a second index of the page's session ends its wire", async () => {
  const h = createHarness()
  const twice = h.wire()
  await twice.deliver(
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [
        { id: 'A', ix: 0, lastSeq: 0, initial: true },
        { id: 'A', ix: 1, lastSeq: 0, initial: true },
      ],
    }),
  )
  expect(twice.terminated()).toBe(true)
  expect(twice.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(0)

  const wire = h.wire()
  const sessionId = crypto.randomUUID()
  await wire.deliver(encode.reconcile({ sessionId, attempt: 1, open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const opens = (ix: number) => encode.reconcile({ sessionId, released: [], open: [{ id: 'A', ix, lastSeq: 0 }] })
  // The page names the channel again where it holds it: that's no violation.
  await wire.deliver(opens(0))
  expect(wire.terminated()).toBe(false)
  await wire.deliver(opens(1))
  expect(wire.terminated()).toBe(true)
  expect(wire.sent.filter((frame) => frame.tag === TAG.RECONCILED)).toHaveLength(2)
})

test('a page that awaits one channel at two indexes of its session has its wire ended as the channel registers', async () => {
  const h = createHarness()
  const wire = h.wire()
  const sessionId = crypto.randomUUID()
  await wire.deliver(encode.reconcile({ sessionId, attempt: 1, open: [] }))
  for (const ix of [0, 1])
    await wire.deliver(
      encode.reconcile({
        sessionId,
        released: [],
        open: [{ id: 'B', ix, lastSeq: 0, initial: true }],
      }),
    )
  expect(wire.terminated()).toBe(false)
  h.mux.registerChannel(new ServerChannel({ id: 'B' }), ATTACHABLE_NOW)
  expect(wire.terminated()).toBe(true)
})

test('a bug in handling a frame is reported, and the server goes on handling the frames after it', async () => {
  const h = createHarness()
  class ChannelWithABug extends ServerChannel<number, never> {
    override _dispatchFrame(): void {
      throw new Error('a bug in handling a frame')
    }
  }
  h.mux.registerChannel(new ChannelWithABug({ id: 'B' }), ATTACHABLE_NOW)
  const bugs: unknown[] = []
  onBug((err) => bugs.push(err))
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const buggy = h.wire()
  const other = h.wire()
  await buggy.deliver(
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: 'B', ix: 0, lastSeq: 0, initial: true }],
    }),
  )
  await other.deliver(
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }],
    }),
  )

  // As a WebSocket host calls it: the promise is dropped, so a rejection would be unhandled.
  void buggy.deliver(encode.text(0, '111', 1))
  await other.deliver(encode.text(0, '222', 1))

  expect(bugs).toEqual([new Error('a bug in handling a frame')])
  expect(h.received).toEqual([222])
  expect(buggy.terminated()).toBe(false)
  vi.restoreAllMocks()
})
