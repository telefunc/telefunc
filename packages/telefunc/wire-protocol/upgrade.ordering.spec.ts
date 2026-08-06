import { expect, test } from 'vitest'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import { ServerChannel } from './server/channel.js'
import { decode, encode, TAG, type DecodedFrame } from './shared-ws.js'

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
  const barrier = encode.reconcile({
    sessionId: sessionId!,
    barrier: true,
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
