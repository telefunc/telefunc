import { afterEach, expect, test } from 'vitest'
import { ChannelMux, type ServerTransport } from './ChannelMux.js'
import { ServerBroadcastEnd, type PageConnection } from './ServerBroadcastEnd.js'
import { encode } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { disposeBackend } from '../../backend/install.js'

afterEach(() => disposeBackend())

/** A mux, wires to it, and Broadcast ends whose owner keeps each connection it is told its page attached on. */
function createHarness() {
  const mux = new ChannelMux()
  const wire = () => {
    const conn = {}
    let sessionId: string | undefined
    const transport: ServerTransport<object> = {
      getSessionId: () => sessionId,
      setSessionId: (_conn, id) => (sessionId = id),
      getConnId: () => null,
      wholeMessages: false,
      sendNow: () => {},
      bufferedAmount: () => 0,
      queueOf: () => ({
        waitingPublishes: () => 0,
        dropWaitingPublishes() {},
        dropWaitingFromPublish() {},
        drainRate: () => 0,
      }),
      terminateWire: () => {},
    }
    mux.onWireOpen(conn, transport)
    return { sessionId: () => sessionId!, deliver: (frame: Uint8Array<ArrayBuffer>) => mux.onWireMessage(conn, frame) }
  }
  const end = (id: string) => {
    const attachedOn: PageConnection[] = []
    const channel = new ServerBroadcastEnd({ id })
    channel.setOwner({
      _onPagePublish() {},
      _onPagePublishBinary() {},
      _onPageAttach: (connection) => attachedOn.push(connection),
    })
    return { attachedOn, register: () => mux.registerChannel(channel) }
  }
  return { wire, end }
}

const opening = (id: string, ix: number): ReconcileOpenEntry => ({ id, ix, lastSeq: 0, initial: true })

/** Each list's connections by identity: the same number is the same object. */
function identities(...lists: PageConnection[][]): number[][] {
  const seen: PageConnection[] = []
  return lists.map((list) =>
    list.map((connection) => {
      if (!seen.includes(connection)) seen.push(connection)
      return seen.indexOf(connection)
    }),
  )
}

test("a page's first reconcile attaches each end on its wire's connection, one for the whole wire", async () => {
  const h = createHarness()
  const [a, b] = [h.end('A'), h.end('B')]
  a.register()
  b.register()
  await h.wire().deliver(encode.reconcile({ open: [opening('A', 0), opening('B', 1)] }))
  expect(identities(a.attachedOn, b.attachedOn)).toEqual([[0], [0]])
})

test('an end the page named before the server registered it attaches on the connection of the wire that named it', async () => {
  const h = createHarness()
  const [a, b] = [h.end('A'), h.end('B')]
  a.register()
  await h.wire().deliver(encode.reconcile({ open: [opening('A', 0), opening('B', 1)] }))
  expect(b.attachedOn).toEqual([])
  b.register()
  expect(identities(a.attachedOn, b.attachedOn)).toEqual([[0], [0]])
})

test("a reconcile of what changed attaches a new end on its wire's connection", async () => {
  const h = createHarness()
  const [a, b] = [h.end('A'), h.end('B')]
  a.register()
  b.register()
  const wire = h.wire()
  await wire.deliver(encode.reconcile({ open: [opening('A', 0)] }))
  await wire.deliver(encode.reconcile({ sessionId: wire.sessionId(), open: [opening('B', 1)], released: [] }))
  expect(identities(a.attachedOn, b.attachedOn)).toEqual([[0], [0]])
})

test('a page back on a new wire attaches each end on a new connection, one for the whole wire', async () => {
  const h = createHarness()
  const [a, b] = [h.end('A'), h.end('B')]
  a.register()
  b.register()
  const first = h.wire()
  await first.deliver(encode.reconcile({ open: [opening('A', 0), opening('B', 1)] }))
  const open = [
    { id: 'A', ix: 0, lastSeq: 0 },
    { id: 'B', ix: 1, lastSeq: 0 },
  ]
  await h.wire().deliver(encode.reconcile({ sessionId: first.sessionId(), open }))
  expect(identities(a.attachedOn, b.attachedOn)).toEqual([
    [0, 1],
    [0, 1],
  ])
})

test("an upgrade's barrier attaches each end on the WebSocket's connection, one for the whole wire", async () => {
  const h = createHarness()
  const [a, b] = [h.end('A'), h.end('B')]
  a.register()
  b.register()
  const sse = h.wire()
  const ws = h.wire()
  await sse.deliver(encode.reconcile({ open: [opening('A', 0), opening('B', 1)] }))
  await ws.deliver(encode.prepare({ sessionId: sse.sessionId(), upgradeId: 'upgrade-1' }))
  const open = [
    { id: 'A', ix: 0, lastSeq: 0 },
    { id: 'B', ix: 1, lastSeq: 0 },
  ]
  await sse.deliver(encode.barrier({ sessionId: sse.sessionId(), upgradeId: 'upgrade-1', open }))
  expect(identities(a.attachedOn, b.attachedOn)).toEqual([
    [0, 1],
    [0, 1],
  ])
})
