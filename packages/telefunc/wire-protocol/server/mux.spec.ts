import { expect, test, vi } from 'vitest'
import { ChannelMux, type ServerTransport } from './mux.js'
import { ServerChannel } from './channel.js'
import { decode, encode, TAG, type DecodedFrame } from '../shared-ws.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { CREDIT_MSG_WINDOW_MAX, CREDIT_WINDOW_MAX_BYTES, WIRE_RECV_BACKLOG_BASE_FRAMES } from '../constants.js'

/** Wires the test opens on `mux`, each recording what the server sends on it. */
function wires(mux: ChannelMux) {
  const sessions = new Map<object, string>()
  const sent = new Map<object, DecodedFrame[]>()
  const terminated = new Set<object>()
  const transport: ServerTransport<object> = {
    getSessionId: (wire) => sessions.get(wire),
    setSessionId: (wire, id) => void sessions.set(wire, id),
    getConnId: () => null,
    sendNow: (wire, frame) => void sent.get(wire)!.push(decode(frame)),
    terminateConnection: (wire) => void terminated.add(wire),
  }
  const open = () => {
    const wire = {}
    sent.set(wire, [])
    mux.onConnectionOpen(wire, transport)
    return wire
  }
  const texts = (wire: object) => sent.get(wire)!.flatMap((frame) => (frame.tag === TAG.TEXT ? [frame.text] : []))
  const attachResults = (wire: object) =>
    sent.get(wire)!.flatMap((frame) => (frame.tag === TAG.ATTACH_RESULT ? [frame.lastSeq] : []))
  const count = (wire: object, tag: number) => sent.get(wire)!.filter((frame) => frame.tag === tag).length
  return { sessions, open, texts, attachResults, count, terminated }
}

/** A wire whose page has one channel attached, which counts what reaches its listeners. */
async function attachedWire() {
  const mux = new ChannelMux()
  const channel = new ServerChannel<unknown, never>({ id: 'burst' })
  const received = { count: 0 }
  channel.listen(() => void received.count++)
  channel.listenBinary(() => void received.count++)
  mux.registerChannel(channel)
  const { open, terminated } = wires(mux)
  const wire = open()
  await mux.onConnectionRawMessage(
    wire,
    encode.reconcile({ open: [{ id: 'burst', ix: 0, lastSeq: 0, initial: true }] }),
  )
  /** As a socket hands over every message of one read: all at once, before the server gets to any of them. */
  const deliver = (frames: Uint8Array<ArrayBuffer>[]) =>
    Promise.all(frames.map((frame) => mux.onConnectionRawMessage(wire, frame)))
  return { deliver, received, terminated: () => terminated.has(wire) }
}

test("a reconnect waiting for a new channel keeps the channels it moved when the previous wire's close lands", async () => {
  const mux = new ChannelMux()
  const clock = new ServerChannel<string, string>({ id: 'clock' })
  mux.registerChannel(clock)
  const { sessions, open, texts } = wires(mux)
  const previous = open()
  await mux.onConnectionRawMessage(
    previous,
    encode.reconcile({ open: [{ id: 'clock', ix: 0, lastSeq: 0, initial: true }] }),
  )
  const next = open()
  // The page reconnects with a callback's channel the server hasn't registered yet (its call failed in the drop).
  const reconciling = mux.onConnectionRawMessage(
    next,
    encode.reconcile({
      sessionId: sessions.get(previous),
      open: [
        { id: 'clock', ix: 0, lastSeq: 0 },
        { id: 'callback', ix: 1, lastSeq: 0, initial: true },
      ],
    }),
  )
  await new Promise((resolve) => setTimeout(resolve, 10))
  mux.onConnectionClosed(previous, { permanent: false }) // the previous wire's ping deadline, inside that wait
  mux.registerChannel(new ServerChannel({ id: 'callback' }))
  await reconciling
  void clock.send('tick')
  expect(texts(next).some((text) => text.includes('tick'))).toBe(true)
})

test('a stale session whose RECONCILED never reached the page leaves the channels a later reconnect moved', async () => {
  const mux = new ChannelMux()
  const clock = new ServerChannel<string, string>({ id: 'clock' })
  mux.registerChannel(clock)
  const { sessions, open, texts } = wires(mux)
  const first = open()
  await mux.onConnectionRawMessage(
    first,
    encode.reconcile({ open: [{ id: 'clock', ix: 0, lastSeq: 0, initial: true }] }),
  )
  const known = sessions.get(first)!
  mux.onConnectionClosed(first, { permanent: false })
  // The server answers this reconnect, but the network drops again before its RECONCILED reaches the page.
  const lost = open()
  await mux.onConnectionRawMessage(
    lost,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock', ix: 0, lastSeq: 0 }] }),
  )
  // So the page's next reconnect names the session it knows, and the channel moves on.
  const live = open()
  await mux.onConnectionRawMessage(
    live,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock', ix: 0, lastSeq: 0 }] }),
  )
  mux.onConnectionClosed(lost, { permanent: false }) // the dead wire's ping deadline
  void clock.send('tick')
  expect(texts(live).some((text) => text.includes('tick'))).toBe(true)
})

test("a new channel its client names after a reconcile naming one the server hasn't registered attaches within connectTtl, which ends one it never names", async () => {
  vi.useFakeTimers()
  try {
    const mux = new ChannelMux()
    const { sessions, open, count } = wires(mux)
    const wire = open()
    const closedWith = new Map<string, unknown>()
    for (const id of ['clock-ttl', 'never-named']) {
      const channel = new ServerChannel<string, string>({ id })
      channel.onClose((err) => void closedWith.set(id, err))
      mux.registerChannel(channel)
    }
    // The reconcile the client has in flight names a callback whose call was aborted, and is answered at once.
    const aborted = { id: 'aborted-callback', ix: 0, lastSeq: 0, initial: true as const }
    void mux.onConnectionRawMessage(wire, encode.reconcile({ open: [aborted] }))
    await vi.advanceTimersByTimeAsync(0)
    expect(count(wire, TAG.RECONCILED)).toBe(1)
    // So the client's next one names the new channel.
    const clock = { id: 'clock-ttl', ix: 1, lastSeq: 0, initial: true as const }
    await mux.onConnectionRawMessage(wire, encode.reconcile({ sessionId: sessions.get(wire), open: [aborted, clock] }))
    await vi.advanceTimersByTimeAsync(getServerConfig().channel.connectTtl + 500)
    expect(closedWith.has('clock-ttl')).toBe(false)
    expect(closedWith.get('never-named')).toBeInstanceOf(Error)
  } finally {
    vi.useRealTimers()
  }
})

test('a wire that drops while it awaits a channel still detaches the ones its reconcile re-attached, so what they send meanwhile replays', async () => {
  const mux = new ChannelMux()
  const clock = new ServerChannel<string, string>({ id: 'clock-held' })
  mux.registerChannel(clock)
  const { sessions, open, texts } = wires(mux)
  const wire = open()
  await mux.onConnectionRawMessage(
    wire,
    encode.reconcile({ open: [{ id: 'clock-held', ix: 0, lastSeq: 0, initial: true }] }),
  )
  const known = sessions.get(wire)!
  // The page adds a callback whose call was aborted, which the wire awaits; this reconcile re-attached the Clock.
  void mux.onConnectionRawMessage(
    wire,
    encode.reconcile({
      sessionId: known,
      open: [
        { id: 'clock-held', ix: 0, lastSeq: 0 },
        { id: 'aborted-callback', ix: 1, lastSeq: 0, initial: true },
      ],
    }),
  )
  await new Promise((resolve) => setTimeout(resolve, 10))
  mux.onConnectionClosed(wire, { permanent: false }) // the network drops while the wire awaits the callback
  expect((clock as unknown as { _peer: unknown })._peer).toBeNull()
  void clock.send('while-offline')
  const reconnected = open()
  await mux.onConnectionRawMessage(
    reconnected,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock-held', ix: 0, lastSeq: 0 }] }),
  )
  expect(texts(reconnected).some((text) => text.includes('while-offline'))).toBe(true)
})

test("what a channel sends once its reconnect's wire dropped while awaiting a lost callback replays on the next reconnect", async () => {
  const mux = new ChannelMux()
  const clock = new ServerChannel<string, string>({ id: 'clock-first' })
  mux.registerChannel(clock)
  const { sessions, open, texts } = wires(mux)
  const first = open()
  await mux.onConnectionRawMessage(
    first,
    encode.reconcile({ open: [{ id: 'clock-first', ix: 0, lastSeq: 0, initial: true }] }),
  )
  const known = sessions.get(first)!
  mux.onConnectionClosed(first, { permanent: false })
  // The reconnect names a callback whose call was lost in the cut, so its wire awaits it.
  const held = open()
  void mux.onConnectionRawMessage(
    held,
    encode.reconcile({
      sessionId: known,
      open: [
        { id: 'clock-first', ix: 0, lastSeq: 0 },
        { id: 'lost-callback', ix: 1, lastSeq: 0, initial: true },
      ],
    }),
  )
  await new Promise((resolve) => setTimeout(resolve, 10))
  mux.onConnectionClosed(held, { permanent: false }) // cut again, while it awaits the callback
  void clock.send('in-the-hold')
  const live = open()
  await mux.onConnectionRawMessage(
    live,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock-first', ix: 0, lastSeq: 0 }] }),
  )
  expect(texts(live).some((text) => text.includes('in-the-hold'))).toBe(true)
})

test("a burst of a channel's full message window, with the refresh and probe a page sends among it, is processed", async () => {
  const wire = await attachedWire()
  const frames = Array.from({ length: CREDIT_MSG_WINDOW_MAX }, (_, i) => encode.text(0, '1', i + 1))
  frames.push(encode.msgWindow(0, 2 * CREDIT_MSG_WINDOW_MAX), encode.bdpPing(0))
  await wire.deliver(frames)
  expect(wire.terminated()).toBe(false)
  expect(wire.received.count).toBe(CREDIT_MSG_WINDOW_MAX)
})

test("a burst of a channel's full byte window is processed", async () => {
  const wire = await attachedWire()
  const chunk = new Uint8Array(64 * 1024)
  const frames = Array.from({ length: CREDIT_WINDOW_MAX_BYTES / chunk.byteLength }, (_, i) =>
    encode.binary(0, chunk, i + 1),
  )
  await wire.deliver(frames)
  expect(wire.terminated()).toBe(false)
  expect(wire.received.count).toBe(frames.length)
})

test('a peer flooding past what its channels can have in flight is still terminated', async () => {
  const wire = await attachedWire()
  const frames = Array.from({ length: WIRE_RECV_BACKLOG_BASE_FRAMES + CREDIT_MSG_WINDOW_MAX + 1 }, (_, i) =>
    encode.text(0, '1', i + 1),
  )
  await wire.deliver(frames)
  expect(wire.terminated()).toBe(true)
})

test("a RECONCILE that crossed the ATTACH_RESULT saying its channel never registered doesn't await it again", async () => {
  vi.useFakeTimers()
  try {
    const mux = new ChannelMux()
    const { sessions, open, attachResults } = wires(mux)
    const wire = open()
    const lost = { id: 'lost-call-callback', ix: 0, lastSeq: 0, initial: true as const }
    await mux.onConnectionRawMessage(wire, encode.reconcile({ open: [lost] }))
    await vi.advanceTimersByTimeAsync(getServerConfig().channel.connectTtl)
    // The page named it again before that ATTACH_RESULT reached it, and releases it once it does.
    await mux.onConnectionRawMessage(wire, encode.reconcile({ sessionId: sessions.get(wire), open: [lost] }))
    const late = new ServerChannel({ id: lost.id })
    mux.registerChannel(late) // its call arrives after all
    await vi.advanceTimersByTimeAsync(10)
    expect(attachResults(wire)).toEqual([null])
    expect((late as unknown as { _peer: unknown })._peer).toBeNull()
  } finally {
    vi.useRealTimers()
  }
})

test('a wire that closes stops awaiting the channels it named', async () => {
  const mux = new ChannelMux()
  const { open } = wires(mux)
  const wire = open()
  await mux.onConnectionRawMessage(
    wire,
    encode.reconcile({ open: [{ id: 'callback-on-its-way', ix: 0, lastSeq: 0, initial: true }] }),
  )
  mux.onConnectionClosed(wire, { permanent: false })
  expect((mux as unknown as { pendingRegisterWaiters: Map<string, unknown> }).pendingRegisterWaiters.size).toBe(0)
  const late = new ServerChannel({ id: 'callback-on-its-way' })
  mux.registerChannel(late)
  expect((late as unknown as { _peer: unknown })._peer).toBeNull()
})

test('what a wire holds for a channel it awaits counts against its recv backlog', async () => {
  const mux = new ChannelMux()
  const { open, terminated } = wires(mux)
  const wire = open()
  await mux.onConnectionRawMessage(
    wire,
    encode.reconcile({ open: [{ id: 'never-registered', ix: 0, lastSeq: 0, initial: true }] }),
  )
  for (let seq = 1; seq <= WIRE_RECV_BACKLOG_BASE_FRAMES + 1 && !terminated.has(wire); seq++)
    await mux.onConnectionRawMessage(wire, encode.text(0, '0', seq))
  expect(terminated.has(wire)).toBe(true)
})

/** A page's SSE wire with a channel attached and a callback the server awaits, and the WebSocket it staged. */
async function upgradingWithAwaitedCallback(mux: ChannelMux) {
  mux.registerChannel(new ServerChannel({ id: 'clock-upgrading' }))
  const { sessions, open, texts, attachResults } = wires(mux)
  const old = open()
  const clock = { id: 'clock-upgrading', ix: 0, lastSeq: 0 }
  const callback = { id: 'callback-upgrading', ix: 1, lastSeq: 0, initial: true as const }
  await mux.onConnectionRawMessage(old, encode.reconcile({ open: [{ ...clock, initial: true }, callback] }))
  await mux.onConnectionRawMessage(old, encode.text(1, '"sent before its call arrived"', 1))
  const ws = open()
  const sessionId = sessions.get(old)!
  await mux.onConnectionRawMessage(ws, encode.prepare({ upgradeId: 'upgrade', sessionId }))
  const barrier = encode.barrier({ sessionId, upgradeId: 'upgrade', open: [clock, callback] })
  const backlog = (wire: object) =>
    (
      mux as unknown as { connectionEntries: Map<object, { state: { recvBacklogFrames: number } }> }
    ).connectionEntries.get(wire)!.state.recvBacklogFrames
  return { old, ws, barrier, callback, texts, attachResults, backlog }
}

test('a callback the barrier moves to the WebSocket gets what the old wire held for it once its call arrives there', async () => {
  const mux = new ChannelMux()
  const { old, ws, barrier, callback, texts, attachResults, backlog } = await upgradingWithAwaitedCallback(mux)
  await mux.onConnectionRawMessage(old, barrier)
  expect(backlog(ws)).toBe(1)
  const server = new ServerChannel<string, string>({ id: callback.id })
  const received: string[] = []
  server.listen((message) => void received.push(message))
  mux.registerChannel(server)
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(received).toEqual(['sent before its call arrived'])
  expect(attachResults(ws)).toEqual([1])
  expect(backlog(ws)).toBe(0)
  void server.send('over the WebSocket')
  expect(texts(ws).some((text) => text.includes('over the WebSocket'))).toBe(true)
})

test('a callback whose call arrives while the barrier listing it waits its turn gets what the old wire held for it, on the WebSocket', async () => {
  const mux = new ChannelMux()
  const { old, ws, barrier, callback, texts, attachResults } = await upgradingWithAwaitedCallback(mux)
  const committing = mux.onConnectionRawMessage(old, barrier)
  const server = new ServerChannel<string, string>({ id: callback.id })
  const received: string[] = []
  server.listen((message) => void received.push(message))
  mux.registerChannel(server) // before the barrier's turn
  await committing
  expect(received).toEqual(['sent before its call arrived'])
  expect([...attachResults(old), ...attachResults(ws)]).toEqual([])
  void server.send('over the WebSocket')
  expect(texts(ws).some((text) => text.includes('over the WebSocket'))).toBe(true)
})

/** A page's SSE wire with a channel attached, a channel returned to it later, and what its upgrade sends. */
async function upgradeOf(mux: ChannelMux) {
  const clock = { id: crypto.randomUUID(), ix: 0, lastSeq: 0 }
  const returned = { id: crypto.randomUUID(), ix: 1, lastSeq: 0 }
  for (const { id } of [clock, returned]) mux.registerChannel(new ServerChannel({ id }))
  const { sessions, open, count, terminated } = wires(mux)
  const old = open()
  await mux.onConnectionRawMessage(old, encode.reconcile({ open: [{ ...clock, initial: true }] }))
  const sessionId = sessions.get(old)!
  return {
    old,
    sessionId,
    open,
    count,
    terminated,
    session: () => sessions.get(old),
    prepare: (ws: object, upgradeId: string) =>
      mux.onConnectionRawMessage(ws, encode.prepare({ upgradeId, sessionId })),
    register: () =>
      mux.onConnectionRawMessage(old, encode.reconcile({ sessionId, open: [clock, { ...returned, initial: true }] })),
    barrier: (upgradeId: string) =>
      mux.onConnectionRawMessage(old, encode.barrier({ sessionId, upgradeId, open: [clock, returned] })),
  }
}

test('a channel the page registers on its SSE wire while its upgrade is staged leaves the session and the stage, so the barrier commits', async () => {
  const mux = new ChannelMux()
  const upgrade = await upgradeOf(mux)
  const ws = upgrade.open()
  await upgrade.prepare(ws, 'upgrade')
  await upgrade.register()
  expect(upgrade.session()).toBe(upgrade.sessionId)
  expect(upgrade.terminated.has(ws)).toBe(false)
  await upgrade.barrier('upgrade')
  expect(upgrade.count(ws, TAG.RECONCILED)).toBe(1)
})

test('a PREPARE the server reads after a registration on the SSE wire still stages, since the session stays', async () => {
  const mux = new ChannelMux()
  const upgrade = await upgradeOf(mux)
  const ws = upgrade.open()
  await upgrade.register()
  await upgrade.prepare(ws, 'upgrade')
  expect(upgrade.count(ws, TAG.READY)).toBe(1)
  await upgrade.barrier('upgrade')
  expect(upgrade.count(ws, TAG.RECONCILED)).toBe(1)
})

test("a page's next upgrade attempt replaces the stage its previous one left on the server", async () => {
  const mux = new ChannelMux()
  const upgrade = await upgradeOf(mux)
  const abandoned = upgrade.open()
  await upgrade.prepare(abandoned, 'first') // its READY never reached the page, and its close never reaches the server
  await upgrade.register()
  const ws = upgrade.open()
  await upgrade.prepare(ws, 'second')
  expect(upgrade.count(ws, TAG.READY)).toBe(1)
  expect(upgrade.terminated.has(abandoned)).toBe(true)
  await upgrade.barrier('second')
  expect(upgrade.count(ws, TAG.RECONCILED)).toBe(1)
})
