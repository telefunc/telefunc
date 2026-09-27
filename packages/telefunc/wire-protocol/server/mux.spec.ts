import { expect, test, vi } from 'vitest'
import { ChannelMux, type ServerTransport } from './mux.js'
import { ServerChannel } from './channel.js'
import { decode, encode, TAG, type DecodedFrame } from '../shared-ws.js'
import { getServerConfig } from '../../node/server/serverConfig.js'

/** Wires the test opens on `mux`, each recording what the server sends on it. */
function wires(mux: ChannelMux) {
  const sessions = new Map<object, string>()
  const sent = new Map<object, DecodedFrame[]>()
  const transport: ServerTransport<object> = {
    getSessionId: (wire) => sessions.get(wire),
    setSessionId: (wire, id) => void sessions.set(wire, id),
    getConnId: () => null,
    sendNow: (wire, frame) => void sent.get(wire)!.push(decode(frame)),
    terminateConnection: () => {},
  }
  const open = () => {
    const wire = {}
    sent.set(wire, [])
    mux.onConnectionOpen(wire, transport)
    return wire
  }
  const texts = (wire: object) => sent.get(wire)!.flatMap((frame) => (frame.tag === TAG.TEXT ? [frame.text] : []))
  return { sessions, open, texts }
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

test("a new channel outwaits a reconcile its client has in flight for a channel the server hasn't registered", async () => {
  vi.useFakeTimers()
  try {
    const mux = new ChannelMux()
    const clock = new ServerChannel<string, string>({ id: 'clock-ttl' })
    let closedWith: unknown = 'open'
    clock.onClose((err) => void (closedWith = err))
    mux.registerChannel(clock)
    const { connectTtl } = getServerConfig().channel
    // The client names this channel only once the server answers its held reconcile, up to connectTtl later.
    await vi.advanceTimersByTimeAsync(connectTtl + 500)
    expect(closedWith).toBe('open')
    await vi.advanceTimersByTimeAsync(connectTtl)
    expect(closedWith).toBeInstanceOf(Error)
  } finally {
    vi.useRealTimers()
  }
})

test('a wire that drops while a reconcile on it is held still detaches its channels, so what they send meanwhile replays', async () => {
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
  // The page adds a callback whose call was aborted: the server holds this reconcile, which re-attached the Clock.
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
  mux.onConnectionClosed(wire, { permanent: false }) // the network drops during the hold
  expect((clock as unknown as { _peer: unknown })._peer).toBeNull()
  void clock.send('while-offline')
  const reconnected = open()
  await mux.onConnectionRawMessage(
    reconnected,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock-held', ix: 0, lastSeq: 0 }] }),
  )
  expect(texts(reconnected).some((text) => text.includes('while-offline'))).toBe(true)
})

test("a frame sent to a reconnect's wire that dropped while its first reconcile was held replays on the next reconnect", async () => {
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
  // The reconnect names a callback whose call was lost in the cut, so the server holds its first reconcile.
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
  mux.onConnectionClosed(held, { permanent: false }) // cut again, inside the hold
  void clock.send('in-the-hold')
  const live = open()
  await mux.onConnectionRawMessage(
    live,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock-first', ix: 0, lastSeq: 0 }] }),
  )
  expect(texts(live).some((text) => text.includes('in-the-hold'))).toBe(true)
})
