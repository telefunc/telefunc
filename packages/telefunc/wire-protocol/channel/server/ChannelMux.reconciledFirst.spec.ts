// A page that reconnects reads its RECONCILED first, then what the server replays of each channel, then what the app
// sent on the channel after the attach, in seq order: on both of the server's SSE paths, the Web stream a Web adapter
// answers with and the Node `Readable` the Node adapter pipes to its socket. The real SSE handler and ChannelMux run;
// the page is the test, reading the event stream's frames.

import { expect, test, vi } from 'vitest'

import { loadStreamNodeModuleOnce } from '../../../utils/loadStreamNodeModule.js'
import { decode, encode } from '../../wire/frame.js'
import { HEADER } from '../../wire/header.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { SessionRegistry } from './SessionRegistry.js'
import { ServerChannel } from './ServerChannel.js'
import { openStream, openWebSocket, order } from './serverPage.fixture.js'

/** A frame that carries `message`, which a channel's `send` serializes. */
const carries = (message: string) => (frame: Uint8Array) => {
  const decoded = decode(frame)
  return decoded.tag === TAG.TEXT && decoded.text === JSON.stringify(message)
}

test.each(['web', 'node'] as const)(
  'over SSE on the %s path, a reconnect reads the RECONCILED, then the replay, then what the app sent in the gap before the RECONCILED went out, lossy sends too',
  async (path) => {
    // The Node adapter loads `node:stream` before it serves; the server's push readable reads it from there.
    await loadStreamNodeModuleOnce()
    const mux = getChannelMux()
    const channel = new ServerChannel()
    mux.registerChannel(channel)

    // The page attaches the channel; the app sends two messages, which the page doesn't confirm.
    const first = await openStream(
      path,
      encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
    )
    const [reconciled] = await first.readUntil((frame) => frame[0] === TAG.RECONCILED)
    const answer = reconciled === undefined ? undefined : decode(reconciled)
    if (answer?.tag !== TAG.RECONCILED) throw new Error('no RECONCILED on the first wire')
    void channel.send('a')
    void channel.send('b')
    await first.readUntil(carries('b'))
    await first.drop()

    // The page reconnects. The app sends a third message and two lossy ones after the server attached the channel to
    // the new wire and before the RECONCILED goes out: on SSE, the stream waits for the batch POSTs in flight between
    // the two. A page that keeps up gets every lossy send.
    const sendReconciled = mux.sendReconciled.bind(mux)
    vi.spyOn(mux, 'sendReconciled').mockImplementationOnce((outcome) => {
      void channel.send('c')
      void channel.send('l1', { lossy: true })
      void channel.send('l2', { lossy: true })
      sendReconciled(outcome)
    })
    const second = await openStream(
      path,
      encode.reconcile({ sessionId: answer.payload.sessionId, open: [{ id: channel.id, ix: 0, lastSeq: 0 }] }),
    )
    expect(order(await second.readUntil(carries('l2')))).toEqual(['RECONCILED', '1:a', '2:b', '3:c', '4:l1', '5:l2'])
    await second.drop()
    vi.restoreAllMocks()
  },
)

test('over a WebSocket, a reconnect reads the RECONCILED, then the replay, then what the app sent in the gap before the RECONCILED went out, lossy sends too', async () => {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel)
  const first = openWebSocket({ reads: true })
  await mux.onWireMessage(
    first.wire,
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const { sessionId } = first.wire
  if (sessionId === undefined) throw new Error('no session on the first wire')
  void channel.send('a')
  void channel.send('b')
  first.close(false)

  const sendReconciled = mux.sendReconciled.bind(mux)
  vi.spyOn(mux, 'sendReconciled').mockImplementationOnce((outcome) => {
    void channel.send('c')
    void channel.send('l1', { lossy: true })
    void channel.send('l2', { lossy: true })
    sendReconciled(outcome)
  })
  const second = openWebSocket({ reads: true })
  await mux.onWireMessage(second.wire, encode.reconcile({ sessionId, open: [{ id: channel.id, ix: 0, lastSeq: 0 }] }))
  expect(order(second.frames)).toEqual(['RECONCILED', '1:a', '2:b', '3:c', '4:l1', '5:l2'])
  vi.restoreAllMocks()
  second.close()
})
test('a WebSocket message of two RECONCILEs ends its wire: a page has one RECONCILE out at a time', async () => {
  const page = openWebSocket()
  const first = encode.reconcile({ open: [] })
  const second = encode.reconcile({ open: [] })
  await page.mux.onWireMessage(
    page.wire,
    encode.message([first, second], first.byteLength + HEADER + second.byteLength),
  )
  for (let i = 0; i < 200 && !page.state.terminated; i++) await page.mux.onWireMessage(page.wire, encode.ping([]))
  expect(page.state.terminated).toBe(true)
  page.close()
})

test("a BARRIER riding behind a PING in one WebSocket message gets its RECONCILED on the WebSocket it commits, which then carries the channel's frames", async () => {
  const old = openWebSocket({ reads: true })
  const channel = new ServerChannel()
  old.mux.registerChannel(channel)
  await old.mux.onWireMessage(
    old.wire,
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const sessionId = old.wire.sessionId
  if (sessionId === undefined) throw new Error('no session on the old wire')
  const upgraded = openWebSocket({ reads: true })
  await upgraded.mux.onWireMessage(upgraded.wire, encode.prepare({ upgradeId: 'u1', sessionId }))
  const ping = encode.ping([])
  const barrier = encode.barrier({ sessionId, upgradeId: 'u1', open: [{ id: channel.id, ix: 0, lastSeq: 0 }] })
  await old.mux.onWireMessage(old.wire, encode.message([ping, barrier], ping.byteLength + HEADER + barrier.byteLength))
  void channel.send('after the upgrade')
  await vi.waitFor(() => expect(upgraded.frames.some((frame) => frame[0] === TAG.TEXT)).toBe(true))
  expect(order(upgraded.frames)).toEqual(['RECONCILED', '1:after the upgrade'])
  old.close()
  upgraded.close()
})

test('a WebSocket message of a RECONCILE and a BARRIER ends its wire: a page has one of them out at a time', async () => {
  const old = openWebSocket({ reads: true })
  const channel = new ServerChannel()
  old.mux.registerChannel(channel)
  const attach = { id: channel.id, ix: 0, lastSeq: 0 }
  await old.mux.onWireMessage(old.wire, encode.reconcile({ open: [{ ...attach, initial: true }] }))
  const sessionId = old.wire.sessionId
  if (sessionId === undefined) throw new Error('no session on the old wire')
  const upgraded = openWebSocket({ reads: true })
  await upgraded.mux.onWireMessage(upgraded.wire, encode.prepare({ upgradeId: 'u1', sessionId }))
  const reconcile = encode.reconcile({ sessionId, open: [attach] })
  const barrier = encode.barrier({ sessionId, upgradeId: 'u1', open: [attach] })
  await old.mux.onWireMessage(
    old.wire,
    encode.message([reconcile, barrier], reconcile.byteLength + HEADER + barrier.byteLength),
  )
  expect(old.state.terminated).toBe(true)
  old.close()
  upgraded.close()
})

test('a reconcile that throws a bug ends its wire, transiently: nothing stays held on a wire that is up', async () => {
  const page = openWebSocket()
  vi.spyOn(SessionRegistry.prototype, 'setSession').mockImplementationOnce(() => {
    throw new Error('a bug in reconcile')
  })
  vi.spyOn(console, 'error').mockImplementation(() => {})
  await page.mux.onWireMessage(page.wire, encode.reconcile({ open: [] }))
  await Promise.resolve()
  expect(page.state.terminated).toBe(true)
  // The server's bug, not the page's fault: the page's channels keep their reconnect grace.
  expect(page.mux.readPermanentTermination(page.wire)).toBe(false)
  vi.restoreAllMocks()
  page.close()
})
