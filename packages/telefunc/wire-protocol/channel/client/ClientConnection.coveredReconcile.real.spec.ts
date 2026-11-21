// A RECONCILE an end covers, on the real Node server with the real page: the end of its own wire, read before its turn,
// or the kept end of the session it names. A channel it names that the server hasn't registered yet ends with that
// end's cause as it registers, as the page's end of it does.

import { expect, test, vi } from 'vitest'
import { decodeU32 } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, lengthBytes, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ServerChannel } from '../server/ServerChannel.js'
import { ClientChannel } from './ClientChannel.js'
import { delay, pair, rewriting, until, useRealServer, world } from './realServer.fixture.js'

// A receive backlog the test can pass in one message: the base and one channel's window.
vi.mock('../../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../constants.js')>()),
  CREDIT_MSG_WINDOW_MAX: 64,
  WIRE_RECV_BACKLOG_BASE_FRAMES: 64,
}))

useRealServer(['sse', 'ws'], { reconnectTimeout: 1_500 })

const budget = 'The page sent more on its connection than the server holds while it works on what came before.'

/** The page's end of server channel `id`, which the server hasn't registered, on the page's connection `connectionKey`. */
function pageOf(id: string, connectionKey: string, transports: ('sse' | 'ws')[]) {
  const page = new ClientChannel<string, string>({
    channelId: id,
    connection: { telefuncUrl: world.telefuncUrl, transports, idleTimeout: 0, connectionKey },
  })
  void page.opened.catch(() => {})
  const ended: string[] = []
  page.onClose((err) => void ended.push(err ? err.message : 'clean'))
  return { page, ended }
}

/** The connId an SSE open's body names, read off its metadata. */
function connIdOf(bytes: Uint8Array<ArrayBuffer>): string {
  const metadata: { connId: string } = JSON.parse(new TextDecoder().decode(bytes.subarray(4, 4 + decodeU32(bytes))))
  return metadata.connId
}

/** Whether `message` carries a RECONCILE that names channel `id`. */
function namesChannel(message: Uint8Array<ArrayBuffer>, id: string): boolean {
  return (isOneFrame(message) ? [message] : [...messageFrames(message)]).some((frame) => {
    const decoded = frame[0] === TAG.RECONCILE ? decode(frame) : null
    return decoded?.tag === TAG.RECONCILE && decoded.payload.open.some((each) => each.id === id)
  })
}

test("a WebSocket message whose frames behind the page's RECONCILE for a new channel pass the receive budget: the server's channel ends with the budget's cause as it registers, as the page's does", async () => {
  const connectionKey = crypto.randomUUID()
  const live = pair(connectionKey, ['ws'])
  await live.page.opened
  const added = new ServerChannel<string, string>()
  // Past the base and the window of the one channel the session holds, read before the RECONCILE's turn.
  const flood = Array.from({ length: 200 }, (_, i) => encode.text(0, '"x"', i + 1))
  world.wsSends = (message) => {
    if (!namesChannel(message, added.id)) return [message]
    world.wsSends = null
    const frames = [...(isOneFrame(message) ? [message] : messageFrames(message)), ...flood]
    const bytes = frames.reduce(
      (sum, frame, i) => sum + frame.byteLength + (i < frames.length - 1 ? lengthBytes(frame[0] ?? 0) : 0),
      0,
    )
    return [encode.message(frames, bytes)]
  }
  const addedPage = pageOf(added.id, connectionKey, ['ws'])
  const serverEnds: string[] = []
  added.onClose((err) => void serverEnds.push(err ? err.message : 'clean'))
  try {
    await until(() => live.ended.length > 0 && addedPage.ended.length > 0)
    getChannelMux().registerChannel(added)
    await delay(20)
    expect({
      page: [live.ended.map((err) => err.message.includes(budget)), addedPage.ended.map((end) => end.includes(budget))],
      server: serverEnds.map((end) => end.includes(budget)),
    }).toEqual({ page: [[true], [true]], server: [true] })
  } finally {
    live.page.abort()
    addedPage.page.abort()
    added.abort()
  }
})

test("an SSE reopen answered with its session's kept end names a channel opened while the page was away: the server's channel ends with the end's cause as it registers, as the page's does", async () => {
  let connId = ''
  // The page never reads the first wire's last word, and its RECONCILE for the new channel reaches the server only
  // once that wire is gone: the reopen is the first RECONCILE that names it there.
  const first = rewriting((frame) => (frame[0] === TAG.END ? null : frame))
  world.hooks = [
    {
      kind: 'open',
      stream: first.stream,
      body: (bytes) => {
        connId = connIdOf(bytes)
        return [bytes]
      },
    },
    { kind: 'post', carries: TAG.RECONCILE, delayMs: 3_000 },
  ]
  const connectionKey = crypto.randomUUID()
  const live = pair(connectionKey, ['sse'])
  await live.page.opened
  const added = new ServerChannel<string, string>()
  const addedPage = pageOf(added.id, connectionKey, ['sse'])
  const serverEnds: string[] = []
  added.onClose((err) => void serverEnds.push(err ? err.message : 'clean'))
  try {
    await delay(200)
    const wire = getChannelMux().getWireByConnId(connId)
    if (wire !== undefined) getChannelMux().endFault(wire, { scope: 'session', cause: 'receiveBudget' })
    first.cut()
    await until(() => live.ended.length > 0 && addedPage.ended.length > 0)
    getChannelMux().registerChannel(added)
    await delay(20)
    expect({
      wire: wire !== undefined,
      page: [live.ended.map((err) => err.message.includes(budget)), addedPage.ended.map((end) => end.includes(budget))],
      server: serverEnds.map((end) => end.includes(budget)),
    }).toEqual({ wire: true, page: [[true], [true]], server: [true] })
  } finally {
    live.page.abort()
    addedPage.page.abort()
    added.abort()
  }
}, 15_000)
