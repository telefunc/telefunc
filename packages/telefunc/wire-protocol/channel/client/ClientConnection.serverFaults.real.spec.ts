// The ends the server decides, on the real Node server and the real page: which channels a fault ends and with what
// cause on both ends, and what a request that meets the end is answered. A fetch wrapper stands for a page that breaks
// the protocol: it puts a frame the server can't take into the page's request, after a channel's text. The page drops
// the answer of a request in flight once its event stream says the session ended, so the request a hook changed is sent
// once more, as it went, and that answer is read: the server's for any request that meets the end. Where a row asks
// what the page makes of one carrier of the end, the wrapper keeps the others from it.

import { expect, test } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { encodeU32 } from '../../wire/envelope.js'
import { ERROR_REASON, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ServerChannel } from '../server/ServerChannel.js'
import { ClientChannel } from './ClientChannel.js'
import {
  delay,
  findText,
  framesStart,
  opens,
  pair,
  sends,
  textOf,
  until,
  uploads,
  useRealServer,
  withholding,
  world,
} from './realServer.fixture.js'

useRealServer(['sse', 'ws'])

const told = {
  unreadable:
    "Channel closed: the page's connection ended: The server can't read what the page sent on its connection.",
  overLimit: "Channel closed: the server refused what the page sent on it: A frame is over the server's frame limit.",
  refused: "Channel closed: the server refused what the page sent on it: The server can't take a frame of the page's.",
}

/** The frames put in after the frame of a channel's text, given that frame's channel and seq. */
type Bad = (text: { ix: number; seq: number }) => Uint8Array<ArrayBuffer>[]
const framed = (frame: Uint8Array<ArrayBuffer>) => [encodeU32(frame.byteLength), frame]
/** The header of a frame of channel `ix` (the text's own where 'own') whose length says more than the limit. */
const overLimit =
  (ix: number | 'own'): Bad =>
  (text) => [
    encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1),
    encode.close(ix === 'own' ? text.ix : ix, 0, text.seq + 1).subarray(0, 7),
  ]
/** A frame of channel `ix` that only a server sends. */
const serverOnly =
  (ix: number): Bad =>
  () =>
    framed(encode.attachResult(ix, null))
/** A frame of the text's own channel the protocol doesn't allow: a page sends no ERROR but a lost one. */
const refusedOwn: Bad = (text) => framed(encode.error(text.ix, ERROR_REASON.UNCONFIRMED, text.seq + 1))
/** A message of several frames, which SSE doesn't carry: the server can't read the request's frames. */
const unframeable = () => framed(new Uint8Array([TAG.LENGTH, 0, 0, 0]))
const severalFrames: Bad = unframeable

/** A batch POST's body with `bad` after the frame that carries `text`. */
const after = (text: string, bad: Bad) => (bytes: Uint8Array<ArrayBuffer>) => {
  const found = findText(bytes, text)
  return found === null ? [bytes] : [bytes.subarray(0, found.end), ...bad(found), bytes.subarray(found.end)]
}

/** The streaming upload with `bad` after the frame that carries `text`: a frame is a chunk, after its length's. */
const uploadAfter = (text: string, bad: Bad) => (body: ReadableStream<Uint8Array>) =>
  body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform: (chunk, controller) => {
        controller.enqueue(chunk)
        const found = textOf(new Uint8Array(chunk), text)
        if (found !== null) for (const part of bad(found)) controller.enqueue(part)
      },
    }),
  )

/** How `channel` ended, each time: its error's class and message. */
function endsOf(channel: ServerChannel<string, string> | ClientChannel<string, string>): [string, string][] {
  const ends: [string, string][] = []
  channel.onClose((err) => void ends.push(err === null ? ['clean', ''] : [err.name, err.message]))
  return ends
}

/** Two channels on the page's connection `connectionKey`, over `transports`. */
function opening(
  connectionKey: string,
  transports: ('sse' | 'ws')[] = ['sse'],
): [ReturnType<typeof pair>, ReturnType<typeof pair>] {
  return [pair(connectionKey, transports), pair(connectionKey, transports)]
}

/** A channel the page opens whose server end isn't registered yet: the server awaits it. */
function awaitedPair(connectionKey: string) {
  const channel = new ServerChannel<string, string>()
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: world.telefuncUrl, transports: ['sse'], idleTimeout: 0, connectionKey },
  })
  void page.opened.catch(() => {})
  return { channel, page, serverEnds: endsOf(channel), pageEnds: endsOf(page) }
}

/** The answer to the request the hook changed, sent once more as it went: status and `Content-Type`. */
async function answerAgain(): Promise<[number, string | null]> {
  const [hooked] = world.hooked
  if (!hooked) throw new Error('no request was changed')
  const answer = await hooked.again()
  return [answer.status, answer.headers.get('content-type')]
}

const framesNamingNoChannel = (cause: number) =>
  expect.stringMatching(new RegExp(`^text/vnd\\.telefunc\\.refusal; kind=frames; cause=${cause}; reason="[^"]+"$`))

test.each([
  ['an over-limit frame', overLimit(999)],
  ['a frame only a server sends', serverOnly(999)],
])(
  'a batch POST carrying %s of an index the session holds in no form ends the session: every channel ends on the server with one sentence of the connection, and a request that meets the end is refused `frames` naming no channel',
  async (_bad, bad) => {
    world.hooks = [{ kind: 'post', text: 'a1', body: after('a1', bad) }]
    const [first, second] = opening(crypto.randomUUID())
    await Promise.all([first.page.opened, second.page.opened])
    const ends = [first, second].map(({ channel }) => endsOf(channel))
    sends(first.page, 'a1')
    await until(() => ends.every((each) => each.length > 0))
    expect(ends).toEqual([[['ChannelClosedError', told.unreadable]], [['ChannelClosedError', told.unreadable]]])
    expect(await answerAgain()).toEqual([400, framesNamingNoChannel(8)])
  },
)

test('the streaming upload carrying an over-limit frame of an index the session holds in no form ends the session: every channel ends on the server with one sentence of the connection', async () => {
  world.uploads = true
  world.hooks = [{ kind: 'upload', upload: uploadAfter('a1', overLimit(999)) }]
  const [first, second] = opening(crypto.randomUUID())
  await Promise.all([first.page.opened, second.page.opened])
  await until(() => uploads() === 1)
  const ends = [first, second].map(({ channel }) => endsOf(channel))
  sends(first.page, 'a1')
  await until(() => ends.every((each) => each.length > 0))
  expect(ends).toEqual([[['ChannelClosedError', told.unreadable]], [['ChannelClosedError', told.unreadable]]])
})

test("an over-limit frame of a channel the server still awaits ends that channel alone: the other's text arrives once and it lives on, and the awaited channel ends on the server with the same error once it registers", async () => {
  world.hooks = [{ kind: 'post', text: 'b1', body: after('b1', overLimit('own')) }]
  const key = crypto.randomUUID()
  const other = pair(key)
  await other.page.opened
  const otherEnds = endsOf(other.channel)
  const awaited = awaitedPair(key)
  sends(awaited.page, 'b1')
  sends(other.page, 'a1')
  // The other's text may go again on the next wire, after a reconnect: its arrival is waited for, then whether it came
  // once.
  await until(() => awaited.pageEnds.length > 0 && other.got.length > 0)
  await delay(500)
  expect(awaited.pageEnds).toEqual([['ChannelClosedError', told.overLimit]])
  expect([other.ended, otherEnds, other.got]).toEqual([[], [], ['a1']])
  getChannelMux().registerChannel(awaited.channel)
  await until(() => awaited.serverEnds.length > 0)
  expect(awaited.serverEnds).toEqual([['ChannelClosedError', told.overLimit]])
})

test('a held frame the server refuses as its channel registers ends that channel alone, on both ends, and the wire goes on', async () => {
  world.hooks = [{ kind: 'post', text: 'b1', body: after('b1', refusedOwn) }]
  const key = crypto.randomUUID()
  const other = pair(key)
  await other.page.opened
  const otherEnds = endsOf(other.channel)
  const awaited = awaitedPair(key)
  sends(awaited.page, 'b1')
  // The POST's own answer says the server took the held frame: the channel registers only after it.
  await until(() => world.hooked[0]?.status === 200)
  const opened = opens()
  getChannelMux().registerChannel(awaited.channel)
  await until(() => awaited.serverEnds.length > 0 && awaited.pageEnds.length > 0)
  await delay(300)
  expect([awaited.serverEnds, awaited.pageEnds]).toEqual([
    [['ChannelClosedError', told.refused]],
    [['ChannelClosedError', told.refused]],
  ])
  expect([other.ended, otherEnds, opens()]).toEqual([[], [], opened])
  sends(other.page, 'a1')
  await until(() => other.got.includes('a1'))
})

test("a body whose frames the server can't read ends the session: every channel ends on the server with one sentence of the connection, and a request that meets the end is refused `frames` naming no channel", async () => {
  world.hooks = [{ kind: 'post', text: 'a1', body: after('a1', severalFrames) }]
  const [first, second] = opening(crypto.randomUUID())
  await Promise.all([first.page.opened, second.page.opened])
  const ends = [first, second].map(({ channel }) => endsOf(channel))
  sends(first.page, 'a1')
  await until(() => ends.every((each) => each.length > 0))
  expect(ends).toEqual([[['ChannelClosedError', told.unreadable]], [['ChannelClosedError', told.unreadable]]])
  expect(await answerAgain()).toEqual([400, framesNamingNoChannel(8)])
})

test("a request that meets the wire a channel's over-limit frame closed is refused as that frame was: `frames` naming its channel", async () => {
  world.hooks = [{ kind: 'post', text: 'a1', body: after('a1', overLimit('own')) }]
  const refusedPair = pair(crypto.randomUUID())
  await refusedPair.page.opened
  sends(refusedPair.page, 'a1')
  await until(() => refusedPair.ended.length > 0)
  expect(await answerAgain()).toEqual([
    400,
    expect.stringMatching(/^text\/vnd\.telefunc\.refusal; kind=frames; channel=\d+; cause=5; reason="[^"]+"$/),
  ])
})

/** How a channel that sent nothing wrong ends on the page as the server ends its session. */
const sessionEndOnPage = [['ChannelClosedError', told.unreadable]]

/** The event stream without the channels' ERROR frames, held open past the server's end of it: the page learns of the
 *  session's end from the answer to its request alone. */
function answerDecides(body: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
  const reader = withholding((frame) => frame[0] === TAG.ERROR)
    .stream(body)
    .getReader()
  return new ReadableStream<Uint8Array>({
    pull: async (controller) => {
      const { done, value } = await reader.read()
      if (done) return new Promise<void>(() => {})
      controller.enqueue(value)
    },
    cancel: (reason) => reader.cancel(reason),
  })
}

test("a batch POST of two channels' data the server refuses `frames` naming no channel ends both on the page with the server's sentence of the connection", async () => {
  const carried: string[] = []
  world.hooks = [
    { kind: 'open', stream: answerDecides },
    {
      kind: 'post',
      text: 'b1',
      body: (bytes) => {
        carried.push(...['a1', 'b1'].filter((text) => findText(bytes, text) !== null))
        return after('b1', severalFrames)(bytes)
      },
    },
  ]
  const [first, second] = opening(crypto.randomUUID())
  await Promise.all([first.page.opened, second.page.opened])
  const ends = [first, second].map(({ page }) => endsOf(page))
  sends(first.page, 'a1')
  sends(second.page, 'b1')
  await until(() => ends.every((each) => each.length > 0))
  expect([carried, ends]).toEqual([
    ['a1', 'b1'],
    [sessionEndOnPage, sessionEndOnPage],
  ])
})

test("the streaming upload the server refuses `frames` naming no channel ends every channel on the page with the server's sentence of the connection", async () => {
  world.uploads = true
  world.hooks = [
    { kind: 'open', stream: answerDecides },
    { kind: 'upload', upload: uploadAfter('a1', overLimit(999)) },
  ]
  const [first, second] = opening(crypto.randomUUID())
  await Promise.all([first.page.opened, second.page.opened])
  await until(() => uploads() === 1)
  const ends = [first, second].map(({ page }) => endsOf(page))
  sends(first.page, 'a1')
  await until(() => ends.every((each) => each.length > 0))
  expect(ends).toEqual([sessionEndOnPage, sessionEndOnPage])
})

test("the barrier POST of a move to a WebSocket the server refuses `frames` naming no channel ends every channel on the page with the server's sentence of the connection", async () => {
  let requestsBefore = -1
  world.hooks = [
    { kind: 'open', stream: answerDecides },
    {
      kind: 'post',
      carries: TAG.BARRIER,
      body: (bytes) => {
        requestsBefore = world.started.length
        return [bytes.subarray(0, framesStart(bytes)), ...unframeable(), bytes.subarray(framesStart(bytes))]
      },
    },
  ]
  const [first, second] = opening(crypto.randomUUID(), ['sse', 'ws'])
  const ends = [first, second].map(({ page }) => endsOf(page))
  await until(() => ends.every((each) => each.length > 0))
  // No request went after the barrier's, such as a PING meeting the end.
  expect([world.started.slice(requestsBefore), ends]).toEqual([['post'], [sessionEndOnPage, sessionEndOnPage]])
})

test('a session the server ends tells each channel on the page its end on the event stream alone, and the page opens no other wire', async () => {
  world.hooks = [{ kind: 'post', text: 'a1', body: after('a1', severalFrames), held: true }]
  const [first, second] = opening(crypto.randomUUID())
  await Promise.all([first.page.opened, second.page.opened])
  const ends = [first, second].map(({ page }) => endsOf(page))
  sends(first.page, 'a1')
  await until(() => ends.every((each) => each.length > 0))
  await delay(500)
  expect([ends, opens()]).toEqual([[sessionEndOnPage, sessionEndOnPage], 1])
})

/** The channel and seq of the frame of `text` a WebSocket message carries; null where it carries none. */
function textIn(message: Uint8Array<ArrayBuffer>, text: string): { ix: number; seq: number } | null {
  for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
    const found = textOf(frame, text)
    if (found !== null) return found
  }
  return null
}

test('a session the server ends on a WebSocket tells each channel on the page its end, and the page opens no other socket', async () => {
  world.wsSends = (message) => (textIn(message, 'a1') ? [message, encode.attachResult(999, null)] : [message])
  const [first, second] = opening(crypto.randomUUID(), ['ws'])
  await Promise.all([first.page.opened, second.page.opened])
  const ends = [first, second].map(({ page }) => endsOf(page))
  sends(first.page, 'a1')
  await until(() => ends.every((each) => each.length > 0))
  await delay(500)
  expect([ends, world.sockets]).toEqual([[sessionEndOnPage, sessionEndOnPage], 1])
})

test("a frame of one channel the server refuses on a WebSocket ends that channel on the page with its cause before the page opens another socket, and the other's texts arrive once each", async () => {
  world.wsSends = (message) => {
    const found = textIn(message, 'a1')
    return found ? [message, encode.error(found.ix, ERROR_REASON.UNCONFIRMED, found.seq + 1)] : [message]
  }
  const [refused, other] = opening(crypto.randomUUID(), ['ws'])
  await Promise.all([refused.page.opened, other.page.opened])
  const [refusedEnds, otherEnds] = [endsOf(refused.page), endsOf(other.page)]
  let socketsAtEnd = 0
  refused.page.onClose(() => void (socketsAtEnd = world.sockets))
  sends(other.page, 'b1')
  sends(refused.page, 'a1')
  await until(() => refusedEnds.length > 0)
  sends(other.page, 'b2')
  await until(() => other.got.length === 2)
  await delay(300)
  expect([refusedEnds, socketsAtEnd, otherEnds, other.got]).toEqual([
    [['ChannelClosedError', told.refused]],
    1,
    [],
    ['b1', 'b2'],
  ])
})
