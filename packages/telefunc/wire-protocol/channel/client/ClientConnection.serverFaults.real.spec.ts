// The ends the server decides, on the real Node server and the real page over SSE: which channels a fault ends and with
// what cause, and what a request that meets the end is answered. A fetch wrapper stands for a page that breaks the
// protocol: it puts a frame the server can't take into the page's request, after a channel's text. The page drops the
// answer of a request in flight once its event stream says the session ended, so the request a hook changed is sent
// once more, as it went, and that answer is read: the server's for any request that meets the end.

import { expect, test } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { encodeU32 } from '../../wire/envelope.js'
import { ERROR_REASON, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ServerChannel } from '../server/ServerChannel.js'
import { ClientChannel } from './ClientChannel.js'
import {
  delay,
  findText,
  opens,
  pair,
  sends,
  textOf,
  until,
  uploads,
  useRealServer,
  world,
} from './realServer.fixture.js'

useRealServer(['sse'])

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
const severalFrames: Bad = () => framed(new Uint8Array([TAG.LENGTH, 0, 0, 0]))

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
    const key = crypto.randomUUID()
    const pairs = [pair(key), pair(key)]
    await Promise.all(pairs.map(({ page }) => page.opened))
    const ends = pairs.map(({ channel }) => endsOf(channel))
    sends(pairs[0]!.page, 'a1')
    await until(() => ends.every((each) => each.length > 0))
    expect(ends).toEqual([[['ChannelClosedError', told.unreadable]], [['ChannelClosedError', told.unreadable]]])
    expect(await answerAgain()).toEqual([400, framesNamingNoChannel(8)])
  },
)

test('the streaming upload carrying an over-limit frame of an index the session holds in no form ends the session: every channel ends on the server with one sentence of the connection', async () => {
  world.uploads = true
  world.hooks = [{ kind: 'upload', upload: uploadAfter('a1', overLimit(999)) }]
  const key = crypto.randomUUID()
  const pairs = [pair(key), pair(key)]
  await Promise.all(pairs.map(({ page }) => page.opened))
  await until(() => uploads() === 1)
  const ends = pairs.map(({ channel }) => endsOf(channel))
  sends(pairs[0]!.page, 'a1')
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
  await until(() => awaited.pageEnds.length > 0)
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
  await until(() => world.hooked.length > 0)
  await delay(200)
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
  const key = crypto.randomUUID()
  const pairs = [pair(key), pair(key)]
  await Promise.all(pairs.map(({ page }) => page.opened))
  const ends = pairs.map(({ channel }) => endsOf(channel))
  sends(pairs[0]!.page, 'a1')
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
