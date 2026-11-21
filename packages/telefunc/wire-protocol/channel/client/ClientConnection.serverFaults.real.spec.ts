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
  framesOf,
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

/** The streaming upload with `bad` after the frame that carries `text`, which stays open once the page ends its body,
 *  as a page's that is gone before its end reached the server; `vanish()` drops its connection. */
function vanishingAfter(text: string, bad: Bad) {
  let vanish = () => {}
  const upload = (body: ReadableStream<Uint8Array>) =>
    body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        start: (controller) => void (vanish = () => controller.error(new Error('the page is gone'))),
        transform: (chunk, controller) => {
          controller.enqueue(chunk)
          const found = textOf(new Uint8Array(chunk), text)
          if (found !== null) for (const part of bad(found)) controller.enqueue(part)
        },
        flush: () => new Promise<void>(() => {}),
      }),
    )
  return { upload, vanish: () => vanish() }
}

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

/** The event stream without the channels' ERROR frames and its END, held open past the server's end of it: the page
 *  learns of the session's end from the answer to its request alone. `withheld`: the tags of the frames it kept from the
 *  page. */
function answerDecides() {
  const withheld: number[] = []
  const stream = (body: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> => {
    const reader = withholding((frame) => {
      const terminal = frame[0] === TAG.ERROR || frame[0] === TAG.END
      if (terminal) withheld.push(frame[0] ?? 0)
      return terminal
    })
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
  return { stream, withheld }
}

/** The streaming upload with `bad` after the frame that carries `text`, then its end: the server answers it once it
 *  read the body's end, which the page, kept from the stream's END, wouldn't send. */
const endingAfter = (text: string, bad: Bad) => (body: ReadableStream<Uint8Array>) =>
  body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform: (chunk, controller) => {
        controller.enqueue(chunk)
        const found = textOf(new Uint8Array(chunk), text)
        if (found === null) return
        for (const part of bad(found)) controller.enqueue(part)
        controller.terminate()
      },
    }),
  )

test("a batch POST of two channels' data the server refuses `frames` naming no channel ends both on the page with the server's sentence of the connection", async () => {
  const carried: string[] = []
  const decides = answerDecides()
  world.hooks = [
    { kind: 'open', stream: decides.stream },
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
  // From that answer alone: the stream's END was kept from the page, and no other wire opened to read the kept end.
  expect([carried, ends, decides.withheld.includes(TAG.END), opens()]).toEqual([
    ['a1', 'b1'],
    [sessionEndOnPage, sessionEndOnPage],
    true,
    1,
  ])
})

test("the streaming upload the server refuses `frames` naming no channel ends every channel on the page with the server's sentence of the connection", async () => {
  world.uploads = true
  const decides = answerDecides()
  world.hooks = [
    { kind: 'open', stream: decides.stream },
    { kind: 'upload', upload: endingAfter('a1', overLimit(999)) },
  ]
  const [first, second] = opening(crypto.randomUUID())
  await Promise.all([first.page.opened, second.page.opened])
  await until(() => uploads() === 1)
  const ends = [first, second].map(({ page }) => endsOf(page))
  sends(first.page, 'a1')
  await until(() => ends.every((each) => each.length > 0))
  // From that answer alone: the stream's END was kept from the page, and no other wire opened to read the kept end.
  expect([ends, decides.withheld.includes(TAG.END), opens()]).toEqual([[sessionEndOnPage, sessionEndOnPage], true, 1])
})

/** The page's first WebSocket, whose close the page reads `ms` after it came. */
function closingFirstSocketLate(ms: number) {
  const Counted = globalThis.WebSocket
  let made = 0
  globalThis.WebSocket = class ClosingLate extends Counted {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols)
      if (made++ > 0) return
      let held = true
      // Added before the page sets its handler, so it runs first and can keep the close from it until it is let go.
      this.addEventListener('close', (event) => {
        if (!held) return
        held = false
        event.stopImmediatePropagation()
        const { code, reason } = event
        setTimeout(() => this.dispatchEvent(Object.assign(new Event('close'), { code, reason })), ms)
      })
    }
  }
  return { restore: () => void (globalThis.WebSocket = Counted) }
}

// The staged WebSocket holds no session, so the server's end closes it as a loss, which races the barrier POST's answer
// to the page. Each order is made here, and each takes only the requests its carrier needs, beside the page's PINGs,
// its heartbeat, which go in either.
test.each([
  ["the POST's answer reaches the page first", 'answer', ['post']],
  ["the WebSocket's loss reaches the page first, and the POST's answer never", 'loss', ['post', 'open']],
])(
  "the barrier POST of a move to a WebSocket the server refuses `frames` naming no channel ends every channel on the page with the server's sentence of the connection: %s",
  async (_order, first, requests) => {
    let requestsBefore = -1
    // Each batch POST after the barrier's that carries PINGs alone, by where it went among the requests.
    const pingsAt: number[] = []
    const pings: (typeof world.hooks)[number] = {
      kind: 'post',
      body: (bytes) => {
        const tags = Array.from(framesOf(bytes), ({ frame }) => frame[0])
        if (requestsBefore >= 0 && tags.every((tag) => tag === TAG.PING)) pingsAt.push(world.started.length)
        return [bytes]
      },
    }
    const decides = answerDecides()
    const late = first === 'answer' ? closingFirstSocketLate(1_000) : null
    world.hooks = [
      { kind: 'open', stream: decides.stream },
      {
        kind: 'post',
        carries: TAG.BARRIER,
        body: (bytes) => {
          requestsBefore = world.started.length
          return [bytes.subarray(0, framesStart(bytes)), ...unframeable(), bytes.subarray(framesStart(bytes))]
        },
        // Where the loss comes first, the reopen it makes reads the end the server kept.
        ...(first === 'loss' ? { held: true } : {}),
      },
      ...Array.from({ length: 20 }, () => pings),
    ]
    try {
      const [one, two] = opening(crypto.randomUUID(), ['sse', 'ws'])
      const ends = [one, two].map(({ page }) => endsOf(page))
      await until(() => ends.every((each) => each.length > 0))
      // Past the late close, so that no request it could make goes unseen.
      await delay(1_200)
      const sent = world.started.flatMap((kind, at) => (at < requestsBefore || pingsAt.includes(at) ? [] : [kind]))
      expect([sent, ends, decides.withheld.includes(TAG.END)]).toEqual([
        requests,
        [sessionEndOnPage, sessionEndOnPage],
        true,
      ])
    } finally {
      late?.restore()
    }
  },
)

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

/** The page's WebSocket sends no acknowledgement and no PING, so what the server sends it waits in the server's piece
 *  sender; and puts `bad` after the frame of `text`. */
const unacknowledged =
  (text: string, bad: (found: { ix: number; seq: number }) => Uint8Array<ArrayBuffer>) =>
  (message: Uint8Array<ArrayBuffer>) =>
    (isOneFrame(message) ? [message] : [...messageFrames(message)]).flatMap((frame) => {
      if (frame[0] === TAG.PIECES_ACK || frame[0] === TAG.PING) return []
      const found = textOf(frame, text)
      return found ? [frame, bad(found)] : [frame]
    })

test("a session the server ends on a WebSocket while a channel's data waits unacknowledged tells the page its end, on the first socket", async () => {
  const live = pair(crypto.randomUUID(), ['ws'])
  await live.page.opened
  const pageEnds = endsOf(live.page)
  live.channel.listen(() => void live.channel.send('x'.repeat(256 * 1024)).catch(() => {}))
  world.wsSends = unacknowledged('a1', () => encode.attachResult(999, null))
  sends(live.page, 'a1')
  await until(() => pageEnds.length > 0)
  await delay(300)
  expect([pageEnds, world.sockets]).toEqual([sessionEndOnPage, 1])
})

test("a channel the server ends on a WebSocket while its data waits unacknowledged reads its cause; the other's text arrives once, on the next socket", async () => {
  const [refused, other] = opening(crypto.randomUUID(), ['ws'])
  await Promise.all([refused.page.opened, other.page.opened])
  const [refusedEnds, otherEnds] = [endsOf(refused.page), endsOf(other.page)]
  refused.channel.listen(() => void refused.channel.send('x'.repeat(256 * 1024)).catch(() => {}))
  world.wsSends = unacknowledged('a1', (found) => encode.error(found.ix, ERROR_REASON.UNCONFIRMED, found.seq + 1))
  sends(refused.page, 'a1')
  await until(() => refusedEnds.length > 0)
  world.wsSends = null
  sends(other.page, 'b1')
  await until(() => other.got.length > 0)
  await delay(300)
  expect([refusedEnds, otherEnds, other.got]).toEqual([[['ChannelClosedError', told.refused]], [], ['b1']])
})

test('a session the server ends on a WebSocket ends a channel of it still awaiting registration, on both ends: it never attaches', async () => {
  const key = crypto.randomUUID()
  const live = pair(key, ['ws'])
  const awaited = new ServerChannel<string, string>()
  const page = new ClientChannel<string, string>({
    channelId: awaited.id,
    connection: { telefuncUrl: world.telefuncUrl, transports: ['ws'], idleTimeout: 0, connectionKey: key },
  })
  void page.opened.catch(() => {})
  const [pageEnds, serverEnds] = [endsOf(page), endsOf(awaited)]
  await live.page.opened
  world.wsSends = (message) => (textIn(message, 'a1') ? [message, encode.attachResult(999, null)] : [message])
  sends(live.page, 'a1')
  await until(() => live.ended.length > 0)
  await delay(300)
  getChannelMux().registerChannel(awaited)
  await until(() => serverEnds.length > 0)
  await delay(300)
  expect([pageEnds, serverEnds, world.sockets]).toEqual([sessionEndOnPage, sessionEndOnPage, 1])
})

test("a page gone with its streaming upload open after the server ended its session: the server's read of that upload ends with the connection, and no answer is written to it", async () => {
  world.uploads = true
  const vanishing = vanishingAfter('a1', serverOnly(999))
  world.hooks = [{ kind: 'upload', upload: vanishing.upload }]
  const live = pair(crypto.randomUUID())
  await live.page.opened
  await until(() => uploads() === 1)
  const pageEnds = endsOf(live.page)
  sends(live.page, 'a1')
  await until(() => pageEnds.length > 0)
  await delay(300)
  vanishing.vanish()
  await until(() => world.closedStreaming.length > 0).catch(() => {})
  await delay(300)
  expect([pageEnds, world.pendingBodyReads, world.closedStreaming]).toEqual([sessionEndOnPage, 0, [null]])
})
