// A session the server ended, whose last word never reached the page, or reached it as what it can't read: on the real
// Node server and the real page. On a WebSocket the page's first close comes as 1006 with no reason, as where the
// connection dies before the close frame, or as 4002 with another reason; on SSE the event stream's last event (or a
// channel's ERROR, where one carried it) and the batch POST's answer never reach the page, and the stream is cut, or the
// last event carries another reason. The page then reopens, naming its session, and the server answers that reopen with
// the session's end.

import { expect, test } from 'vitest'
import { createDeferred } from '../../../utils/createDeferred.js'
import { encodeSseRequest, encodeU32 } from '../../wire/envelope.js'
import { encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { REFUSAL_MEDIA_TYPE } from '../../refusal.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER } from '../../wire/requestKind.js'
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
  rewriting,
  sends,
  textOf,
  until,
  useRealServer,
  world,
} from './realServer.fixture.js'

useRealServer(['sse', 'ws'])

const sessionEnd = [
  [
    'ChannelClosedError',
    "Channel closed: the page's connection ended: The server can't read what the page sent on its connection.",
  ],
]

function endsOf(channel: ServerChannel<string, string> | ClientChannel<string, string>): [string, string][] {
  const ends: [string, string][] = []
  channel.onClose((err) => void ends.push(err === null ? ['clean', ''] : [err.name, err.message]))
  return ends
}

/** A channel the page opens on `transport` whose server end isn't registered yet: the server awaits it. */
function awaitedOn(connectionKey: string, transport: 'sse' | 'ws') {
  const channel = new ServerChannel<string, string>()
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: world.telefuncUrl, transports: [transport], idleTimeout: 0, connectionKey },
  })
  void page.opened.catch(() => {})
  return { channel, pageEnds: endsOf(page), serverEnds: endsOf(channel) }
}

/** Whether a message the server sent carries a channel's ERROR. */
function carriesError(data: unknown): boolean {
  if (!(data instanceof ArrayBuffer)) return false
  const message = new Uint8Array(data)
  const frames = isOneFrame(message) ? [message] : [...messageFrames(message)]
  return frames.some((frame) => frame[0] === TAG.ERROR)
}

/** A close as the page reads it in place of its socket's: `told` with 4002, or none (null) with 1006, as where the
 *  connection dies before the close frame. Node 22 has no `CloseEvent`: an `Event` with its fields. */
function closeOf(told: string | null) {
  const fields =
    told === null ? { code: 1006, reason: '', wasClean: false } : { code: 4002, reason: told, wasClean: true }
  return Object.assign(new Event('close'), fields)
}

/** The page's WebSockets as `script` says, by the order the page makes them, from 0: whether a message the server sent
 *  reaches the page, and the close the page reads in place of the socket's, where one. `sockets` holds them. */
function scriptedSockets(script: {
  heard: (n: number, data: unknown) => boolean
  closed: (n: number) => ReturnType<typeof closeOf> | undefined
}) {
  const Counted = globalThis.WebSocket
  const sockets: WebSocket[] = []
  globalThis.WebSocket = class Scripted extends Counted {
    private readonly n = sockets.push(this) - 1
    override set onmessage(handler: ((event: MessageEvent) => void) | null) {
      super.onmessage = (event: MessageEvent) => {
        if (script.heard(this.n, event.data)) handler?.(event)
      }
    }
    override get onmessage() {
      return super.onmessage
    }
    override set onclose(handler: ((event: CloseEvent) => void) | null) {
      super.onclose = (event: CloseEvent) => handler?.(script.closed(this.n) ?? event)
    }
    override get onclose() {
      return super.onclose
    }
  }
  return { sockets, restore: () => void (globalThis.WebSocket = Counted) }
}

/** The page's WebSockets, whose first close reaches the page as `closeOf(told)`, and whose first socket's messages
 *  carrying a channel's ERROR (where one carried the end) never reach it. */
function losingFirstClose(told: string | null): () => void {
  return scriptedSockets({
    heard: (n, data) => n > 0 || !carriesError(data),
    closed: (n) => (n === 0 ? closeOf(told) : undefined),
  }).restore
}

const transports: ('ws' | 'sse')[] = ['ws', 'sse']

/** Ends the session over `transport` with a frame of an index it holds in no form, after the text `a1`, and keeps the
 *  end's last word from the page, or hands it `told` in its place; resolves once the server's channel ended, with how
 *  many wires the page opened. */
async function endLosingLastWord(transport: 'sse' | 'ws', awaited: boolean, told: string | null = null) {
  const key = crypto.randomUUID()
  const restore = transport === 'ws' ? losingFirstClose(told) : () => {}
  const lastWord = told === null ? null : encode.end(told)
  const cutter = rewriting((frame) => {
    if (frame[0] === TAG.ERROR) return null
    return frame[0] === TAG.END ? lastWord : frame
  })
  if (transport === 'sse') {
    world.hooks = [
      { kind: 'open', stream: cutter.stream },
      {
        kind: 'post',
        text: 'a1',
        held: true,
        body: (bytes) => {
          const end = findText(bytes, 'a1')?.end ?? bytes.length
          const bad = encode.attachResult(999, null)
          return [bytes.subarray(0, end), encodeU32(bad.byteLength), bad, bytes.subarray(end)]
        },
      },
    ]
  }
  world.wsSends = (message) => {
    const frames = isOneFrame(message) ? [message] : [...messageFrames(message)]
    return frames.some((frame) => textOf(frame, 'a1')) ? [message, encode.attachResult(999, null)] : [message]
  }
  try {
    const waiting = awaited ? awaitedOn(key, transport) : null
    const live = pair(key, [transport])
    await live.page.opened
    const serverEnds = endsOf(live.channel)
    sends(live.page, 'a1')
    await until(() => serverEnds.length > 0)
    // A last word handed in place of the server's ends its stream as the server's did.
    if (told === null) cutter.cut()
    // Within the test's own limit, so a page that reads nothing is seen as such.
    await until(() => live.ended.length > 0 && (waiting === null || waiting.pageEnds.length > 0), 3_000).catch(() => {})
    await delay(300)
    const wires = transport === 'ws' ? world.sockets : opens()
    return { live, serverEnds, waiting, wires }
  } finally {
    restore()
  }
}

test.each(transports)(
  "a session the server ended whose last word the page missed, on %s: the page's reopen is answered with the session's end, so every channel reads its cause, as the server's did",
  async (transport) => {
    const { live, serverEnds, wires } = await endLosingLastWord(transport, false)
    const pageEnds = live.ended.map(({ name, message }) => [name, message])
    expect([pageEnds, serverEnds, wires]).toEqual([sessionEnd, sessionEnd, 2])
  },
)

test.each(transports)(
  'a session the server ended whose last word the page missed, on %s: a channel the server still awaited reads its cause on the page at the reopen, and on the server as it registers, and never attaches',
  async (transport) => {
    const { waiting, wires } = await endLosingLastWord(transport, true)
    if (waiting === null) throw new Error('no awaited channel')
    getChannelMux().registerChannel(waiting.channel)
    await until(() => waiting.serverEnds.length > 0, 500).catch(() => {})
    expect([waiting.pageEnds, waiting.serverEnds, wires]).toEqual([sessionEnd, sessionEnd, 2])
  },
)

const media = REFUSAL_MEDIA_TYPE
/** What the page can't read as the end the server decided: the session's end (`kind=frames; cause=8`) cut short at each
 *  of its parts, a refusal of each other kind, a cause no end has, and a channel that is no index. */
const unread = [
  media.slice(0, -3),
  media,
  `${media}; kind=fra`,
  `${media}; kind=frames`,
  `${media}; kind=frames; cause=`,
  `${media}; kind=retry`,
  `${media}; kind=route; cause=8`,
  `${media}; kind=reload; cause=8`,
  `${media}; kind=frames; cause=99`,
  `${media}; kind=frames; channel=x; cause=8`,
  `${media}; kind=frames; channel=-1; cause=8`,
  `${media}; kind=frames; channel=01; cause=8`,
  `${media}; kind=frames; channel=65536; cause=8`,
]
const unreadRows = transports.flatMap((transport) => unread.map((told): ['sse' | 'ws', string] => [transport, told]))

test.each(unreadRows)(
  "a session the server ended whose last word reaches the page on %s as `%s`: the page reads it as a loss, and its reopen is answered with the session's end",
  async (transport, told) => {
    const { live, serverEnds, wires } = await endLosingLastWord(transport, false, told)
    const pageEnds = live.ended.map(({ name, message }) => [name, message])
    expect([pageEnds, serverEnds, wires]).toEqual([sessionEnd, sessionEnd, 2])
  },
)

/** Whether a message the page sends carries its RECONCILE. */
function carriesReconcile(message: Uint8Array<ArrayBuffer>): boolean {
  const frames = isOneFrame(message) ? [message] : [...messageFrames(message)]
  return frames.some((frame) => frame[0] === TAG.RECONCILE)
}

test.each(transports)(
  "a session the server ended on a reopen whose RECONCILED the page never read, on %s, and whose last word the page missed too: the page reopens naming the session it had, and that reopen is answered with the session's end",
  async (transport) => {
    const key = crypto.randomUUID()
    const unattached = encode.attachResult(999, null)
    // The first wire is lost; its reopen reads nothing, and ends with the session.
    const scripted = scriptedSockets({ heard: (n) => n !== 1, closed: (n) => (n < 2 ? closeOf(null) : undefined) })
    world.wsSends = (message) =>
      scripted.sockets.length === 2 && carriesReconcile(message) ? [message, unattached] : [message]
    const first = rewriting((frame) => frame)
    const reopen = rewriting(() => null)
    // The reopen's URL and its wire's `connId`, for a POST on that wire the page doesn't send.
    const reopened = createDeferred<{ at: string; connId: string }>()
    // The wrapper reads a request's body before its URL.
    let connId = ''
    world.hooks = [
      { kind: 'open', stream: first.stream },
      {
        kind: 'open',
        stream: reopen.stream,
        body: (bytes) => {
          const metadata: unknown = JSON.parse(new TextDecoder().decode(bytes.subarray(4, framesStart(bytes))))
          if (typeof metadata !== 'object' || metadata === null || !('connId' in metadata)) throw new Error('no connId')
          connId = String(metadata.connId)
          return [bytes]
        },
        url: (at) => {
          reopened.resolve({ at, connId })
          return at
        },
      },
    ]
    try {
      const live = pair(key, [transport])
      await live.page.opened
      const serverEnds = endsOf(live.channel)
      if (transport === 'ws') scripted.sockets[0]?.close()
      else first.cut()
      if (transport === 'sse') {
        const { at, connId: reopenedOn } = await reopened.promise
        await fetch(at, {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream', [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE },
          body: new Blob([encodeSseRequest({ connId: reopenedOn }, [unattached])]),
        })
      }
      await until(() => serverEnds.length > 0)
      if (transport === 'sse') reopen.cut()
      await until(() => live.ended.length > 0, 3_000).catch(() => {})
      await delay(300)
      const pageEnds = live.ended.map(({ name, message }) => [name, message])
      const wires = transport === 'ws' ? world.sockets : opens()
      expect([pageEnds, serverEnds, wires]).toEqual([sessionEnd, sessionEnd, 3])
    } finally {
      scripted.restore()
    }
  },
)
