// A session the server ended, whose last word never reached the page: on the real Node server and the real page. On a
// WebSocket the page's first close comes as 1006 with no reason, as where the connection dies before the close frame;
// on SSE the event stream's last event (or a channel's ERROR, where one carried it) and the batch POST's answer never
// reach the page, and the stream is cut. The page then reopens, naming its session, and the server answers that reopen
// with the session's end.

import { expect, test } from 'vitest'
import { encodeU32 } from '../../wire/envelope.js'
import { encode, isOneFrame, messageFrames } from '../../wire/frame.js'
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
  useRealServer,
  withholding,
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

/** The page's WebSockets, whose first close reaches the page as 1006 with no reason, and whose first socket's messages
 *  carrying a channel's ERROR (where one carried the end) never reach it. */
function losingFirstClose(): () => void {
  const Counted = globalThis.WebSocket
  let lost = false
  globalThis.WebSocket = class LostClose extends Counted {
    override set onmessage(handler: ((event: MessageEvent) => void) | null) {
      super.onmessage = (event: MessageEvent) => {
        if (!lost && carriesError(event.data)) return
        handler?.(event)
      }
    }
    override get onmessage() {
      return super.onmessage
    }
    override set onclose(handler: ((event: CloseEvent) => void) | null) {
      super.onclose = (event: CloseEvent) => {
        const first = !lost
        lost = true
        // Node 22 has no `CloseEvent`: an `Event` with its fields.
        handler?.(first ? Object.assign(new Event('close'), { code: 1006, reason: '', wasClean: false }) : event)
      }
    }
    override get onclose() {
      return super.onclose
    }
  }
  return () => void (globalThis.WebSocket = Counted)
}

const transports: ('ws' | 'sse')[] = ['ws', 'sse']

/** Ends the session over `transport` with a frame of an index it holds in no form, after the text `a1`, and keeps the
 *  end's last word from the page; resolves once the server's channel ended, with how many wires the page opened. */
async function endLosingLastWord(transport: 'sse' | 'ws', awaited: boolean) {
  const key = crypto.randomUUID()
  const restore = transport === 'ws' ? losingFirstClose() : () => {}
  const cutter = withholding((frame) => frame[0] === TAG.END || frame[0] === TAG.ERROR)
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
    cutter.cut()
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
