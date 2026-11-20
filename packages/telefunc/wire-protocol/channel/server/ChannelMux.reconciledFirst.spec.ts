// A page that reconnects reads its RECONCILED first, then what the server replays of each channel, then what the app
// sent on the channel after the attach, in seq order: on both of the server's SSE paths, the Web stream a Web adapter
// answers with and the Node `Readable` the Node adapter pipes to its socket. The real SSE handler and ChannelMux run;
// the page is the test, reading the event stream's frames.

import { expect, test, vi } from 'vitest'
import { Readable } from 'node:stream'

import { WIRE_VERSION } from '../../constants.js'
import { loadStreamNodeModuleOnce } from '../../../utils/loadStreamNodeModule.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isSequencedFrame } from '../../wire/frame.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

type ServePath = 'web' | 'node'

/** A page's event stream: what its frames carried so far, read as the page reads them. */
async function openStream(path: ServePath, reconcile: Uint8Array<ArrayBuffer>) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const body = encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcile])
  const opened =
    path === 'web'
      ? await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
      : await handleSseChannelRequest(
          new Request(url, { method: 'POST' }),
          Readable.from([new Uint8Array(await body.arrayBuffer())]),
        )
  const stream = opened.body
  if (typeof stream === 'string') throw new Error(`no event stream: ${opened.statusCode} ${stream}`)
  const reader = (stream instanceof ReadableStream ? stream : webStreamOf(stream)).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** Reads until a frame `until` accepts has come, and returns every frame so far. */
    async readUntil(until: (frame: Uint8Array) => boolean): Promise<Uint8Array[]> {
      while (!frames.some(until)) {
        const read = await reader.read()
        if (read.done) break
        events.push(read.value, (frame) => frames.push(frame))
      }
      return frames
    },
    /** The page's connection drops: the server's wire closes transiently. */
    drop: () => (stream instanceof ReadableStream ? reader.cancel() : stream.destroy()),
  }
}

/** The Node adapter's body as a stream the page reads. */
function webStreamOf(readable: Readable): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      readable.on('data', (chunk: Uint8Array) => controller.enqueue(chunk))
      readable.on('end', () => controller.close())
    },
  })
}

/** The RECONCILED and each channel frame, as `RECONCILED` or `<seq>:<message>`; a lossy send's head names the seq its
 *  message has. */
function order(frames: Uint8Array[]): string[] {
  return frames.flatMap((frame) => {
    const decoded = decode(frame)
    if (decoded.tag === TAG.RECONCILED) return ['RECONCILED']
    if (!isSequencedFrame(decoded) || decoded.tag === TAG.PUBLISH_TO) return []
    return [`${decoded.seq}:${decoded.tag === TAG.TEXT ? JSON.parse(decoded.text) : `0x${decoded.tag.toString(16)}`}`]
  })
}

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
