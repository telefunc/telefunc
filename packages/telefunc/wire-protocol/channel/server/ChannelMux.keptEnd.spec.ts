// What a session's kept end gives a page that missed its last word, on the real mux: every channel whose own end the
// page has not read ends with the session's cause, and the server's channel keeps its own. The kept end is a cause
// alone. The page names its session from its first wire on, and numbers each wire it opens.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { decode, encode } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { openWebSocket } from './serverPage.fixture.js'

config.channel = { reconnectTimeout: 1_000, connectTtl: 15_000 }

/** What the channel at `ix` was sent on the wire, in order: its texts, and its terminal word by name. */
const channelWords = (frames: Uint8Array[], ix: number) =>
  frames.flatMap((frame) => {
    const decoded = decode(frame)
    if (!('index' in decoded) || decoded.index !== ix) return []
    if (decoded.tag === TAG.TEXT) return [JSON.parse(decoded.text)]
    if (decoded.tag === TAG.ABORT) return ['ABORT']
    if (decoded.tag === TAG.ERROR) return ['ERROR']
    if (decoded.tag === TAG.CLOSE_DONE) return ['CLOSE_DONE']
    return []
  })

function channel() {
  const c = new ServerChannel<string, string>()
  getChannelMux().registerChannel(c)
  const ends: string[] = []
  c.onClose((err) => void ends.push(err ? err.name : 'clean'))
  return { c, ends }
}

const entry = (id: string, ix: number): ReconcileOpenEntry => ({ id, ix, lastSeq: 0, initial: true })
const settle = () => new Promise((resolve) => setTimeout(resolve, 20))

test("a channel aborted before its session ended, whose abort the page never read, reads the session's end at the reopen, as the other does; the server's channel keeps its abort", async () => {
  const a = channel()
  const b = channel()
  const first = openWebSocket()
  const second = openWebSocket()
  const open = [entry(a.c.id, 0), entry(b.c.id, 1)]
  try {
    const sessionId = crypto.randomUUID()
    await first.mux.onWireMessage(first.wire, encode.reconcile({ sessionId, attempt: 1, open }))
    void a.c.send('m1').catch(() => {})
    void a.c.send('m2').catch(() => {})
    a.c.abort('the reason')
    // Every frame on the first wire is lost with it: the page read none of them.
    first.mux.endFault(first.wire, { scope: 'session', cause: 'receiveBudget' })
    await second.mux.onWireMessage(second.wire, encode.reconcile({ sessionId, attempt: 2, open }))
    await settle()
    expect({
      a: channelWords(second.frames, 0),
      b: channelWords(second.frames, 1),
      secondEnds: second.state.closedAs,
      serverA: [...a.ends],
      serverB: [...b.ends],
    }).toEqual({ a: [], b: [], secondEnds: 'receiveBudget', serverA: ['Abort'], serverB: ['ChannelOverflowError'] })
  } finally {
    first.close()
    second.close()
  }
})

test("a guard: a channel whose close the page hadn't answered when its session ended reads the session's end", async () => {
  const a = channel()
  const first = openWebSocket()
  const second = openWebSocket()
  const open = [entry(a.c.id, 0)]
  try {
    const sessionId = crypto.randomUUID()
    await first.mux.onWireMessage(first.wire, encode.reconcile({ sessionId, attempt: 1, open }))
    void a.c.close({ timeout: 10_000 })
    first.mux.endFault(first.wire, { scope: 'session', cause: 'receiveBudget' })
    await second.mux.onWireMessage(second.wire, encode.reconcile({ sessionId, attempt: 2, open }))
    await settle()
    expect({ a: channelWords(second.frames, 0), secondEnds: second.state.closedAs, serverA: [...a.ends] }).toEqual({
      a: [],
      secondEnds: 'receiveBudget',
      serverA: ['ChannelOverflowError'],
    })
  } finally {
    first.close()
    second.close()
  }
})
