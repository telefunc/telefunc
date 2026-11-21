// Which wire holds a page's session, on the real mux: a session keeps one id for its life and names the one wire that
// holds it; a wire that holds nothing goes only as a loss; a reopen or an upgrade's commit hands the session over. Each
// wire reads the session id from the RECONCILED it was sent.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux, type ServerEnd, type ServerTransport } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'

const KEEP = 1_000
config.channel = { reconnectTimeout: KEEP, connectTtl: 15_000 }
const bad = encode.attachResult(999, null)
const unreadable = Uint8Array.from([0xff, 0xff, 0xff])

/** A WebSocket wire the test holds: each frame the mux sent it, and each end it gave it. */
function wire() {
  const mux = getChannelMux()
  const w = {}
  const frames: Uint8Array[] = []
  const ends: ServerEnd[] = []
  const transport: ServerTransport<typeof w> = {
    getConnId: () => null,
    sendNow: (_, message) => {
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) frames.push(frame)
    },
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => 0,
    close: (x, end) => {
      ends.push(end)
      mux.onWireClosed(x, end.how)
    },
  }
  mux.onWireOpen(w, transport)
  const tags = () => frames.map((frame) => frame[0])
  return {
    w,
    frames,
    ends,
    tags,
    hows: () => ends.map(({ how }) => how),
    /** The id the last RECONCILED gave this wire. */
    sessionId: () => {
      const reconciled = frames
        .filter((frame) => frame[0] === TAG.RECONCILED)
        .map(decode)
        .at(-1)
      return reconciled?.tag === TAG.RECONCILED ? reconciled.payload.sessionId : undefined
    },
    send: (frame: Uint8Array<ArrayBuffer>) => mux.onWireMessage(w, frame),
    lose: () => mux.onWireClosed(w, 'transient'),
    leave: () => mux.onWireClosed(w, 'pageLeft'),
  }
}

function channel() {
  const c = new ServerChannel<string, string>()
  getChannelMux().registerChannel(c)
  const ends: string[] = []
  c.onClose((err) => void ends.push(err ? err.message : 'clean'))
  return { c, ends }
}

const entry = (id: string, ix: number): ReconcileOpenEntry => ({ id, ix, lastSeq: 0, initial: true })
/** A RECONCILE carrying the page's attempt number. */
const reconcile = (payload: Parameters<typeof encode.reconcile>[0], attempt: number) =>
  encode.reconcile(Object.assign({ attempt }, payload))
/** A PREPARE carrying its probe's attempt number, which the stage keeps: the BARRIER carries none. */
const prepare = (payload: Parameters<typeof encode.prepare>[0], attempt: number) =>
  encode.prepare(Object.assign({ attempt }, payload))

test('a reopen naming a session the old wire still holds keeps its id, and the old wire then holds nothing', async () => {
  const a = channel()
  const first = wire()
  const second = wire()
  try {
    await first.send(reconcile({ open: [entry(a.c.id, 0)] }, 1))
    const sessionId = first.sessionId()
    await second.send(reconcile({ sessionId, open: [entry(a.c.id, 0)] }, 2))
    await first.send(bad)
    expect({ id: second.sessionId() === sessionId, firstEnds: first.hows(), aEnds: [...a.ends] }).toEqual({
      id: true,
      firstEnds: ['transient'],
      aEnds: [],
    })
  } finally {
    first.leave()
    second.leave()
  }
})

test('an unreadable first message on a wire that holds nothing ends it as a loss', async () => {
  const first = wire()
  try {
    await first.send(unreadable)
    expect(first.ends).toEqual([{ how: 'transient', refusal: null }])
  } finally {
    first.leave()
  }
})

test('a staged probe whose stage a newer PREPARE replaces closes as a loss, and the newer one commits', async () => {
  const a = channel()
  const source = wire()
  const target = wire()
  const replacement = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await target.send(prepare({ sessionId, upgradeId: 'probe' }, 2))
    await replacement.send(prepare({ sessionId, upgradeId: 'replacement' }, 3))
    await source.send(encode.barrier({ sessionId, upgradeId: 'replacement', open }))
    expect({
      targetEnds: target.hows(),
      replacementEnds: replacement.hows(),
      replacementId: replacement.sessionId() === sessionId,
      aEnds: [...a.ends],
    }).toEqual({ targetEnds: ['transient'], replacementEnds: [], replacementId: true, aEnds: [] })
  } finally {
    replacement.leave()
    source.leave()
    target.leave()
  }
})

test("a fault on the barrier's old wire after its commit is a loss, and the session goes on on the new one", async () => {
  const a = channel()
  const source = wire()
  const target = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await target.send(prepare({ sessionId, upgradeId: 'up' }, 2))
    await source.send(encode.barrier({ sessionId, upgradeId: 'up', open }))
    await source.send(bad)
    expect({
      sourceEnds: source.hows(),
      targetEnds: [...target.ends],
      aEnds: [...a.ends],
      id: target.sessionId(),
    }).toEqual({
      sourceEnds: ['transient'],
      targetEnds: [],
      aEnds: [],
      id: sessionId,
    })
  } finally {
    source.leave()
    target.leave()
  }
})

test('a fault on the wire a reopen replaced ends nothing, and the next reopen reconciles', async () => {
  const a = channel()
  const first = wire()
  const moved = wire()
  const retry = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await first.send(reconcile({ open }, 1))
    const sessionId = first.sessionId()
    await moved.send(reconcile({ sessionId, open }, 2))
    // The old wire was still open at the server when the page reopened; the reply to the reopen never reached it.
    await first.send(bad)
    await retry.send(reconcile({ sessionId, open }, 3))
    expect({ retryEnds: [...retry.ends], reconciled: retry.sessionId() === sessionId, aEnds: [...a.ends] }).toEqual({
      retryEnds: [],
      reconciled: true,
      aEnds: [],
    })
  } finally {
    first.leave()
    moved.leave()
    retry.leave()
  }
})

test('a RECONCILE whose wire was lost before its turn attaches nothing and leaves no registration wait', async () => {
  const pending = new ServerChannel<string, string>()
  const first = wire()
  const next = wire()
  try {
    // The tail is unframeable: it ends the wire before the RECONCILE's turn, and the wire holds nothing yet.
    const message = new Uint8Array([
      ...encode.reconcile(Object.assign({ attempt: 1 }, { open: [entry(pending.id, 0)] })),
      0xff,
    ])
    await first.send(message)
    await next.send(reconcile({ open: [entry(pending.id, 0)] }, 2))
    getChannelMux().registerChannel(pending)
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect({
      firstEnds: first.hows(),
      onFirst: first.tags().includes(TAG.ATTACH_RESULT),
      onNext: next.tags().includes(TAG.ATTACH_RESULT),
    }).toEqual({ firstEnds: ['transient'], onFirst: false, onNext: true })
  } finally {
    first.leave()
    next.leave()
    pending.abort()
  }
})
