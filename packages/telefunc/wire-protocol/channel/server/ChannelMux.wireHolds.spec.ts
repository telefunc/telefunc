// Which wire holds a page's session, on the real mux: a session keeps the id the page made for its life and names the
// one wire that holds it; a wire that holds nothing goes only as a loss; a reopen or an upgrade's commit hands the session over. Each
// wire reads the session id from the RECONCILED it was sent.

import { expect, test, vi } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import type { ReconcileOpenEntry, ReconcilePayload } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'
import { getChannelMux, type ServerEnd, type ServerTransport } from './ChannelMux.js'
import { RegistrationWaits } from './RegistrationWaits.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'
import { upgradesTakenHere } from '../../../node/server/runTelefunc/takesUpgrades.js'

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
    close: (_, end) => void ends.push(end),
  }
  mux.onWireOpen(w, transport, upgradesTakenHere)
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
  getChannelMux().registerChannel(c, ATTACHABLE_NOW)
  const ends: string[] = []
  c.onClose((err) => void ends.push(err ? err.message : 'clean'))
  return { c, ends }
}

const entry = (id: string, ix: number): ReconcileOpenEntry => ({ id, ix, lastSeq: 0, initial: true })
/** A RECONCILE carrying the page's attempt number; one that names no session names a new one, as a page's first does. */
const reconcile = (payload: Omit<ReconcilePayload, 'sessionId'> & { sessionId?: string }, attempt: number) =>
  encode.reconcile({ sessionId: crypto.randomUUID(), ...payload, attempt })
/** A PREPARE carrying its probe's attempt number, which the stage keeps: the BARRIER carries none. */
const prepare = (payload: { sessionId: string; upgradeId: string }, attempt: number) =>
  encode.prepare({ ...payload, attempt })

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
      ...encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [entry(pending.id, 0)] }),
      0xff,
    ])
    await first.send(message)
    await next.send(reconcile({ open: [entry(pending.id, 0)] }, 2))
    getChannelMux().registerChannel(pending, ATTACHABLE_NOW)
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

test('a PREPARE older than the stage it meets closes its own probe as a loss and leaves the stage, which commits', async () => {
  const a = channel()
  const source = wire()
  const newer = wire()
  const older = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await newer.send(prepare({ sessionId, upgradeId: 'newer' }, 3))
    await older.send(prepare({ sessionId, upgradeId: 'older' }, 2))
    await source.send(encode.barrier({ sessionId, upgradeId: 'newer', open }))
    expect({
      olderEnds: older.hows(),
      newerEnds: newer.hows(),
      newerId: newer.sessionId() === sessionId,
      aEnds: [...a.ends],
    }).toEqual({ olderEnds: ['transient'], newerEnds: [], newerId: true, aEnds: [] })
  } finally {
    older.leave()
    newer.leave()
    source.leave()
  }
})

test('a BARRIER naming another upgrade is refused silently and leaves the stage; the matching BARRIER still commits', async () => {
  const a = channel()
  const source = wire()
  const target = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await target.send(prepare({ sessionId, upgradeId: 'prepared' }, 2))
    await source.send(encode.barrier({ sessionId, upgradeId: 'different', open }))
    const afterMismatch = { sourceEnds: source.hows(), targetEnds: target.hows() }
    await source.send(encode.barrier({ sessionId, upgradeId: 'prepared', open }))
    expect({
      afterMismatch,
      targetId: target.sessionId() === sessionId,
      targetEnds: target.hows(),
      aEnds: [...a.ends],
    }).toEqual({ afterMismatch: { sourceEnds: [], targetEnds: [] }, targetId: true, targetEnds: [], aEnds: [] })
  } finally {
    source.leave()
    target.leave()
  }
})

test("an older attempt's reopen read after the newer one's takes nothing, ends nothing, and goes as a loss", async () => {
  const a = channel()
  const b = channel()
  const first = wire()
  try {
    await first.send(reconcile({ open: [entry(a.c.id, 0)] }, 1))
    const sessionId = first.sessionId()
    first.lose()
    const older = wire()
    const newer = wire()
    try {
      await newer.send(reconcile({ sessionId, open: [entry(a.c.id, 0)] }, 3))
      await newer.send(reconcile({ sessionId: newer.sessionId(), open: [entry(b.c.id, 1)], released: [] }, 3))
      const newerBefore = newer.frames.length
      await older.send(reconcile({ sessionId, open: [entry(a.c.id, 0)] }, 2))
      void a.c.send('after the late reopen').catch(() => {})
      await new Promise((resolve) => setTimeout(resolve, 20))
      const text = (frames: Uint8Array[]) => frames.some((frame) => frame[0] === TAG.TEXT)
      expect({
        bEnds: [...b.ends],
        aEnds: [...a.ends],
        olderEnds: older.hows(),
        olderReconciled: older.tags().includes(TAG.RECONCILED),
        aOnNewer: text(newer.frames.slice(newerBefore)),
        aOnOlder: text(older.frames),
      }).toEqual({
        bEnds: [],
        aEnds: [],
        olderEnds: ['transient'],
        olderReconciled: false,
        aOnNewer: true,
        aOnOlder: false,
      })
    } finally {
      older.leave()
      newer.leave()
    }
  } finally {
    first.leave()
  }
})

test("a stale BARRIER from a wire a newer reopen took the session from goes as a loss with its own stage, and the newer wire's stage commits", async () => {
  const a = channel()
  const source = wire()
  const oldProbe = wire()
  const newer = wire()
  const newProbe = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await oldProbe.send(prepare({ sessionId, upgradeId: 'old' }, 2))
    await newer.send(reconcile({ sessionId, open }, 3))
    await newProbe.send(prepare({ sessionId, upgradeId: 'new' }, 4))
    await source.send(encode.barrier({ sessionId, upgradeId: 'old', open }))
    const afterStale = { sourceEnds: source.hows(), oldProbeEnds: oldProbe.hows(), newProbeEnds: newProbe.hows() }
    await newer.send(encode.barrier({ sessionId, upgradeId: 'new', open }))
    expect({ afterStale, committed: newProbe.sessionId() === sessionId, aEnds: [...a.ends] }).toEqual({
      afterStale: { sourceEnds: ['transient'], oldProbeEnds: ['transient'], newProbeEnds: [] },
      committed: true,
      aEnds: [],
    })
  } finally {
    source.leave()
    oldProbe.leave()
    newer.leave()
    newProbe.leave()
  }
})

test('a reopen whose RECONCILED never reached the page is taken over by the next attempt, which gets what the channel sent meanwhile', async () => {
  const a = channel()
  const first = wire()
  const second = wire()
  const third = wire()
  try {
    await first.send(reconcile({ open: [entry(a.c.id, 0)] }, 1))
    const sessionId = first.sessionId()
    first.lose()
    // The page never reads this wire's RECONCILED, so it names its channel from seq 0 again on the next one.
    await second.send(reconcile({ sessionId, open: [entry(a.c.id, 0)] }, 2))
    void a.c.send('sent while the page waited').catch(() => {})
    await third.send(reconcile({ sessionId, open: [entry(a.c.id, 0)] }, 3))
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect({
      thirdId: third.sessionId() === sessionId,
      thirdText: third.tags().includes(TAG.TEXT),
      aEnds: [...a.ends],
    }).toEqual({ thirdId: true, thirdText: true, aEnds: [] })
  } finally {
    first.leave()
    second.leave()
    third.leave()
  }
})

test('a RECONCILE without its attempt on a wire that holds no session goes as a loss', async () => {
  const unnumbered = wire()
  try {
    await unnumbered.send(encode.reconcile({ sessionId: crypto.randomUUID(), open: [] }))
    expect([unnumbered.hows(), unnumbered.tags().includes(TAG.RECONCILED)]).toEqual([['transient'], false])
  } finally {
    unnumbered.leave()
  }
})

test("a page's first RECONCILE names the id it made, and the session's end is kept under it for its next wire", async () => {
  const a = channel()
  const pageId = crypto.randomUUID()
  const first = wire()
  const second = wire()
  try {
    await first.send(reconcile({ sessionId: pageId, open: [entry(a.c.id, 0)] }, 1))
    const named = first.sessionId()
    // The page reads neither the RECONCILED nor the last word.
    getChannelMux().endFault(first.w, { scope: 'session', cause: 'receiveBudget' })
    await second.send(reconcile({ sessionId: pageId, open: [entry(a.c.id, 0)] }, 2))
    expect({
      named: named === pageId,
      secondEnds: second.hows(),
      reconciled: second.tags().includes(TAG.RECONCILED),
    }).toEqual({
      named: true,
      secondEnds: ['receiveBudget'],
      reconciled: false,
    })
  } finally {
    first.leave()
    second.leave()
  }
})

test('a named id the server has no record of, with channels it never had, is answered as a missing session', async () => {
  const unknown = wire()
  try {
    await unknown.send(
      reconcile({ sessionId: crypto.randomUUID(), open: [{ id: crypto.randomUUID(), ix: 0, lastSeq: 4 }] }, 2),
    )
    const reconciled = unknown.frames.filter((frame) => frame[0] === TAG.RECONCILED).map(decode)[0]
    expect([[...unknown.ends], reconciled?.tag === TAG.RECONCILED ? reconciled.payload.open : null]).toEqual([[], []])
  } finally {
    unknown.leave()
  }
})

test('a reopen retires what the old holder awaited: the channel registering later attaches on the new wire alone', async () => {
  const pending = new ServerChannel<string, string>()
  const first = wire()
  const second = wire()
  try {
    await first.send(reconcile({ open: [entry(pending.id, 0)] }, 1))
    const sessionId = first.sessionId()
    await second.send(reconcile({ sessionId, open: [entry(pending.id, 0)] }, 2))
    const firstBefore = first.frames.length
    getChannelMux().registerChannel(pending, ATTACHABLE_NOW)
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect({
      onFirst: first.frames.slice(firstBefore).some((frame) => frame[0] === TAG.ATTACH_RESULT),
      onSecond: second.tags().includes(TAG.ATTACH_RESULT),
    }).toEqual({ onFirst: false, onSecond: true })
  } finally {
    first.leave()
    second.leave()
    pending.abort()
  }
})

test('a reopen whose wire is lost before its turn takes nothing from the wire that holds the session: both channels stay, and the next message arrives there', async () => {
  const a = channel()
  const b = channel()
  const holder = wire()
  const dying = wire()
  try {
    await holder.send(reconcile({ open: [entry(a.c.id, 0), entry(b.c.id, 1)] }, 1))
    const sessionId = holder.sessionId()
    const holderBefore = holder.frames.length
    // The reopen's turn is queued when its wire is lost.
    const settling = dying.send(reconcile({ sessionId, open: [entry(a.c.id, 0)] }, 2))
    dying.lose()
    await settling
    void a.c.send('next').catch(() => {})
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect({
      aEnds: [...a.ends],
      bEnds: [...b.ends],
      onHolder: holder.frames.slice(holderBefore).some((frame) => frame[0] === TAG.TEXT),
      dyingReconciled: dying.tags().includes(TAG.RECONCILED),
    }).toEqual({ aEnds: [], bEnds: [], onHolder: true, dyingReconciled: false })
  } finally {
    holder.leave()
    dying.leave()
  }
})

test('a page that knows only an id whose session the server dropped reads an end decided on its reopen under that id, though that reopen was never answered', async () => {
  const pending = new ServerChannel<string, string>()
  const first = wire()
  const second = wire()
  const third = wire()
  try {
    // The first wire holds only a channel not registered yet: lost, the session goes with it.
    await first.send(reconcile({ open: [entry(pending.id, 0)] }, 1))
    const known = first.sessionId()
    first.lose()
    getChannelMux().registerChannel(pending, ATTACHABLE_NOW)
    await second.send(reconcile({ sessionId: known, open: [entry(pending.id, 0)] }, 2))
    // The server ends the session on the second wire; the page reads neither its RECONCILED nor its last word.
    await second.send(unreadable)
    await third.send(reconcile({ sessionId: known, open: [entry(pending.id, 0)] }, 3))
    expect(third.hows()).toEqual(['unreadable'])
  } finally {
    first.leave()
    second.leave()
    third.leave()
    pending.abort()
  }
})

test("a channel named in a RECONCILE queued behind its wire's session end ends with that end's cause as it registers", async () => {
  const live = channel()
  const waiting = new ServerChannel<string, string>()
  const holder = wire()
  try {
    await holder.send(reconcile({ open: [entry(live.c.id, 0)] }, 1))
    const sessionId = holder.sessionId()
    const queued = holder.send(
      reconcile({ sessionId, open: [entry(live.c.id, 0), entry(waiting.id, 1)], released: [] }, 1),
    )
    getChannelMux().endFault(holder.w, { scope: 'session', cause: 'unreadable' })
    await queued
    const ends: string[] = []
    waiting.onClose((err) => void ends.push(err ? err.message : 'clean'))
    getChannelMux().registerChannel(waiting, ATTACHABLE_NOW)
    expect([...ends]).toEqual([END_CAUSE.unreadable.error().message])
  } finally {
    holder.leave()
    waiting.abort()
  }
})

test("a reopen answered with its session's kept end ends a registered channel it names that the session never held, with that end's cause", async () => {
  const live = channel()
  const other = channel()
  const first = wire()
  const second = wire()
  try {
    await first.send(reconcile({ open: [entry(live.c.id, 0)] }, 1))
    const sessionId = first.sessionId()
    getChannelMux().endFault(first.w, { scope: 'session', cause: 'receiveBudget' })
    await second.send(reconcile({ sessionId, open: [entry(live.c.id, 0), entry(other.c.id, 1)] }, 2))
    expect({ answer: second.hows(), otherEnds: [...other.ends] }).toEqual({
      answer: ['receiveBudget'],
      otherEnds: [END_CAUSE.receiveBudget.error().message],
    })
  } finally {
    first.leave()
    second.leave()
    other.c.abort()
  }
})

test("two reopens answered with their session's kept end, both unread, naming one channel not registered yet leave one wait for it, and the channel ends with that cause as it registers", async () => {
  const live = channel()
  const pending = new ServerChannel<string, string>()
  const first = wire()
  const second = wire()
  const third = wire()
  const waits = vi.spyOn(RegistrationWaits.prototype, 'wait')
  try {
    await first.send(reconcile({ open: [entry(live.c.id, 0)] }, 1))
    const sessionId = first.sessionId()
    // The page reads neither the last word nor the two answers below.
    getChannelMux().endFault(first.w, { scope: 'session', cause: 'receiveBudget' })
    await second.send(reconcile({ sessionId, open: [entry(live.c.id, 0), entry(pending.id, 1)] }, 2))
    await third.send(reconcile({ sessionId, open: [entry(live.c.id, 0), entry(pending.id, 1)] }, 3))
    const ends: string[] = []
    pending.onClose((err) => void ends.push(err ? err.message : 'clean'))
    getChannelMux().registerChannel(pending, ATTACHABLE_NOW)
    expect({
      waits: waits.mock.calls.filter(([awaited]) => awaited.channelId === pending.id).length,
      answers: [second.hows(), third.hows()],
      ends: [...ends],
    }).toEqual({
      waits: 1,
      answers: [['receiveBudget'], ['receiveBudget']],
      ends: [END_CAUSE.receiveBudget.error().message],
    })
  } finally {
    waits.mockRestore()
    first.leave()
    second.leave()
    third.leave()
    pending.abort()
  }
})

test("an app whose channel's open registers a channel the wire awaits at two indexes closes the wire mid-RECONCILE: the channel the RECONCILE attaches after that close gets a reported end", async () => {
  const pending = new ServerChannel<string, string>()
  const trigger = channel()
  const after = channel()
  const holder = wire()
  const pendingEnds: string[] = []
  pending.onClose((err) => void pendingEnds.push(err ? err.message : 'clean'))
  try {
    await holder.send(reconcile({ open: [entry(pending.id, 1)] }, 1))
    const sessionId = holder.sessionId()
    trigger.c.onOpen(() => getChannelMux().registerChannel(pending, ATTACHABLE_NOW))
    // `pending` again at index 2: its registration, as `trigger` opens, attaches it at 1 and then finds it held at 2,
    // a broken page, which ends the session before `after` attaches.
    await holder.send(
      reconcile({ sessionId, open: [entry(pending.id, 2), entry(trigger.c.id, 0), entry(after.c.id, 3)] }, 1),
    )
    expect({
      how: holder.hows(),
      after: after.ends.map((end) => end.includes("can't read")),
      pending: pendingEnds.map((end) => end.includes("can't read")),
      reconciled: holder.tags().filter((tag) => tag === TAG.RECONCILED).length,
    }).toEqual({ how: ['unreadable'], after: [true], pending: [true], reconciled: 1 })
  } finally {
    holder.leave()
    pending.abort()
  }
})

// A refused open leaves nothing: everything a wire's first RECONCILE, or a PREPARE, can be refused for is checked before
// a session is made, claimed or handed over, or a stage is staged or replaced.

test('a first RECONCILE that carries `released` goes as a loss and leaves no session under the id it names: a PREPARE naming that id goes as a loss too', async () => {
  const opener = wire()
  const probe = wire()
  const sessionId = crypto.randomUUID()
  try {
    await opener.send(encode.reconcile({ sessionId, attempt: 1, open: [], released: [] }))
    await probe.send(prepare({ sessionId, upgradeId: 'up' }, 2))
    expect({
      opener: opener.hows(),
      probe: probe.hows(),
      ready: probe.tags().includes(TAG.READY),
    }).toEqual({ opener: ['transient'], probe: ['transient'], ready: false })
  } finally {
    opener.leave()
    probe.leave()
  }
})

test("a RECONCILE naming another session than its wire's is refused before that session's stage is claimed: its staged probe stays, and its BARRIER commits", async () => {
  const a = channel()
  const source = wire()
  const target = wire()
  const other = wire()
  const open = [entry(a.c.id, 0)]
  try {
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await target.send(prepare({ sessionId, upgradeId: 'up' }, 2))
    await other.send(reconcile({ open: [] }, 1))
    await other.send(reconcile({ sessionId, open: [] }, 2))
    await source.send(encode.barrier({ sessionId, upgradeId: 'up', open }))
    expect({
      other: other.hows(),
      target: target.hows(),
      targetId: target.sessionId() === sessionId,
      aEnds: [...a.ends],
    }).toEqual({ other: ['unreadable'], target: [], targetId: true, aEnds: [] })
  } finally {
    other.leave()
    source.leave()
    target.leave()
  }
})

test("a PREPARE over the staged byte budget is refused before it replaces its session's older stage, which stays and commits", async () => {
  // Frames of about 1.5 MB, each under the control frame cap: 44 of them fit the budget, a 45th doesn't.
  const pad = 'x'.repeat(1_500_000)
  const padded = (sessionId: string, upgradeId: string, attempt: number) => {
    const payload = { sessionId, upgradeId, attempt, pad }
    return encode.prepare(payload)
  }
  const others = Array.from({ length: 44 }, () => ({ holder: wire(), probe: wire() }))
  const a = channel()
  const source = wire()
  const older = wire()
  const newer = wire()
  const open = [entry(a.c.id, 0)]
  try {
    for (const { holder, probe } of others) {
      await holder.send(reconcile({ open: [] }, 1))
      await probe.send(padded(holder.sessionId() ?? '', 'filler', 2))
    }
    await source.send(reconcile({ open }, 1))
    const sessionId = source.sessionId() ?? ''
    await older.send(prepare({ sessionId, upgradeId: 'older' }, 2))
    await newer.send(padded(sessionId, 'newer', 3))
    await source.send(encode.barrier({ sessionId, upgradeId: 'older', open }))
    expect({
      fillersStaged: others.every(({ probe }) => probe.tags().includes(TAG.READY) && probe.ends.length === 0),
      newer: newer.hows(),
      older: older.hows(),
      olderId: older.sessionId() === sessionId,
      aEnds: [...a.ends],
    }).toEqual({ fillersStaged: true, newer: ['transient'], older: [], olderId: true, aEnds: [] })
  } finally {
    for (const { holder, probe } of others) {
      probe.leave()
      holder.leave()
    }
    newer.leave()
    older.leave()
    source.leave()
  }
})

// The order a page's attempts arrive in, against what the server has of its session: the live session, or the record
// it keeps of one that retired (emptied with no wire holding it) or that it ended, each holding the highest attempt
// that took it. Each arrival is a wire whose first RECONCILE names the session with its attempt: it takes the session
// (`took`), goes as a loss (`transient`), or is answered with the session's end.

/** Each attempt in order, on a wire of its own, and what it came to as it was answered. */
async function arrivals(sessionId: string, attempts: number[]) {
  const wires: ReturnType<typeof wire>[] = []
  const came: string[] = []
  for (const attempt of attempts) {
    const arriving = wire()
    wires.push(arriving)
    await arriving.send(reconcile({ sessionId, open: [] }, attempt))
    came.push(arriving.tags().includes(TAG.RECONCILED) ? 'took' : (arriving.hows()[0] ?? 'nothing'))
  }
  return {
    came,
    leave: () => {
      for (const each of wires) each.leave()
    },
  }
}

/** A session attempt 1 took with nothing in it, as the server has it once `how` happened to it. */
async function sessionThat(how: 'lives' | 'retired' | 'ended') {
  const a = channel()
  const holder = wire()
  await holder.send(reconcile({ open: how === 'ended' ? [entry(a.c.id, 0)] : [] }, 1))
  const sessionId = holder.sessionId() ?? ''
  if (how === 'retired') holder.lose()
  if (how === 'ended') await holder.send(bad)
  return { sessionId, holder }
}

test.each([
  ['lives', [1, 3, 2], ['transient', 'took', 'transient']],
  ['lives', [2, 1, 3], ['took', 'transient', 'took']],
  ['retired', [1, 3, 2], ['transient', 'took', 'transient']],
  ['retired', [2, 1, 3], ['took', 'transient', 'took']],
  ['ended', [1, 3, 2], ['transient', 'unreadable', 'transient']],
  ['ended', [2, 1, 3], ['unreadable', 'transient', 'unreadable']],
] as const)(
  'attempts arriving against a session that %s, in the order %j, come to %j: one not above the highest the server has takes nothing',
  async (how, attempts, came) => {
    const { sessionId, holder } = await sessionThat(how)
    const arrived = await arrivals(sessionId, [...attempts])
    try {
      expect(arrived.came).toEqual(came)
    } finally {
      arrived.leave()
      holder.leave()
    }
  },
)

test('a session that retires again after a newer attempt took it back keeps that attempt as its highest: an older one read after takes nothing', async () => {
  const { sessionId, holder } = await sessionThat('retired')
  const newer = wire()
  try {
    await newer.send(reconcile({ sessionId, open: [] }, 3))
    newer.lose()
    const arrived = await arrivals(sessionId, [2])
    arrived.leave()
    expect({ newer: newer.tags().includes(TAG.RECONCILED), older: arrived.came }).toEqual({
      newer: true,
      older: ['transient'],
    })
  } finally {
    newer.leave()
    holder.leave()
  }
})

test("a first RECONCILE without its attempt naming a session whose end is kept goes as a loss: it is given no end, and the channel it names isn't touched", async () => {
  const { sessionId, holder } = await sessionThat('ended')
  const y = channel()
  const unnumbered = wire()
  try {
    await unnumbered.send(encode.reconcile({ sessionId, open: [entry(y.c.id, 0)] }))
    expect({ how: unnumbered.hows(), yEnds: [...y.ends] }).toEqual({ how: ['transient'], yEnds: [] })
  } finally {
    unnumbered.leave()
    holder.leave()
    y.c.abort()
  }
})

test('a first RECONCILE that names no session, as a page of 0.3.1 sends it, goes as a loss', async () => {
  const first = wire()
  try {
    const frame: unknown = Reflect.apply(encode.reconcile, encode, [{ open: [] }])
    if (!(frame instanceof Uint8Array)) throw new Error('no frame')
    await first.send(Uint8Array.from(frame))
    expect([first.hows(), first.tags().includes(TAG.RECONCILED)]).toEqual([['transient'], false])
  } finally {
    first.leave()
  }
})
