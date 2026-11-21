// Which wire holds a page's session, on the real Node server with the real page: a reopen keeps the session's id, a wire
// that holds nothing ends only as a loss, an older attempt's open read late takes nothing, and a page whose first
// RECONCILED and last word were both lost, on SSE or on a WebSocket, reads the server's cause at its reopen.

import { expect, test } from 'vitest'
import { decodeU32, encodeU32 } from '../../wire/envelope.js'
import { decode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { getWireUrl } from './getSessionUrl.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../../wire/requestKind.js'
import { delay, framesStart, opens, pair, rewriting, sends, until, useRealServer, world } from './realServer.fixture.js'

useRealServer(['sse', 'ws'], { reconnectTimeout: 1_500 })

/** The session id of each RECONCILED an event stream hands the page, and `cut()` of that stream. */
function readingReconciled(ids: string[]) {
  return rewriting((frame) => {
    if (frame[0] === TAG.RECONCILED) {
      const decoded = decode(frame)
      if (decoded.tag === TAG.RECONCILED) ids.push(decoded.payload.sessionId)
    }
    return frame
  })
}

/** The page's WebSockets as made, for a test to close one as a dropped connection does, and the index of each socket
 *  a RECONCILED reached the page on. */
function capturedSockets() {
  const Counted = globalThis.WebSocket
  const sockets: WebSocket[] = []
  const reconciledOn: number[] = []
  globalThis.WebSocket = class Captured extends Counted {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols)
      const index = sockets.push(this) - 1
      this.addEventListener('message', (event) => {
        if (!(event.data instanceof ArrayBuffer)) return
        const message = new Uint8Array(event.data)
        for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
          if (frame[0] === TAG.RECONCILED) reconciledOn.push(index)
        }
      })
    }
  }
  return { sockets, reconciledOn, restore: () => void (globalThis.WebSocket = Counted) }
}

/** The session each RECONCILE the page sends on a WebSocket names, in order. */
function namedOnSockets(named: (string | undefined)[]) {
  world.wsSends = (message) => {
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
      const decoded = frame[0] === TAG.RECONCILE ? decode(frame) : null
      if (decoded?.tag === TAG.RECONCILE) named.push(decoded.payload.sessionId)
    }
    return [message]
  }
}

/** The page's first WebSocket as if its RECONCILED and the server's last word never reached the page: the messages
 *  that carry them are dropped, and its close reads as a dropped connection's. */
function withholdingFirstSocket() {
  const Counted = globalThis.WebSocket
  const seen = { reconciled: false }
  let made = 0
  globalThis.WebSocket = class Withholding extends Counted {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols)
      if (made++ > 0) return
      // Added before the page sets its handlers, so these run first and can keep an event from them.
      this.addEventListener('message', (event) => {
        if (!(event.data instanceof ArrayBuffer)) return
        const message = new Uint8Array(event.data)
        const frames = isOneFrame(message) ? [message] : [...messageFrames(message)]
        if (frames.some((frame) => frame[0] === TAG.RECONCILED)) seen.reconciled = true
        if (frames.some((frame) => frame[0] === TAG.RECONCILED || frame[0] === TAG.END))
          event.stopImmediatePropagation()
      })
      let dropped = false
      this.addEventListener('close', (event) => {
        if (dropped) return
        dropped = true
        event.stopImmediatePropagation()
        this.dispatchEvent(Object.assign(new Event('close'), { code: 1006, reason: '' }))
      })
    }
  }
  return { seen, restore: () => void (globalThis.WebSocket = Counted) }
}
/** The connId an SSE open's body names, read off its metadata. */
function connIdOf(bytes: Uint8Array<ArrayBuffer>): string {
  const metadata: { connId: string } = JSON.parse(new TextDecoder().decode(bytes.subarray(4, 4 + decodeU32(bytes))))
  return metadata.connId
}

test('a reopen over SSE is answered with the session id the page had', async () => {
  const ids: string[] = []
  const first = readingReconciled(ids)
  world.hooks = [
    { kind: 'open', stream: first.stream },
    { kind: 'open', stream: readingReconciled(ids).stream },
  ]
  const live = pair(crypto.randomUUID(), ['sse'])
  try {
    await live.page.opened
    first.cut()
    await until(() => ids.length === 2)
    expect([ids[1] === ids[0], live.ended]).toEqual([true, []])
  } finally {
    live.page.abort()
  }
})

test('a reopen over a WebSocket is answered with the session id the page had', async () => {
  const named: (string | undefined)[] = []
  namedOnSockets(named)
  const captured = capturedSockets()
  const live = pair(crypto.randomUUID(), ['ws'])
  try {
    await live.page.opened
    captured.sockets[0]?.close()
    await until(() => named.length === 2)
    // The second wire's RECONCILED reaches the page before that wire is dropped.
    await until(() => captured.reconciledOn.includes(1))
    captured.sockets[1]?.close()
    // The third RECONCILE names what the second wire's RECONCILED gave.
    await until(() => named.length === 3)
    expect([named[2] === named[1], live.ended]).toEqual([true, []])
  } finally {
    captured.restore()
    live.page.abort()
  }
})

for (const transport of ['ws', 'sse'] satisfies ('ws' | 'sse')[]) {
  test(`an unreadable first message on every ${transport} wire after a loss is a loss each time: the page reopens until its recovery deadline, and ends with the loss`, async () => {
    const garbled = Uint8Array.from([0xff, 0xff, 0xff])
    // On SSE the open's frame is one of a tag no frame has, length-prefixed so that it is read whole.
    const garble = (bytes: Uint8Array<ArrayBuffer>) => [bytes.subarray(0, framesStart(bytes)), encodeU32(3), garbled]
    const first = rewriting((frame) => frame)
    world.hooks = [{ kind: 'open', stream: first.stream }]
    const captured = capturedSockets()
    const live = pair(crypto.randomUUID(), [transport])
    try {
      // The first wire reconciles, so the page's recovery deadline is the server's `reconnectTimeout` past the loss.
      await live.page.opened
      world.wsSends = (message) => {
        const frames = isOneFrame(message) ? [message] : [...messageFrames(message)]
        return frames.some((frame) => frame[0] === TAG.RECONCILE) ? [garbled] : [message]
      }
      for (let n = 0; n < 40; n++) world.hooks.push({ kind: 'open', body: garble })
      const wiresBefore = transport === 'ws' ? world.sockets : opens()
      if (transport === 'ws') captured.sockets[0]?.close()
      else first.cut()
      await until(() => live.ended.length > 0, 25_000)
      const wires = (transport === 'ws' ? world.sockets : opens()) - wiresBefore
      expect([wires > 1, live.ended.map((err) => err.message.includes("can't read"))]).toEqual([true, [false]])
    } finally {
      captured.restore()
      live.page.abort()
    }
  }, 30_000)
}

test(
  'an older SSE open the page gave up, read after its newer reopen, takes nothing: no channel ends and every message arrives once',
  { timeout: 15_000 },
  async () => {
    const first = rewriting((frame) => frame)
    let olderOpen: Uint8Array<ArrayBuffer> | null = null
    const garbled = Uint8Array.from([0xff, 0xff, 0xff])
    world.hooks = [
      { kind: 'open', stream: first.stream },
      // The older reopen is held on the way: the server reads something it can't, and the page reopens again.
      {
        kind: 'open',
        body: (bytes) => {
          olderOpen = bytes
          return [bytes.subarray(0, framesStart(bytes)), encodeU32(3), garbled]
        },
      },
    ]
    const connectionKey = crypto.randomUUID()
    const a = pair(connectionKey, ['sse'])
    try {
      await a.page.opened
      first.cut()
      await until(() => opens() === 3)
      const b = pair(connectionKey, ['sse'])
      try {
        await b.page.opened
        sends(b.page, 'b1')
        // The older reopen reaches the server now, as from a proxy that held it.
        const held = olderOpen
        if (held === null) throw new Error('no older open')
        const pageOpens = opens()
        const late = await fetch(getMarkedRequestUrl(getWireUrl(world.telefuncUrl), REQUEST_KIND.SSE), {
          method: 'POST',
          headers: { accept: 'text/event-stream', [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE },
          body: held,
        })
        await late.body?.cancel()
        sends(a.page, 'a1')
        await until(() => a.got.length > 0 && b.got.length > 0)
        await delay(300)
        // The late request is the one open since: the page opened no other wire.
        expect({ a: a.got, b: b.got, ended: [...a.ended, ...b.ended], opensSince: opens() - pageOpens }).toEqual({
          a: ['a1'],
          b: ['b1'],
          ended: [],
          opensSince: 1,
        })
      } finally {
        b.page.abort()
      }
    } finally {
      a.page.abort()
    }
  },
)

test("a first SSE wire whose RECONCILED and last word both never reach the page: the reopen reads the server's cause", async () => {
  let connId = ''
  const first = rewriting((frame) => (frame[0] === TAG.RECONCILED || frame[0] === TAG.END ? null : frame))
  world.hooks = [
    {
      kind: 'open',
      stream: first.stream,
      body: (bytes) => {
        connId = connIdOf(bytes)
        return [bytes]
      },
    },
  ]
  const live = pair(crypto.randomUUID(), ['sse'])
  const serverEnds: string[] = []
  live.channel.onClose((err) => void serverEnds.push(err ? err.message : 'clean'))
  try {
    await until(() => connId !== '' && getChannelMux().getWireByConnId(connId) !== undefined)
    await delay(200)
    const wire = getChannelMux().getWireByConnId(connId)
    if (wire !== undefined) getChannelMux().endFault(wire, { scope: 'session', cause: 'receiveBudget' })
    first.cut()
    // Within the test's own limit, so a page that reads nothing is seen as such.
    await until(() => live.ended.length > 0, 12_000).catch(() => {})
    const budget = 'The page sent more on its connection than the server holds while it works on what came before.'
    expect([
      live.ended.map((err) => err.message.includes(budget)),
      serverEnds.map((end) => end.includes(budget)),
    ]).toEqual([[true], [true]])
  } finally {
    live.page.abort()
  }
}, 20_000)

test("a first WebSocket wire whose RECONCILED and last word both never reach the page: the reopen reads the server's cause", async () => {
  // An unreadable message behind the page's first RECONCILE ends the session that RECONCILE took.
  let garbled = false
  world.wsSends = (message) => {
    const frames = isOneFrame(message) ? [message] : [...messageFrames(message)]
    if (garbled || !frames.some((frame) => frame[0] === TAG.RECONCILE)) return [message]
    garbled = true
    return [message, Uint8Array.from([0xff, 0xff, 0xff])]
  }
  const withheld = withholdingFirstSocket()
  const socketsBefore = world.sockets
  const live = pair(crypto.randomUUID(), ['ws'])
  const serverEnds: string[] = []
  live.channel.onClose((err) => void serverEnds.push(err ? err.message : 'clean'))
  try {
    // Within the test's own limit, so a page that reads nothing is seen as such.
    await until(() => live.ended.length > 0, 12_000).catch(() => {})
    // The page reopened: the cause it reads is the kept end its second wire was answered with.
    expect([
      withheld.seen.reconciled,
      world.sockets - socketsBefore > 1,
      serverEnds.length,
      live.ended.map((err) => err.message),
    ]).toEqual([true, true, 1, serverEnds])
  } finally {
    withheld.restore()
    live.page.abort()
  }
}, 20_000)
