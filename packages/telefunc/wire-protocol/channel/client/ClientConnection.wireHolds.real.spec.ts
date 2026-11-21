// Which wire holds a page's session, on the real Node server with the real page: a reopen keeps the session's id, and a
// wire that holds nothing ends only as a loss.

import { expect, test } from 'vitest'
import { encodeU32 } from '../../wire/envelope.js'
import { decode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { framesStart, opens, pair, rewriting, until, useRealServer, world } from './realServer.fixture.js'

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
