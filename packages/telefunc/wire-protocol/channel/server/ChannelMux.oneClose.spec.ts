// The one close the server decides on a wire: decided once, before anything else can enter; a session's last word is
// that close even when the wire's sender saw its backlog pass earlier in the same turn; and a transport whose close
// throws still leaves the mux holding nothing of the wire. On the real mux, with a WebSocket wire the test holds.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { getChannelMux, type ServerEnd, type ServerTransport } from './ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'

config.channel = { reconnectTimeout: 1_000, connectTtl: 15_000 }

/** A WebSocket wire whose socket holds `buffered` bytes, and each end the mux gave it; `throws` makes its close throw. */
function wire({ throws = false } = {}) {
  const mux = getChannelMux()
  const w = { buffered: 0 }
  const ends: ServerEnd[] = []
  const transport: ServerTransport<typeof w> = {
    getConnId: () => null,
    sendNow: () => {},
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: (x) => x.buffered,
    close: (_x, end) => {
      ends.push(end)
      if (throws) throw new Error('The socket could not close.')
    },
  }
  mux.onWireOpen(w, transport)
  return { w, ends, hows: () => ends.map(({ how }) => how) }
}

function channel() {
  const c = new ServerChannel<string, string>()
  getChannelMux().registerChannel(c, ATTACHABLE_NOW)
  const ends: string[] = []
  c.onClose((err) => void ends.push(err ? err.message : 'clean'))
  return { c, ends }
}

test("a session's end closes its wire with its cause though the wire's sender saw its backlog pass earlier in the same turn", async () => {
  const mux = getChannelMux()
  const page = wire()
  const outcome = await mux.dispatchWireMessage(
    page.w,
    encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }),
  )
  if (outcome === null) throw new Error('No RECONCILE outcome')
  page.w.buffered = Number.MAX_SAFE_INTEGER
  // The RECONCILED meets the full socket: the sender's backlog passed.
  mux.sendReconciled(outcome)
  mux.endFault(page.w, { scope: 'session', cause: 'unreadable' })
  await Promise.resolve()
  expect(page.hows()).toEqual(['unreadable'])
})

test("a wire whose transport throws as it closes is closed once, and its session's channels end with the cause", async () => {
  const mux = getChannelMux()
  const page = wire({ throws: true })
  const x = channel()
  try {
    await mux.onWireMessage(
      page.w,
      encode.reconcile({
        sessionId: crypto.randomUUID(),
        attempt: 1,
        open: [{ id: x.c.id, ix: 0, lastSeq: 0, initial: true }],
      }),
    )
    expect(() => mux.endFault(page.w, { scope: 'session', cause: 'unreadable' })).toThrow('The socket could not close.')
    // A later decision finds no wire.
    mux.endFault(page.w, { scope: 'loss' })
    expect({ closes: page.hows(), ended: x.ends.length }).toEqual({ closes: ['unreadable'], ended: 1 })
  } finally {
    x.c.abort()
  }
})
