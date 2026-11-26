// A close stays whole whatever its transports do: a session's end keeps its cause when its transport throws as it is
// handed a channel's earlier end, or reports the wire closed from inside that hand-over; and a session's wire that
// closes releases the session even when the WebSocket its page was moving to can't be closed. On the real mux, with
// wires the test holds whose transports report their close synchronously, as some hosts do, and through the real SSE
// handler.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux, type ServerEnd, type ServerTransport } from './ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'
import { openStream } from './serverPage.fixture.js'
import { noUpgrades, upgradesTakenHere } from '../../../node/server/runTelefunc/takesUpgrades.js'

config.channel = { reconnectTimeout: 1_000, connectTtl: 2_000 }

const mux = getChannelMux()

/** A WebSocket wire, or an SSE one with `sse`, the ends the mux gave it, and the tag of each frame it was handed:
 *  `handed` runs on each frame; with `closeThrows`, its close throws instead of reporting the wire closed. */
function wire({
  sse = false,
  closeThrows = false,
  handed,
}: { sse?: boolean; closeThrows?: boolean; handed?: (tag: number) => void } = {}) {
  const w = {}
  const ends: ServerEnd[] = []
  const frames: number[] = []
  const connId = crypto.randomUUID()
  const transport: ServerTransport<typeof w> = {
    getConnId: () => (sse ? connId : null),
    sendNow: (_x, message) => {
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
        const { tag } = decode(frame)
        frames.push(tag)
        handed?.(tag)
      }
    },
    type: sse ? CHANNEL_TRANSPORT.SSE : CHANNEL_TRANSPORT.WS,
    wholeMessages: !sse,
    queueOf: () => ({
      waitingPublishes: () => 0,
      dropWaitingPublishes: () => {},
      dropWaitingFrom: () => {},
      dropUngiven: () => 0,
    }),
    bufferedAmount: () => 0,
    close: (x, end) => {
      ends.push(end)
      if (closeThrows) throw new Error('The socket could not close.')
      mux.onWireClosed(x, end.how)
    },
  }
  mux.onWireOpen(w, transport, sse ? noUpgrades : upgradesTakenHere)
  return {
    w,
    connId,
    frames,
    hows: () => ends.map(({ how }) => how),
    lose: () => mux.onWireClosed(w, 'transient'),
    leave: () => mux.onWireClosed(w, 'pageLeft'),
  }
}

function channel(ix: number) {
  const c = new ServerChannel<string, string>()
  const ends: string[] = []
  c.onClose((err) => void ends.push(err?.name ?? 'clean'))
  mux.registerChannel(c, ATTACHABLE_NOW)
  const entry: ReconcileOpenEntry = { id: c.id, ix, lastSeq: 0, initial: true }
  return { c, ends, entry }
}

test.each(['throws', 'reports the wire closed'] as const)(
  "a session's end keeps its cause when the transport %s as it is handed a channel's earlier end",
  async (how) => {
    const refused = channel(0)
    const other = channel(1)
    const holder = wire({
      handed: (tag) => {
        if (tag !== TAG.ERROR) return
        if (how === 'throws') throw new Error('The socket could not write.')
        holder.leave()
      },
    })
    const sessionId = crypto.randomUUID()
    try {
      // The RECONCILED stays owed, so the refused channel's end waits behind it until the session ends.
      await mux.dispatchWireMessage(
        holder.w,
        encode.reconcile({ sessionId, attempt: 1, open: [refused.entry, other.entry] }),
      )
      mux.endFault(holder.w, { scope: 'channel', ix: 0, cause: 'frameRefused', wire: 'kept' })
      expect(other.ends).toEqual([])
      try {
        mux.endFault(holder.w, { scope: 'session', cause: 'unreadable' })
      } catch (err) {
        if (how !== 'throws') throw err
        expect(err).toMatchObject({ message: 'The socket could not write.' })
      }
      const next = wire()
      try {
        await mux.onWireMessage(next.w, encode.reconcile({ sessionId, attempt: 2, open: [] }))
        expect({ other: other.ends, next: next.hows() }).toEqual({
          other: ['ChannelClosedError'],
          next: ['unreadable'],
        })
      } finally {
        next.leave()
      }
    } finally {
      holder.leave()
      refused.c.abort()
      other.c.abort()
    }
  },
)

const endOf = { unreadable: 'ChannelClosedError', transient: 'NetworkError', pageLeft: 'ChannelPageLeftError' } as const

test.each(['unreadable', 'transient', 'pageLeft'] as const)(
  "a session's SSE wire that closes as %s while its page moves to a WebSocket that can't be closed ends its channels",
  async (cause) => {
    const holder = wire({ sse: true })
    const probe = wire({ closeThrows: true })
    const x = channel(0)
    const sessionId = crypto.randomUUID()
    try {
      await mux.onWireMessage(holder.w, encode.reconcile({ sessionId, attempt: 1, open: [x.entry] }))
      await mux.onWireMessage(probe.w, encode.prepare({ sessionId, upgradeId: crypto.randomUUID(), attempt: 2 }))
      expect(probe.frames).toContain(TAG.READY)
      try {
        if (cause === 'unreadable') mux.endFault(holder.w, { scope: 'session', cause })
        else if (cause === 'transient') holder.lose()
        else holder.leave()
      } catch (err) {
        expect(err).toMatchObject({ message: 'The socket could not close.' })
      }
      // The channel waits out the reconnect grace after a loss, which no reopen ends.
      if (cause === 'transient') await new Promise((resolve) => setTimeout(resolve, 1_200))
      expect({
        indexed: mux.getWireByConnId(holder.connId) !== undefined,
        probe: probe.hows(),
        x: [...x.ends],
      }).toEqual({
        indexed: false,
        probe: ['transient'],
        x: [endOf[cause]],
      })
    } finally {
      holder.leave()
      probe.leave()
      x.c.abort()
    }
  },
)

test("a page that leaves while its WebSocket can't be closed gets its SSE stream ended, and its channels end as it left", async () => {
  const x = channel(0)
  const sessionId = crypto.randomUUID()
  const holder = await openStream('web', encode.reconcile({ sessionId, attempt: 1, open: [x.entry] }))
  await holder.readUntil((frame) => frame[0] === TAG.RECONCILED)
  const probe = wire({ closeThrows: true })
  try {
    await mux.onWireMessage(probe.w, encode.prepare({ sessionId, upgradeId: crypto.randomUUID(), attempt: 2 }))
    expect(probe.frames).toContain(TAG.READY)
    const left = await holder.leave().then(
      () => 'answered',
      (err: unknown) => (err instanceof Error ? err.message : String(err)),
    )
    const stream = await Promise.race([
      holder.readUntil(() => false).then(() => 'ended'),
      new Promise((resolve) => setTimeout(resolve, 500, 'still open')),
    ])
    expect({ left, stream, probe: probe.hows(), x: [...x.ends] }).toEqual({
      left: 'The socket could not close.',
      stream: 'ended',
      probe: ['transient'],
      x: ['ChannelPageLeftError'],
    })
  } finally {
    probe.leave()
    x.c.abort()
  }
})
