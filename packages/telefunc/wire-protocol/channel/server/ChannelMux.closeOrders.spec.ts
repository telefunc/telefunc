// A close the server decides stays decided whatever its last output does: a transport that throws as it is handed a
// channel's earlier end, or that reports the wire closed from inside that hand-over. On the real mux, with wires the
// test holds whose transports report their close synchronously, as some hosts do.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux, type ServerEnd, type ServerTransport } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'

config.channel = { reconnectTimeout: 1_000, connectTtl: 2_000 }

const mux = getChannelMux()

/** A WebSocket wire, the ends the mux gave it, and the tag of each frame it was handed; `handed` runs on each frame. */
function wire({ handed }: { handed?: (tag: number) => void } = {}) {
  const w = {}
  const ends: ServerEnd[] = []
  const transport: ServerTransport<typeof w> = {
    getConnId: () => null,
    sendNow: (_x, message) => {
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) handed?.(decode(frame).tag)
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
  return { w, hows: () => ends.map(({ how }) => how), leave: () => mux.onWireClosed(w, 'pageLeft') }
}

function channel(ix: number) {
  const c = new ServerChannel<string, string>()
  const ends: string[] = []
  c.onClose((err) => void ends.push(err?.name ?? 'clean'))
  mux.registerChannel(c)
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
