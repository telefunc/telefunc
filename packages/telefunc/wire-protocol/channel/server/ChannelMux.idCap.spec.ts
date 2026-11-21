// A channel's id is at most 256 bytes as JSON writes it, the length the control-frame cap counts each entry's id at:
// an app's longer id is a usage error at its creation, and a page's in a RECONCILE or a barrier breaks the protocol.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { getChannelMux, type ServerEnd, type ServerTransport } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'

config.channel = { reconnectTimeout: 1_000, connectTtl: 15_000 }

/** A WebSocket wire the test holds, which records each end the server gives it. */
function page() {
  const mux = getChannelMux()
  const wire: { id?: string } = {}
  const ends: ServerEnd[] = []
  const transport: ServerTransport<typeof wire> = {
    getConnId: () => null,
    // The wire keeps the id the last RECONCILED gave it.
    sendNow: (w, message) => {
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
        const decoded = frame[0] === TAG.RECONCILED ? decode(frame) : null
        if (decoded?.tag === TAG.RECONCILED) w.id = decoded.payload.sessionId
      }
    },
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => 0,
    close: (w, end) => {
      ends.push(end)
      mux.onWireClosed(w, end.how)
    },
  }
  mux.onWireOpen(wire, transport)
  return {
    wire,
    ends,
    send: (frame: Uint8Array<ArrayBuffer>) => mux.onWireMessage(wire, frame),
    end: () => mux.onWireClosed(wire, 'pageLeft'),
  }
}

/** 256 bytes as UTF-8, and twice that as JSON writes it: each quote is escaped. */
const quotes = '"'.repeat(256)

test("an app's channel id over 256 bytes as JSON writes it is a usage error at its creation", () => {
  expect(new ServerChannel({ id: 'x'.repeat(256) }).id.length).toBe(256)
  expect(() => new ServerChannel({ id: 'x'.repeat(257) })).toThrow('256 bytes')
  expect(() => new ServerChannel({ id: quotes })).toThrow('256 bytes')
})

test('a first RECONCILE entry naming an id over the bound ends its wire as a loss: the wire holds no session yet', async () => {
  const wire = page()
  try {
    await wire.send(encode.reconcile({ attempt: 1, open: [{ id: 'x'.repeat(257), ix: 0, lastSeq: 0, initial: true }] }))
    expect([wire.ends.map(({ how }) => how), wire.wire.id]).toEqual([['transient'], undefined])
  } finally {
    wire.end()
  }
})

test('a barrier entry naming an id over the bound as JSON writes it ends the probe, and the old wire goes on', async () => {
  const source = page()
  const target = page()
  try {
    const channel = new ServerChannel()
    getChannelMux().registerChannel(channel)
    await source.send(encode.reconcile({ attempt: 1, open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }))
    const sessionId = source.wire.id
    if (sessionId === undefined) throw new Error('No session')
    await target.send(encode.prepare({ attempt: 2, sessionId, upgradeId: 'upgrade' }))
    const open: ReconcileOpenEntry[] = [{ id: quotes, ix: 1, lastSeq: 0, initial: true }]
    await source.send(encode.barrier({ sessionId, upgradeId: 'upgrade', open }))
    expect([source.ends, target.ends.length]).toEqual([[], 1])
  } finally {
    source.end()
    target.end()
  }
})
