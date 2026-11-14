// The order the page's sends reach the wire in, across its channels, around a RECONCILE: the REAL AttachExchange,
// SendGate and ChannelTable, on a wire that records what it is handed. The server applies one connection's frames in
// the order they arrive, so the page's order is the one its app sees.

import { describe, expect, test } from 'vitest'

import { AttachExchange } from './AttachExchange.js'
import { ChannelTable, type ChannelRecord } from './ChannelTable.js'
import type { ClientChannelTransport, OutboundFrame } from './ClientConnection.js'
import { ReplayBuffer } from './ReplayBuffer.js'
import { SendGate } from './SendGate.js'
import { decode, encode } from '../../wire/frame.js'
import type { ReconciledPayload } from '../../wire/payloads.js'
import { TAG } from '../../wire/tags.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

function createChannel() {
  return {
    id: crypto.randomUUID(),
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onEnded() {},
    _attach: {
      state: () => ({}),
      receiveCredit: { onProbeAnswered() {}, acknowledge() {}, answered() {} },
      capWindows() {},
      onBatched() {},
      onUpgradeJoined() {},
    },
  }
}

/** A text frame is its value, a RECONCILE `RECONCILE <the channels it names>`. */
function label({ frame }: OutboundFrame): string {
  const decoded = decode(frame)
  if (decoded.tag === TAG.TEXT) return decoded.text
  if (decoded.tag === TAG.RECONCILE) return `RECONCILE ${decoded.payload.open.map(({ ix }) => ix).join(',')}`
  return `tag ${decoded.tag}`
}

/** A connection's exchange and gate on one wire. `carriesReconcileInOpen`: SSE's, whose RECONCILE carries the frames
 *  held behind it; else a WebSocket's, whose held frames wait for its RECONCILED. */
function connect(carriesReconcileInOpen: boolean) {
  const wire: string[] = []
  const transport = {
    carriesReconcileInOpen,
    uplink: {},
    heartbeat: null,
    behindAfter: Infinity,
    headroom: () => Infinity,
    sendFrame: (frame: OutboundFrame) => void wire.push(label(frame)),
  } as unknown as ClientChannelTransport
  const channels = new ChannelTable()
  const owner = {
    transport,
    wire: 1,
    onReconcileDue() {},
    onReconcileLate() {},
    get sendsOpen() {
      return !exchange.reconciling && !exchange.reconcileOwed
    },
    reconnecting: false,
  }
  const gate = new SendGate({ owner, channels })
  const exchange = new AttachExchange({ owner, channels, gate })

  return {
    wire,
    register(ix: number): ChannelRecord {
      const record = channels.add(ix, createChannel() as never, new ReplayBuffer(1_000_000))
      exchange.scheduleRegisterReconcile()
      return record
    },
    /** As `onReconcileDue` does once the wire takes a RECONCILE. */
    reconcile() {
      exchange.sendReconcileBatch(exchange.stageReconcileBatch())
    },
    send(record: ChannelRecord, text: string) {
      const seq = record.replay.nextSeq()
      gate.send(record, seq, encode.text(record.ix, text, seq), 'data')
    },
    /** The server's ATTACH_RESULT for channel `ix`, which it awaited. */
    attachResult(ix: number) {
      exchange.handleAttachResult(ix, 0)
    },
    /** The server's RECONCILED, attaching `ixes`; what it releases goes on the wire, as `handleReconciled` sends it. */
    reconciled(ixes: number[]) {
      const ctrl = { sessionId: 'session', open: ixes.map((ix) => ({ ix, lastSeq: 0 })) } as ReconciledPayload
      const { frames } = exchange.applyReconciled(ctrl, { deferOmitted: false, upgradeReady: false })
      for (const frame of frames) wire.push(label(frame))
    },
  }
}

for (const [transport, carriesReconcileInOpen] of [
  ['SSE', true],
  ['WebSocket', false],
] as const) {
  describe(`the page's sends across its channels, over ${transport}`, () => {
    test('a send on a channel registered while a RECONCILE is out keeps its place ahead of a later send', () => {
      const page = connect(carriesReconcileInOpen)
      const a = page.register(0)
      page.reconcile()
      page.reconciled([0])
      const d = page.register(1)
      page.reconcile()
      const c = page.register(2)
      page.send(c, 'c1')
      page.send(a, 'a2')
      page.send(d, 'd1')
      page.reconciled([1])
      page.reconciled([2])
      expect(page.wire.filter((frame) => !frame.startsWith('RECONCILE'))).toEqual(['c1', 'a2', 'd1'])
    })

    test('a send on a channel the server awaits holds no later send', () => {
      const page = connect(carriesReconcileInOpen)
      const a = page.register(0)
      page.reconcile()
      page.reconciled([0])
      // Left out of the RECONCILED that answers the RECONCILE naming it: the server awaits it.
      const b = page.register(1)
      page.reconcile()
      page.reconciled([])
      page.send(b, 'b1')
      page.register(2)
      page.reconcile()
      page.send(a, 'a2')
      page.reconciled([2])
      expect(page.wire.filter((frame) => !frame.startsWith('RECONCILE'))).toEqual(['a2'])
    })

    test('a send on a channel the server awaits keeps its place behind earlier sends when it attaches while a RECONCILE is out', () => {
      const page = connect(carriesReconcileInOpen)
      const b = page.register(1)
      page.reconcile()
      page.reconciled([])
      const c = page.register(2)
      page.reconcile()
      page.send(c, 'c1')
      page.send(b, 'b1')
      page.attachResult(1)
      page.reconciled([2])
      expect(page.wire.filter((frame) => !frame.startsWith('RECONCILE'))).toEqual(['c1', 'b1'])
    })
  })
}
