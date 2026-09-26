import { describe, expect, test } from 'vitest'

import {
  ProtocolViolationError,
  TAG,
  decode,
  decodeClientFrame,
  encode,
  encodePublishBinary,
  isChannelCtrlTag,
  isConnCtrlTag,
  type BarrierPayload,
  type ReconcilePayload,
  type ReconciledPayload,
} from './shared-ws.js'
import {
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  UPGRADE_MAX_ID_BYTES,
  WIRE_MAX_CONN_CTRL_FRAME_BYTES,
} from './constants.js'

const clientFrame = (raw: Uint8Array<ArrayBuffer>) => decodeClientFrame(raw, 64 * 1024)
const hostile = (build: (payload: never) => Uint8Array<ArrayBuffer>, payload: unknown) => build(payload as never)
const goodOpen = [{ id: 'A', ix: 0, lastSeq: 1 }]
const reconciled = (extra: Partial<ReconciledPayload> = {}): ReconciledPayload => ({
  sessionId: 's',
  open: [],
  reconnectTimeout: 1,
  idleTimeout: 2,
  pingInterval: 3,
  clientReplayBuffer: 4,
  clientReplayBufferBinary: 5,
  sseFlushThrottle: 6,
  ssePostIdleFlushDelay: 7,
  transports: [CHANNEL_TRANSPORT.SSE, CHANNEL_TRANSPORT.WS],
  ...extra,
})

describe('upgrade wire vocabulary', () => {
  test('PREPARE and READY round-trip', () => {
    const prepare = { upgradeId: 'upg-1', sessionId: 'sess-0' }
    expect(decode(encode.prepare(prepare))).toEqual({ tag: TAG.PREPARE, payload: prepare })
    expect(decode(encode.ready({ upgradeId: 'upg-9' }))).toEqual({ tag: TAG.READY, payload: { upgradeId: 'upg-9' } })
  })

  test('the new tags are connection ctrl and 0x0a stays reserved', () => {
    for (const tag of [TAG.PREPARE, TAG.READY, TAG.BARRIER]) {
      expect(isConnCtrlTag(tag)).toBe(true)
      expect(isChannelCtrlTag(tag)).toBe(false)
    }
    expect([TAG.PREPARE, TAG.READY, TAG.BARRIER]).toEqual([0x07, 0x08, 0x09])
    const reserved = new Uint8Array(7)
    reserved[0] = 0x0a
    expect(() => decode(reserved)).toThrow()
  })

  test('a BARRIER round-trips at one entry, and it and a RECONCILE at the largest shape the caps admit', () => {
    const one: BarrierPayload = { sessionId: 'sess-0', upgradeId: 'upg-1', open: goodOpen }
    expect(decode(encode.barrier(one))).toEqual({ tag: TAG.BARRIER, payload: one })
    const open = Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, ix) => ({
      id: String(ix).padStart(UPGRADE_MAX_ID_BYTES, 'x'),
      ix: 0xffff - ix,
      lastSeq: 0xffffffff,
      initial: true as const,
      broadcast: { text: false, binary: false },
    }))
    const max: BarrierPayload = { sessionId: 'x'.repeat(64), upgradeId: 'y'.repeat(64), open }
    const encoded = encode.barrier(max)
    // The byte cap is derived from the entry caps precisely so this frame is admissible: a cap
    // that refuses the largest legal barrier would fail every client that hit the entry cap.
    expect(encoded.byteLength).toBeGreaterThan(MAX_CHANNELS_PER_CONNECTION * UPGRADE_MAX_ID_BYTES)
    expect(encoded.byteLength).toBeLessThanOrEqual(WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    expect(decodeClientFrame(encoded, WIRE_MAX_CONN_CTRL_FRAME_BYTES)).toEqual({ tag: TAG.BARRIER, payload: max })
    const reconcile: ReconcilePayload = { sessionId: max.sessionId, open }
    const decoded = decodeClientFrame(encode.reconcile(reconcile), WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    expect(decoded).toEqual({ tag: TAG.RECONCILE, payload: reconcile })
  })

  test('a RECONCILED round-trips the commit upgradeId', () => {
    const payload = reconciled({ open: [{ ix: 0, lastSeq: 3 }], upgradeId: 'upg-1' })
    expect(decode(encode.reconciled(payload))).toEqual({ tag: TAG.RECONCILED, payload })
  })
})

describe('decodeClientFrame — hostile schemas', () => {
  const badReconcile: [string, Record<string, unknown>][] = [
    ['a non-string sessionId', { sessionId: 7, open: goodOpen }],
    ['open that is not an array', { sessionId: 's', open: 'nope' }],
    ['an entry with a non-string id', { sessionId: 's', open: [{ id: 7, ix: 0, lastSeq: 0 }] }],
    ['an entry with a non-integer ix', { sessionId: 's', open: [{ id: 'A', ix: 1.5, lastSeq: 0 }] }],
    ['an entry with a negative ix', { sessionId: 's', open: [{ id: 'A', ix: -1, lastSeq: 0 }] }],
    ['an entry with an overflowing ix', { sessionId: 's', open: [{ id: 'A', ix: 0x10000, lastSeq: 0 }] }],
    ['duplicate ix entries', { sessionId: 's', open: [...goodOpen, { id: 'B', ix: goodOpen[0]!.ix, lastSeq: 0 }] }],
    ['an entry with a non-integer lastSeq', { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: 'x' }] }],
    ['an entry with a negative lastSeq', { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: -3 }] }],
    ['an entry with an overflowing lastSeq', { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: 0x100000000 }] }],
    [
      'an entry whose initial is not literally true',
      { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: 0, initial: 'yes' }] },
    ],
    ['a null entry', { sessionId: 's', open: [null] }],
  ]
  test.each(badReconcile)('a RECONCILE with %s is refused', (_name, payload) => {
    expect(() => clientFrame(hostile(encode.reconcile, payload))).toThrow(ProtocolViolationError)
  })

  test('control: every legal RECONCILE shape passes', () => {
    const legal: ReconcilePayload[] = [
      { sessionId: 's', open: goodOpen },
      { open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] },
      { open: [{ id: 'A', ix: 0xffff, lastSeq: 0xffffffff }] },
      { open: [] },
    ]
    for (const payload of legal) expect(clientFrame(encode.reconcile(payload)).tag).toBe(TAG.RECONCILE)
  })

  const badBarrier: [string, Record<string, unknown>][] = [
    ['no sessionId', { upgradeId: 'u', open: goodOpen }],
    ['an empty sessionId', { sessionId: '', upgradeId: 'u', open: goodOpen }],
    ['a non-string sessionId', { sessionId: 7, upgradeId: 'u', open: goodOpen }],
    ['no upgradeId', { sessionId: 's', open: goodOpen }],
    ['an empty upgradeId', { sessionId: 's', upgradeId: '', open: goodOpen }],
    ['a non-string upgradeId', { sessionId: 's', upgradeId: 7, open: goodOpen }],
    ['open that is not an array', { sessionId: 's', upgradeId: 'u', open: 'nope' }],
    [
      'an entry with an overflowing ix',
      { sessionId: 's', upgradeId: 'u', open: [{ id: 'A', ix: 0x10000, lastSeq: 0 }] },
    ],
  ]
  test.each(badBarrier)('a BARRIER with %s is refused', (_name, payload) => {
    expect(() => clientFrame(hostile(encode.barrier, payload))).toThrow(ProtocolViolationError)
  })

  test('a BARRIER over the byte cap is refused BEFORE it is parsed', () => {
    // Payload is zero bytes — not JSON. If the cap were checked after `decode`, the failure would
    // be the parser's ('payload is not JSON'); naming the cap proves nothing parsed it.
    const oversize = new Uint8Array(WIRE_MAX_CONN_CTRL_FRAME_BYTES + 1) as Uint8Array<ArrayBuffer>
    oversize[0] = TAG.BARRIER
    expect(() => decodeClientFrame(oversize, WIRE_MAX_CONN_CTRL_FRAME_BYTES)).toThrow('upgrade frame over byte cap')

    const legal = encode.barrier({ sessionId: 's', upgradeId: 'u', open: goodOpen })
    expect(decodeClientFrame(legal, WIRE_MAX_CONN_CTRL_FRAME_BYTES).tag).toBe(TAG.BARRIER)
  })

  const nonObjects: [string, unknown][] = [
    ['null', null],
    ['a bare string', 'nope'],
    ['a number', 7],
    ['an array', []],
  ]
  test.each(nonObjects)('a PREPARE payload that is %s is a violation, not a TypeError', (_name, payload) => {
    expect(() => clientFrame(hostile(encode.prepare, payload))).toThrow(ProtocolViolationError)
  })
  test.each(nonObjects)('a RECONCILE payload that is %s is a violation, not a TypeError', (_name, payload) => {
    expect(() => clientFrame(hostile(encode.reconcile, payload))).toThrow(ProtocolViolationError)
  })

  test("a RECONCILE entry's broadcast subscriptions must be two booleans", () => {
    const entry = { id: 'A', ix: 0, lastSeq: 0 }
    const legal = encode.reconcile({ open: [{ ...entry, broadcast: { text: true, binary: false } }] })
    expect(clientFrame(legal)).toMatchObject({ payload: { open: [{ broadcast: { text: true, binary: false } }] } })
    for (const broadcast of [null, true, { text: true }, { text: 'yes', binary: false }]) {
      expect(() => clientFrame(hostile(encode.reconcile, { open: [{ ...entry, broadcast }] }))).toThrow(
        ProtocolViolationError,
      )
    }
  })

  test('truncated bytes, unparsable JSON and an unknown tag are all violations', () => {
    expect(() => clientFrame(new Uint8Array(2) as Uint8Array<ArrayBuffer>)).toThrow(ProtocolViolationError)
    const junk = encode.text(0, 'not json', 1)
    junk[0] = TAG.RECONCILE
    expect(() => clientFrame(junk)).toThrow(ProtocolViolationError)
    const unknown = encode.ping()
    unknown[0] = 0x7f
    expect(() => clientFrame(unknown)).toThrow(ProtocolViolationError)
  })
})

describe('decodeClientFrame — direction', () => {
  const serverOnly: [string, Uint8Array<ArrayBuffer>][] = [
    ['PONG', encode.pong()],
    ['FIN', encode.fin()],
    ['READY', encode.ready({ upgradeId: 'u' })],
    ['STREAM_REQUEST_OPEN_ACK', encode.streamRequestOpenAck()],
    ['PUBLISH', encode.publish(0, `9,1700000000000\n${JSON.stringify(1)}`, 1)],
    ['PUBLISH_BINARY', encode.publishBinary(0, new Uint8Array(14), 1)],
    ['ABORT', encode.abort(0, JSON.stringify('nope'))],
    ['ERROR', encode.error(0)],
    ['RECONCILED', encode.reconciled(reconciled())],
  ]
  test.each(serverOnly)('a client-sent %s is refused', (_name, frame) => {
    expect(() => clientFrame(frame)).toThrow(ProtocolViolationError)
  })

  test('a server-only frame is refused before its payload is parsed', () => {
    const publish = encodePublishBinary(new Uint8Array(), { seq: 1, timestamp: 1 })
    const truncatedOrdering = encode.publishBinary(0, publish.subarray(0, publish.byteLength - 1), 1)
    expect(() => clientFrame(truncatedOrdering)).toThrow(ProtocolViolationError)
  })

  const clientLegal: [string, Uint8Array<ArrayBuffer>][] = [
    ['PING', encode.ping()],
    ['RECONCILE', encode.reconcile({ open: goodOpen })],
    ['PREPARE', encode.prepare({ upgradeId: 'u', sessionId: 's' })],
    ['BARRIER', encode.barrier({ upgradeId: 'u', sessionId: 's', open: goodOpen })],
    ['TEXT', encode.text(0, '"hi"', 1)],
    ['BINARY', encode.binary(0, new Uint8Array([1]), 1)],
    ['TEXT_ACK_REQ', encode.textAckReq(0, '"hi"', 1)],
    ['BINARY_ACK_REQ', encode.binaryAckReq(0, new Uint8Array([1]), 1)],
    ['ACK_RES', encode.ackRes(0, 1, 1, '"ok"')],
    ['PUBLISH_ACK_REQ', encode.publishAckReq(0, '"hi"', 1)],
    ['PUBLISH_BINARY_ACK_REQ', encode.publishBinaryAckReq(0, new Uint8Array([1]), 1)],
    ['CLOSE', encode.close(0, 1_000)],
    ['CLOSE_ACK', encode.closeAck(0)],
    ['WINDOW', encode.window(0, 1_024)],
    ['MSG_WINDOW', encode.msgWindow(0, 8)],
    ['BDP_PING', encode.bdpPing(0)],
    ['BDP_PING_ACK', encode.bdpPingAck(0)],
    ['BROADCAST_SUB', encode.broadcastSub(0, false)],
    ['BROADCAST_UNSUB', encode.broadcastUnsub(0, false)],
  ]
  test.each(clientLegal)('control: a client-sent %s passes', (_name, frame) => {
    expect(clientFrame(frame).tag).toBe(frame[0])
  })
})
