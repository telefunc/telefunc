import { describe, expect, test } from 'vitest'

import {
  ProtocolViolationError,
  TAG,
  decode,
  decodeClientFrame,
  encode,
  isChannelCtrlTag,
  isConnCtrlTag,
  type ReconcilePayload,
  type ReconciledPayload,
} from './shared-ws.js'
import { CHANNEL_TRANSPORT, UPGRADE_MAX_ID_BYTES, UPGRADE_MAX_OPEN_ENTRIES } from './constants.js'

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

  test('the new tags are connection ctrl and 0x09 stays reserved', () => {
    for (const tag of [TAG.PREPARE, TAG.READY]) {
      expect(isConnCtrlTag(tag)).toBe(true)
      expect(isChannelCtrlTag(tag)).toBe(false)
    }
    expect([TAG.PREPARE, TAG.READY]).toEqual([0x07, 0x08])
    const reserved = new Uint8Array(7)
    reserved[0] = 0x09
    expect(() => decode(reserved)).toThrow()
  })

  test('a barrier RECONCILE round-trips at one entry and at the admission cap', () => {
    const one: ReconcilePayload = { sessionId: 'sess-0', barrier: true, upgradeId: 'upg-1', open: goodOpen }
    expect(decode(encode.reconcile(one))).toEqual({ tag: TAG.RECONCILE, payload: one })
    const open = Array.from({ length: UPGRADE_MAX_OPEN_ENTRIES }, (_, ix) => ({
      id: String(ix).padStart(UPGRADE_MAX_ID_BYTES, 'x'),
      ix,
      lastSeq: ix,
    }))
    const max: ReconcilePayload = { sessionId: 'sess-0', barrier: true, upgradeId: 'upg-1', open }
    const encoded = encode.reconcile(max)
    expect(encoded.byteLength).toBeGreaterThan(UPGRADE_MAX_OPEN_ENTRIES * UPGRADE_MAX_ID_BYTES)
    expect(decode(encoded)).toEqual({ tag: TAG.RECONCILE, payload: max })
  })

  test('an ordinary RECONCILE decodes with the barrier fields ABSENT, not undefined', () => {
    const frame = decode(encode.reconcile({ sessionId: 'sess-0', open: goodOpen }))
    expect(frame.tag).toBe(TAG.RECONCILE)
    if (frame.tag !== TAG.RECONCILE) return
    expect('barrier' in frame.payload).toBe(false)
    expect('upgradeId' in frame.payload).toBe(false)
  })

  test('a RECONCILED round-trips the commit upgradeId', () => {
    const payload = reconciled({ open: [{ ix: 0, lastSeq: 3 }], upgradeId: 'upg-1' })
    expect(decode(encode.reconciled(payload))).toEqual({ tag: TAG.RECONCILED, payload })
  })
})

describe('decodeClientFrame — hostile schemas', () => {
  const badReconcile: [string, Record<string, unknown>][] = [
    ['barrier:false with a valid upgradeId', { sessionId: 's', barrier: false, upgradeId: 'u', open: goodOpen }],
    ['barrier:"yes"', { sessionId: 's', barrier: 'yes', upgradeId: 'u', open: goodOpen }],
    ['barrier:1', { sessionId: 's', barrier: 1, upgradeId: 'u', open: goodOpen }],
    ['an orphaned upgradeId and no barrier leg', { sessionId: 's', upgradeId: 'u', open: goodOpen }],
    ['barrier:true and no upgradeId', { sessionId: 's', barrier: true, open: goodOpen }],
    ['barrier:true and a non-string upgradeId', { sessionId: 's', barrier: true, upgradeId: 7, open: goodOpen }],
    ['barrier:true and no sessionId', { barrier: true, upgradeId: 'u', open: goodOpen }],
    ['barrier:true and a non-string sessionId', { sessionId: 7, barrier: true, upgradeId: 'u', open: goodOpen }],
    ['a non-string sessionId and no barrier legs', { sessionId: 7, open: goodOpen }],
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
      { sessionId: 's', barrier: true, upgradeId: 'u', open: goodOpen },
      { open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] },
      { open: [{ id: 'A', ix: 0xffff, lastSeq: 0xffffffff }] },
      { open: [] },
    ]
    for (const payload of legal) expect(clientFrame(encode.reconcile(payload)).tag).toBe(TAG.RECONCILE)
  })

  const badPrepare: [string, Record<string, unknown>][] = [
    ['no upgradeId', { sessionId: 's' }],
    ['an empty upgradeId', { upgradeId: '', sessionId: 's' }],
    ['a non-string upgradeId', { upgradeId: 7, sessionId: 's' }],
    ['no sessionId', { upgradeId: 'u' }],
    ['an empty sessionId', { upgradeId: 'u', sessionId: '' }],
    ['a non-string sessionId', { upgradeId: 'u', sessionId: 7 }],
  ]
  test.each(badPrepare)('a PREPARE with %s is refused', (_name, payload) => {
    expect(() => clientFrame(hostile(encode.prepare, payload))).toThrow(ProtocolViolationError)
  })

  test('a PREPARE over the byte cap is refused BEFORE it is parsed', () => {
    const frame = encode.prepare({ upgradeId: 'u'.repeat(4_096), sessionId: 's' })
    expect(() => decodeClientFrame(frame, 32)).toThrow(ProtocolViolationError)
    expect(decodeClientFrame(frame, frame.byteLength).tag).toBe(TAG.PREPARE)
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

  const clientLegal: [string, Uint8Array<ArrayBuffer>][] = [
    ['PING', encode.ping()],
    ['RECONCILE', encode.reconcile({ open: goodOpen })],
    ['PREPARE', encode.prepare({ upgradeId: 'u', sessionId: 's' })],
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
