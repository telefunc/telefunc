// The upgrade's wire vocabulary, and the totality of the client→server decode seam.
//
// `decode` only CASTS a JSON payload, so every shape claim the types make about client frames is a
// compile-time claim about untrusted bytes until `decodeClientFrame` enforces it. Refusing here is
// what lets the mux read these objects without re-checking them, and what makes its narrowed catch
// sound: past this seam a TypeError is our bug, not a client's frame. Interleavings are the browser
// e2e's job — this file covers only what the e2e cannot construct.

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
/** Shapes the typed encoders cannot express — which is why only a runtime check stops them. */
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
  // Round-trip equality, not encoded bytes: bytes would pin JSON key order, and "decode succeeded"
  // would pass on a codec that silently dropped `upgradeId`.
  test('PREPARE and READY round-trip', () => {
    const prepare = { upgradeId: 'upg-1', sessionId: 'sess-0' }
    expect(decode(encode.prepare(prepare))).toEqual({ tag: TAG.PREPARE, payload: prepare })
    // READY carries upgradeId rather than being bare: a bare READY is indistinguishable across
    // concurrent attempts, so the client could not reject one for an attempt it had abandoned.
    expect(decode(encode.ready({ upgradeId: 'upg-9' }))).toEqual({ tag: TAG.READY, payload: { upgradeId: 'upg-9' } })
  })

  test('the new tags are connection ctrl and 0x09 stays reserved', () => {
    for (const tag of [TAG.PREPARE, TAG.READY]) {
      expect(isConnCtrlTag(tag)).toBe(true)
      expect(isChannelCtrlTag(tag)).toBe(false)
    }
    expect([TAG.PREPARE, TAG.READY]).toEqual([0x07, 0x08])
    // Control that the decoder is genuinely tag-driven — and the version-skew mechanism: an OLD
    // server hitting 0x07 does exactly this and kills the wire.
    const reserved = new Uint8Array(7)
    reserved[0] = 0x09
    expect(() => decode(reserved)).toThrow()
  })

  test('a barrier RECONCILE round-trips at one entry and at the admission cap', () => {
    const one: ReconcilePayload = { sessionId: 'sess-0', barrier: true, upgradeId: 'upg-1', open: goodOpen }
    expect(decode(encode.reconcile(one))).toEqual({ tag: TAG.RECONCILE, payload: one })
    // Sized from the server's own caps, so the codec's idea of "maximum" cannot drift from it.
    const open = Array.from({ length: UPGRADE_MAX_OPEN_ENTRIES }, (_, ix) => ({
      id: String(ix).padStart(UPGRADE_MAX_ID_BYTES, 'x'),
      ix,
      lastSeq: ix,
    }))
    const max: ReconcilePayload = { sessionId: 'sess-0', barrier: true, upgradeId: 'upg-1', open }
    const encoded = encode.reconcile(max)
    // Well past one TCP segment — the point is that framing is length-driven, not delimited.
    expect(encoded.byteLength).toBeGreaterThan(UPGRADE_MAX_OPEN_ENTRIES * UPGRADE_MAX_ID_BYTES)
    expect(decode(encoded)).toEqual({ tag: TAG.RECONCILE, payload: max })
  })

  test('an ordinary RECONCILE decodes with the barrier fields ABSENT, not undefined', () => {
    // The server gates on `barrier === true`, and a key present with an undefined value would still
    // be absent from `Object.keys` — this pins the wire form against an encoder that starts
    // emitting `"barrier": null`.
    const frame = decode(encode.reconcile({ sessionId: 'sess-0', open: goodOpen }))
    expect(frame.tag).toBe(TAG.RECONCILE)
    if (frame.tag !== TAG.RECONCILE) return
    expect('barrier' in frame.payload).toBe(false)
    expect('upgradeId' in frame.payload).toBe(false)
  })

  test('a RECONCILED round-trips the commit upgradeId', () => {
    // The entire client stale-settlement guard reads this one field off the wire.
    const payload = reconciled({ open: [{ ix: 0, lastSeq: 3 }], upgradeId: 'upg-1' })
    expect(decode(encode.reconciled(payload))).toEqual({ tag: TAG.RECONCILED, payload })
  })
})

describe('decodeClientFrame — hostile schemas', () => {
  // The malformed barrier shapes fail in OPPOSITE directions, so neither a `=== true` check nor a
  // `typeof upgradeId` check alone is enough: a truthy non-`true` barrier would COMMIT an upgrade,
  // while a present-but-falsy one falls through to the ordinary path, which rotates the session
  // destructively.
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
    ['an entry with a non-integer lastSeq', { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: 'x' }] }],
    ['an entry with a negative lastSeq', { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: -3 }] }],
    // `lastSeq` present and valid, so this row can only be refused for the `initial` field.
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
    // Without this the table above is satisfied by a seam that refuses every reconcile.
    const legal: ReconcilePayload[] = [
      { sessionId: 's', open: goodOpen },
      { sessionId: 's', barrier: true, upgradeId: 'u', open: goodOpen },
      { open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] },
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
    // Pre-parse, so the cap bounds what the decoder is asked to allocate rather than what survives
    // it. The payload is well-formed, so the refusal cannot be confused with a shape one.
    const frame = encode.prepare({ upgradeId: 'u'.repeat(4_096), sessionId: 's' })
    expect(() => decodeClientFrame(frame, 32)).toThrow(ProtocolViolationError)
    expect(decodeClientFrame(frame, frame.byteLength).tag).toBe(TAG.PREPARE)
  })

  // `decode` CASTS the parsed JSON, so a non-object arrives typed as the payload it is not and every
  // field check dereferences it — a raw TypeError, which the mux rethrew as a telefunc bug while
  // leaving the offending wire alive.
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
    // `decode` asserts and `JSON.parse` throws — two error classes the mux must not tell apart.
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
  // Stated as an ALLOWLIST in the code, so the two tables must be read together: a denylist admits
  // by omission, and the rows one missed each reached a different downstream failure —
  // PUBLISH/PUBLISH_BINARY an `assert(false)` filed as our bug, ABORT/ERROR a ctrl switch with no
  // case, which accepted and silently dropped them.
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

  // The half a refusal table cannot supply: an allowlist that dropped a legal tag would break real
  // clients while every row above stayed green. Enumerated rather than sampled, so a tag added
  // without a direction decision fails HERE, at a table naming it, not downstream at runtime.
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
