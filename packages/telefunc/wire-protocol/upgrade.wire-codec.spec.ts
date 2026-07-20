// The barrier upgrade's WIRE VOCABULARY: `PREPARE`/`READY` as encodable, decodable frames, and the
// reconcile payloads' new optional fields.
//
// ── Why round-trip equality is the right oracle ──────────────────────────────────────────────
// A frame is exactly one thing: bytes that survive the trip. Asserting on the encoded bytes would
// pin an implementation detail (JSON key order); asserting only that decode succeeds would pass on a
// codec that silently dropped `upgradeId`. Equality of the decoded payload against the input object
// is the property that actually matters to every caller downstream.

import { describe, expect, test } from 'vitest'

import {
  ProtocolViolationError,
  TAG,
  decode,
  decodeClientFrame,
  encode,
  isChannelCtrlTag,
  isConnCtrlTag,
  type PreparePayload,
  type ReadyPayload,
  type ReconcilePayload,
  type ReconciledPayload,
} from './shared-ws.js'
import { CHANNEL_TRANSPORT, UPGRADE_MAX_ID_BYTES, UPGRADE_MAX_OPEN_ENTRIES } from './constants.js'

/** The admission caps the server enforces, imported rather than restated: the "max shape"
 *  round-trips must be sized to the largest payload the protocol will ever LEGALLY carry, and a
 *  local copy would let the codec's idea of "maximum" drift silently away from the server's. */
const maxOpenList = (): PreparePayload['open'] =>
  Array.from({ length: UPGRADE_MAX_OPEN_ENTRIES }, (_, ix) => ({
    id: String(ix).padStart(UPGRADE_MAX_ID_BYTES, 'x'),
    ix,
  }))

describe('PREPARE / READY codec', () => {
  // ── NAMED MUTATION TARGET ── corrupting the JSON payload offset in `encodeJsonFrame` reddens this.
  test('round-trips a full PREPARE payload', () => {
    const payload: PreparePayload = {
      upgradeId: 'upg-1',
      sessionId: 'sess-0',
      open: [
        { id: 'A', ix: 0 },
        { id: 'B', ix: 7 },
      ],
    }
    expect(decode(encode.prepare(payload))).toEqual({ tag: TAG.PREPARE, payload })
  })

  test('round-trips a PREPARE at the maximum admissible shape, and with an empty open list', () => {
    const max: PreparePayload = { upgradeId: 'upg-1', sessionId: 'sess-0', open: maxOpenList() }
    const encoded = encode.prepare(max)
    // Well past a single TCP segment — the point is that framing is length-driven, not delimited.
    expect(encoded.byteLength).toBeGreaterThan(UPGRADE_MAX_OPEN_ENTRIES * UPGRADE_MAX_ID_BYTES)
    expect(decode(encoded)).toEqual({ tag: TAG.PREPARE, payload: max })

    const empty: PreparePayload = { upgradeId: 'upg-1', sessionId: 'sess-0', open: [] }
    const frame = decode(encode.prepare(empty))
    expect(frame).toEqual({ tag: TAG.PREPARE, payload: empty })
    // An empty array must survive as an array, not collapse to undefined via JSON.
    expect(frame.tag === TAG.PREPARE && Array.isArray(frame.payload.open)).toBe(true)
  })

  test('a PREPARE open entry carries NO cursors — exactly id and ix', () => {
    // The absence of `lastSeq` is a load-bearing design decision (PREPARE-time cursors are stale by
    // commit and would produce a credit-bypassing replay burst), so it gets a STRUCTURAL check: an
    // excess-property type error would not catch a value that arrived over the wire.
    const frame = decode(encode.prepare({ upgradeId: 'u', sessionId: 's', open: [{ id: 'A', ix: 0 }] }))
    expect(frame.tag === TAG.PREPARE && Object.keys(frame.payload.open[0]!).sort()).toEqual(['id', 'ix'])
  })

  test('READY round-trips and carries upgradeId rather than being a bare frame', () => {
    // A bare READY would be indistinguishable across concurrent/abandoned attempts; the client must
    // be able to reject a READY for an attempt it has given up on.
    const payload: ReadyPayload = { upgradeId: 'upg-9' }
    expect(decode(encode.ready(payload))).toEqual({ tag: TAG.READY, payload })
  })

  test('the new tags are 0x07 and 0x08, classify as connection ctrl, and 0x09 stays reserved', () => {
    // Both sit below DATA_TAG_MIN, so the existing range checks classify them with no new code.
    for (const tag of [TAG.PREPARE, TAG.READY]) {
      expect(isConnCtrlTag(tag)).toBe(true)
      expect(isChannelCtrlTag(tag)).toBe(false)
    }
    expect(TAG.PREPARE).toBe(0x07)
    expect(TAG.READY).toBe(0x08)
    // Control that the decoder is genuinely tag-driven: the next tag in the conn-ctrl range is still
    // unknown, so decoding it must assert rather than fall into some default frame. This is also the
    // version-skew mechanism — an OLD server hitting tag 0x07 does exactly this and kills the wire.
    const reserved = new Uint8Array(7)
    reserved[0] = 0x09
    expect(() => decode(reserved)).toThrow()
  })
})

describe('barrier fields on the reconcile payloads', () => {
  test('round-trips a barrier RECONCILE with upgradeId and fresh cursors, at both shapes', () => {
    const one: ReconcilePayload = {
      sessionId: 'sess-0',
      barrier: true,
      upgradeId: 'upg-1',
      open: [{ id: 'A', ix: 0, lastSeq: 12 }],
    }
    expect(decode(encode.reconcile(one))).toEqual({ tag: TAG.RECONCILE, payload: one })

    const max: ReconcilePayload = {
      sessionId: 'sess-0',
      barrier: true,
      upgradeId: 'upg-1',
      open: maxOpenList().map(({ id, ix }) => ({ id, ix, lastSeq: ix })),
    }
    expect(decode(encode.reconcile(max))).toEqual({ tag: TAG.RECONCILE, payload: max })
  })

  test('an ordinary RECONCILE decodes with the barrier fields ABSENT, not undefined', () => {
    // The distinction matters: the server gates on `barrier === true`, and a key that merely exists
    // with an undefined value would still be absent from `Object.keys`. This pins the wire form so a
    // future encoder change cannot start emitting `"barrier": null`.
    const frame = decode(encode.reconcile({ sessionId: 'sess-0', open: [{ id: 'A', ix: 0, lastSeq: 0 }] }))
    expect(frame.tag).toBe(TAG.RECONCILE)
    if (frame.tag !== TAG.RECONCILE) return
    expect('barrier' in frame.payload).toBe(false)
    expect('upgradeId' in frame.payload).toBe(false)
  })

  test('round-trips a RECONCILED carrying the commit upgradeId', () => {
    // `upgradeId` is what discriminates a COMMITTED from an ordinary reconciled — the entire client
    // stale-settlement guard reads this one field off the wire.
    const payload: ReconciledPayload = {
      sessionId: 'sess-1',
      open: [{ ix: 0, lastSeq: 3 }],
      reconnectTimeout: 1,
      idleTimeout: 2,
      pingInterval: 3,
      clientReplayBuffer: 4,
      clientReplayBufferBinary: 5,
      sseFlushThrottle: 6,
      ssePostIdleFlushDelay: 7,
      transports: [CHANNEL_TRANSPORT.SSE, CHANNEL_TRANSPORT.WS],
      upgradeId: 'upg-1',
    }
    expect(decode(encode.reconciled(payload))).toEqual({ tag: TAG.RECONCILED, payload })
  })
})

// `decode` only CASTS a JSON payload, so every shape claim the types make about client frames is a
// compile-time claim about untrusted bytes until this seam enforces it. Everything below is refused
// HERE, which is what lets the mux's certified code read these objects without re-checking them —
// and what makes the caller's narrowed catch sound: past this point a `TypeError` is our bug, not a
// client's frame, and the two deserve opposite responses.
describe('decodeClientFrame — the hostile-schema table', () => {
  const MAX_PREPARE_BYTES = 64 * 1024
  const clientFrame = (raw: Uint8Array<ArrayBuffer>) => decodeClientFrame(raw, MAX_PREPARE_BYTES)
  /** Shapes the typed encoders cannot express — which is exactly why only a runtime check stops them. */
  const hostile = (build: (payload: never) => Uint8Array<ArrayBuffer>, payload: unknown) => build(payload as never)

  const goodOpen = [{ id: 'A', ix: 0, lastSeq: 1 }]

  describe('the barrier legs are a flat union, not a truthiness test', () => {
    // The two malformed shapes fail in OPPOSITE directions, which is why neither a `=== true` check
    // nor a `typeof upgradeId` check alone is enough: a truthy non-`true` barrier would COMMIT an
    // upgrade, while a present-but-falsy one falls through to the ordinary path, where the stage is
    // released and the session rotated destructively.
    const rows: [name: string, payload: Record<string, unknown>][] = [
      ['barrier:false with a valid upgradeId', { sessionId: 's', barrier: false, upgradeId: 'u', open: goodOpen }],
      ['barrier:"yes" with a matching upgradeId', { sessionId: 's', barrier: 'yes', upgradeId: 'u', open: goodOpen }],
      ['barrier:1', { sessionId: 's', barrier: 1, upgradeId: 'u', open: goodOpen }],
      ['an orphaned upgradeId with no barrier leg', { sessionId: 's', upgradeId: 'u', open: goodOpen }],
      ['barrier:true with no upgradeId', { sessionId: 's', barrier: true, open: goodOpen }],
      ['barrier:true with a non-string upgradeId', { sessionId: 's', barrier: true, upgradeId: 7, open: goodOpen }],
      ['barrier:true with no sessionId', { barrier: true, upgradeId: 'u', open: goodOpen }],
      ['barrier:true with a non-string sessionId', { sessionId: 7, barrier: true, upgradeId: 'u', open: goodOpen }],
    ]
    test.each(rows)('%s is refused', (_name, payload) => {
      expect(() => clientFrame(hostile(encode.reconcile, payload))).toThrow(ProtocolViolationError)
    })

    test('control: both legal shapes pass', () => {
      // Without this the table is satisfied just as well by a seam that refuses every reconcile.
      expect(clientFrame(encode.reconcile({ sessionId: 's', open: goodOpen })).tag).toBe(TAG.RECONCILE)
      expect(clientFrame(encode.reconcile({ sessionId: 's', barrier: true, upgradeId: 'u', open: goodOpen })).tag).toBe(
        TAG.RECONCILE,
      )
    })
  })

  describe('an ordinary reconcile carries a validated open list', () => {
    const rows: [name: string, open: unknown][] = [
      ['open is not an array', 'nope'],
      ['an entry with a non-string id', [{ id: 7, ix: 0, lastSeq: 0 }]],
      ['an entry with a non-integer ix', [{ id: 'A', ix: 1.5, lastSeq: 0 }]],
      ['an entry with a negative ix', [{ id: 'A', ix: -1, lastSeq: 0 }]],
      ['an entry with a non-integer lastSeq', [{ id: 'A', ix: 0, lastSeq: 'x' }]],
      ['an entry with a negative lastSeq', [{ id: 'A', ix: 0, lastSeq: -3 }]],
      ['an entry whose initial is not literally true', [{ id: 'A', ix: 0, lastSeq: 0, initial: 'yes' }]],
      ['a null entry', [null]],
    ]
    test.each(rows)('%s is refused', (_name, open) => {
      expect(() => clientFrame(hostile(encode.reconcile, { sessionId: 's', open }))).toThrow(ProtocolViolationError)
    })

    test('control: a well-formed open list — including initial:true and an empty list — passes', () => {
      expect(clientFrame(encode.reconcile({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] })).tag).toBe(
        TAG.RECONCILE,
      )
      expect(clientFrame(encode.reconcile({ open: [] })).tag).toBe(TAG.RECONCILE)
    })

    test('a non-string sessionId is refused even with no barrier legs', () => {
      expect(() => clientFrame(hostile(encode.reconcile, { sessionId: 7, open: goodOpen }))).toThrow(
        ProtocolViolationError,
      )
    })
  })

  describe('PREPARE identity fields', () => {
    const rows: [name: string, payload: Record<string, unknown>][] = [
      ['no upgradeId', { sessionId: 's', open: [] }],
      ['an empty upgradeId', { upgradeId: '', sessionId: 's', open: [] }],
      ['a non-string upgradeId', { upgradeId: 7, sessionId: 's', open: [] }],
      ['no sessionId', { upgradeId: 'u', open: [] }],
      ['an empty sessionId', { upgradeId: 'u', sessionId: '', open: [] }],
      ['a non-string sessionId', { upgradeId: 'u', sessionId: 7, open: [] }],
      ['a non-array open', { upgradeId: 'u', sessionId: 's', open: 3 }],
    ]
    test.each(rows)('%s is refused', (_name, payload) => {
      expect(() => clientFrame(hostile(encode.prepare, payload))).toThrow(ProtocolViolationError)
    })

    test('a PREPARE over the byte cap is refused BEFORE it is parsed', () => {
      // Pre-parse, so the cap bounds what the decoder is asked to allocate rather than what survives
      // it. Driven at a cap far below the payload so the refusal cannot be confused with a shape one:
      // this payload is perfectly well-formed.
      const frame = encode.prepare({ upgradeId: 'u', sessionId: 's', open: maxOpenList() })
      expect(() => decodeClientFrame(frame, 32)).toThrow(ProtocolViolationError)
      expect(decodeClientFrame(frame, frame.byteLength).tag).toBe(TAG.PREPARE)
    })
  })

  describe('direction', () => {
    // A client sending one of these is talking a protocol we do not speak. The check lives here
    // rather than in the mux because it is a property of the frame, not of any connection's state.
    const rows: [name: string, frame: Uint8Array<ArrayBuffer>][] = [
      ['PONG', encode.pong()],
      ['FIN', encode.fin()],
      ['READY', encode.ready({ upgradeId: 'u' })],
      ['STREAM_REQUEST_OPEN_ACK', encode.streamRequestOpenAck()],
    ]
    test.each(rows)('a client-sent %s is refused', (_name, frame) => {
      expect(() => clientFrame(frame)).toThrow(ProtocolViolationError)
    })

    test('a client-sent RECONCILED is refused', () => {
      const frame = encode.reconciled({
        sessionId: 's',
        open: [],
        reconnectTimeout: 1,
        idleTimeout: 2,
        pingInterval: 3,
        clientReplayBuffer: 4,
        clientReplayBufferBinary: 5,
        sseFlushThrottle: 6,
        ssePostIdleFlushDelay: 7,
        transports: [CHANNEL_TRANSPORT.SSE],
      })
      expect(() => clientFrame(frame)).toThrow(ProtocolViolationError)
    })

    test('control: PING and an ordinary data frame pass', () => {
      expect(clientFrame(encode.ping()).tag).toBe(TAG.PING)
      expect(clientFrame(encode.text(0, '"hi"', 1)).tag).toBe(TAG.TEXT)
    })
  })

  describe('malformed bytes', () => {
    test('a truncated frame and unparseable JSON both become violations, not raw throws', () => {
      // `decode` asserts and `JSON.parse` throws — two different error classes that the mux must not
      // have to tell apart. Wrapping them here is what leaves exactly one error class at the seam.
      expect(() => clientFrame(new Uint8Array(2) as Uint8Array<ArrayBuffer>)).toThrow(ProtocolViolationError)
      const junk = encode.text(0, 'not json', 1)
      junk[0] = TAG.RECONCILE
      expect(() => clientFrame(junk)).toThrow(ProtocolViolationError)
    })

    test('an unknown tag is a violation', () => {
      const frame = encode.ping()
      frame[0] = 0x7f
      expect(() => clientFrame(frame)).toThrow(ProtocolViolationError)
    })
  })
})
