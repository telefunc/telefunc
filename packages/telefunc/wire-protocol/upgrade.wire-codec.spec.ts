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
  TAG,
  decode,
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
