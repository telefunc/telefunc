// T3 — the barrier-commit upgrade's WIRE VOCABULARY, and nothing else.
//
// This layer is deliberately inert: `PREPARE`/`READY` exist as encodable, decodable frames and
// the two reconcile payloads carry their new optional fields, but no production path emits a
// single one of them yet (staging, validation and emission are T4/T5). So the tests here are
// codec tests — encode→decode identity — plus two guards that the additions changed nothing:
//
//   1. A `barrier: true` RECONCILE still routes through the UNCHANGED server dispatch to the
//      ordinary `reconcile` path. The barrier flag is a field the server does not read yet;
//      if adding it altered routing, the whole "behavior-inert" premise would be false.
//   2. `barrierUpgrade: true` rides along on every RECONCILED. It is the one thing T3 puts on
//      the wire for real, and it is only safe because neither payload is schema-validated.
//
// ── Why round-trip equality is the right oracle here ─────────────────────────────────────────
// A frame is exactly one thing: bytes that survive the trip. Asserting on the encoded bytes
// would pin an implementation detail (JSON key order), and asserting only that decode succeeds
// would pass on a codec that silently dropped `upgradeId`. Equality of the decoded payload
// against the input object is the property that actually matters to every caller downstream.

import { afterEach, describe, expect, test } from 'vitest'

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
import { createMuxHarness, reconcileFrame, settle } from './upgrade-mux-harness.js'

let harness: ReturnType<typeof createMuxHarness> | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

// ===== Codec =====

/** The admission caps T4 enforces, imported rather than restated: the "max shape" round-trips
 *  below must be sized to the largest payload the protocol will ever LEGALLY carry, and a local
 *  copy would let the codec's idea of "maximum" drift silently away from the server's. */
const MAX_OPEN_ENTRIES = UPGRADE_MAX_OPEN_ENTRIES
const MAX_ID_BYTES = UPGRADE_MAX_ID_BYTES

const maxOpenList = (): PreparePayload['open'] =>
  Array.from({ length: MAX_OPEN_ENTRIES }, (_, ix) => ({ id: String(ix).padStart(MAX_ID_BYTES, 'x'), ix }))

describe('PREPARE codec', () => {
  // ── NAMED MUTATION TARGET ──
  // Corrupting the JSON payload offset in `encodeJsonFrame` must turn THIS test red.
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

  test('round-trips a PREPARE with an empty open list', () => {
    const payload: PreparePayload = { upgradeId: 'upg-1', sessionId: 'sess-0', open: [] }
    const frame = decode(encode.prepare(payload))
    expect(frame).toEqual({ tag: TAG.PREPARE, payload })
    // An empty array must survive as an array, not collapse to undefined via JSON.
    expect(frame.tag === TAG.PREPARE && Array.isArray(frame.payload.open)).toBe(true)
  })

  test('round-trips a PREPARE at the maximum admissible shape', () => {
    const payload: PreparePayload = { upgradeId: 'upg-1', sessionId: 'sess-0', open: maxOpenList() }
    const encoded = encode.prepare(payload)
    // Well past a single TCP segment — the point is that framing is length-driven, not delimited.
    expect(encoded.byteLength).toBeGreaterThan(MAX_OPEN_ENTRIES * MAX_ID_BYTES)
    expect(decode(encoded)).toEqual({ tag: TAG.PREPARE, payload })
  })

  test('carries NO cursors — a PREPARE open entry has exactly id and ix', () => {
    // The absence of `lastSeq` is a load-bearing design decision (PREPARE-time cursors are stale
    // by commit and would produce a credit-bypassing replay burst), so it gets an assertion
    // rather than only a type. A structural check, because an excess-property type error would
    // not catch a value that arrived over the wire.
    const frame = decode(encode.prepare({ upgradeId: 'u', sessionId: 's', open: [{ id: 'A', ix: 0 }] }))
    expect(frame.tag).toBe(TAG.PREPARE)
    expect(frame.tag === TAG.PREPARE && Object.keys(frame.payload.open[0]!).sort()).toEqual(['id', 'ix'])
  })
})

describe('READY codec', () => {
  test('round-trips a READY payload', () => {
    const payload: ReadyPayload = { upgradeId: 'upg-1' }
    expect(decode(encode.ready(payload))).toEqual({ tag: TAG.READY, payload })
  })

  test('READY carries upgradeId rather than being a bare frame', () => {
    // A bare READY would be indistinguishable across concurrent/abandoned attempts; the client
    // must be able to reject a READY for an attempt it has given up on.
    const frame = decode(encode.ready({ upgradeId: 'upg-9' }))
    expect(frame.tag === TAG.READY && frame.payload.upgradeId).toBe('upg-9')
  })
})

describe('tag classification', () => {
  test('PREPARE and READY are connection-level ctrl tags for free', () => {
    // Both sit below DATA_TAG_MIN, so the existing range checks classify them with no new code.
    for (const tag of [TAG.PREPARE, TAG.READY]) {
      expect(isConnCtrlTag(tag)).toBe(true)
      expect(isChannelCtrlTag(tag)).toBe(false)
    }
  })

  test('the new tags are 0x07 and 0x08, and 0x09 stays reserved', () => {
    expect(TAG.PREPARE).toBe(0x07)
    expect(TAG.READY).toBe(0x08)
    // Control that the decoder is genuinely tag-driven: the next tag in the conn-ctrl range is
    // still unknown, so a decode of it must assert rather than fall into some default frame.
    const reserved = new Uint8Array(7)
    reserved[0] = 0x09
    expect(() => decode(reserved)).toThrow()
  })
})

describe('barrier fields on the reconcile payloads', () => {
  test('round-trips a barrier RECONCILE with upgradeId and fresh cursors', () => {
    const payload: ReconcilePayload = {
      sessionId: 'sess-0',
      upgrade: true,
      barrier: true,
      upgradeId: 'upg-1',
      open: [{ id: 'A', ix: 0, lastSeq: 12 }],
    }
    expect(decode(encode.reconcile(payload))).toEqual({ tag: TAG.RECONCILE, payload })
  })

  test('round-trips a barrier RECONCILE at the maximum admissible shape', () => {
    const payload: ReconcilePayload = {
      sessionId: 'sess-0',
      barrier: true,
      upgradeId: 'upg-1',
      open: maxOpenList().map(({ id, ix }) => ({ id, ix, lastSeq: ix })),
    }
    expect(decode(encode.reconcile(payload))).toEqual({ tag: TAG.RECONCILE, payload })
  })

  test('an ordinary RECONCILE decodes with the barrier fields ABSENT, not undefined', () => {
    // The distinction matters: T4 gates on `barrier === true`, and a key that merely exists with
    // an undefined value would still be absent from `Object.keys` — this pins the wire form so a
    // future encoder change cannot start emitting `"barrier": null`.
    const frame = decode(encode.reconcile({ sessionId: 'sess-0', open: [{ id: 'A', ix: 0, lastSeq: 0 }] }))
    expect(frame.tag).toBe(TAG.RECONCILE)
    if (frame.tag !== TAG.RECONCILE) return
    expect('barrier' in frame.payload).toBe(false)
    expect('upgradeId' in frame.payload).toBe(false)
  })

  test('round-trips a RECONCILED carrying upgradeId and barrierUpgrade', () => {
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
      barrierUpgrade: true,
    }
    expect(decode(encode.reconciled(payload))).toEqual({ tag: TAG.RECONCILED, payload })
  })
})

// ===== Behavior-inertness =====

describe('the additions change no server behavior', () => {
  /** Bring an SSE wire up exactly as the existing specs do, then return its session id. */
  async function connectSse(h: ReturnType<typeof createMuxHarness>): Promise<string> {
    h.registerChannel('A')
    await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
    await settle()
    const sessionId = h.sse.sessionId()
    expect(sessionId).toBeTruthy()
    return sessionId as string
  }

  test('the server advertises barrierUpgrade: true on RECONCILED', async () => {
    const h = (harness = createMuxHarness())
    await connectSse(h)
    const reconciled = h.sse.sent.find((f) => f.tag === TAG.RECONCILED)
    expect(reconciled?.tag === TAG.RECONCILED && reconciled.payload.barrierUpgrade).toBe(true)
  })

  // ── UPDATED BY T4 ── This test previously asserted the opposite: that a `barrier: true` reconcile
  // still routed through the unchanged dispatch to the ordinary `reconcile`, because T3 added the
  // FIELD and not its handling. T4 adds the handling, so the frame is now routed to the barrier
  // commit path — and a barrier naming no staged upgrade is a protocol violation rather than a
  // free destructive rotation. That inversion is the point of the flag, and it is asserted here so
  // the codec spec cannot silently claim inertness the server no longer has.
  test('a barrier:true RECONCILE is now routed to the commit path, not the ordinary reconcile', async () => {
    const h = (harness = createMuxHarness())
    const s0 = await connectSse(h)

    await h.sse.deliver(
      reconcileFrame({ sessionId: s0, barrier: true, upgradeId: 'upg-1', open: [{ id: 'A', ix: 0, lastSeq: 0 }] }),
    )
    await settle()

    expect(h.sse.terminated()).toBe(true)
    // Refused BEFORE any side effect: no second reconciled, no rotation.
    expect(h.sse.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)
    expect(h.sse.sessionId()).toBe(s0)
  })

  test('control: the identical reconcile WITHOUT the barrier fields is still an ordinary reconcile', async () => {
    // The discriminator for the test above, and now load-bearing: it proves the rejection is caused
    // by the `barrier` flag specifically and not by anything else in the frame. Byte-for-byte the
    // same reconcile minus that one field still rotates the wire exactly as it always has.
    const h = (harness = createMuxHarness())
    const s0 = await connectSse(h)

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 0 }] }))
    await settle()

    expect(h.sse.terminated()).toBe(false)
    const answers = h.sse.sent.filter((f) => f.tag === TAG.RECONCILED)
    expect(answers).toHaveLength(2)
    const settled = answers[1]
    expect(settled?.tag === TAG.RECONCILED && settled.payload.open).toEqual([{ ix: 0, lastSeq: 0 }])
    expect(h.sse.sessionId()).not.toBe(s0)
  })
})
