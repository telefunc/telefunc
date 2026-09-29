import { describe, expect, test } from 'vitest'

import {
  ERROR_REASON,
  ProtocolViolationError,
  TAG,
  decode,
  decodeClientFrame,
  encode,
  encodePublishBinary,
  encodePublishText,
  isChannelCtrlTag,
  isConnCtrlTag,
  type BarrierPayload,
  type ReconcilePayload,
  type ReconciledPayload,
  type SeqReader,
} from './shared-ws.js'
import {
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  UPGRADE_MAX_ID_BYTES,
  WIRE_MAX_CONN_CTRL_FRAME_BYTES,
} from './constants.js'

/** A receiver with nothing of any channel: each seq reads as its low 32 bits. */
const wireSeqs: SeqReader = { received: () => 0, sent: () => 0 }

const clientFrame = (raw: Uint8Array<ArrayBuffer>) => decodeClientFrame(raw, 64 * 1024, wireSeqs)
const hostile = (build: (payload: never) => Uint8Array<ArrayBuffer>, payload: unknown) => build(payload as never)
const goodOpen = [{ id: 'A', ix: 0, lastSeq: 1 }]
const reconciled = (extra: Partial<ReconciledPayload> = {}): ReconciledPayload => ({
  sessionId: 's',
  open: [],
  reconnectTimeout: 1,
  idleTimeout: 2,
  pingInterval: 3,
  serverReplayBuffer: 8,
  serverReplayBufferBinary: 9,
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
    expect(decode(encode.prepare(prepare), wireSeqs)).toEqual({ tag: TAG.PREPARE, payload: prepare })
    expect(decode(encode.ready({ upgradeId: 'upg-9' }), wireSeqs)).toEqual({
      tag: TAG.READY,
      payload: { upgradeId: 'upg-9' },
    })
  })

  test('the new tags are connection ctrl and 0x0a stays reserved', () => {
    for (const tag of [TAG.PREPARE, TAG.READY, TAG.BARRIER]) {
      expect(isConnCtrlTag(tag)).toBe(true)
      expect(isChannelCtrlTag(tag)).toBe(false)
    }
    expect([TAG.PREPARE, TAG.READY, TAG.BARRIER]).toEqual([0x07, 0x08, 0x09])
    const reserved = new Uint8Array(7)
    reserved[0] = 0x0a
    expect(() => decode(reserved, wireSeqs)).toThrow()
  })

  test('every tag names one frame', () => {
    const tags = Object.values(TAG)
    expect(new Set(tags).size).toBe(tags.length)
  })

  test("ATTACH_RESULT is a channel ctrl at 0x3b, and round-trips an attach's lastSeq or its absence", () => {
    expect(TAG.ATTACH_RESULT).toBe(0x3b)
    expect(isChannelCtrlTag(TAG.ATTACH_RESULT)).toBe(true)
    expect(decode(encode.attachResult(3, 7), wireSeqs)).toEqual({ tag: TAG.ATTACH_RESULT, index: 3, lastSeq: 7 })
    expect(decode(encode.attachResult(3, null), wireSeqs)).toEqual({ tag: TAG.ATTACH_RESULT, index: 3, lastSeq: null })
  })

  test("BDP_PING round-trips its probe, and BDP_PING_ACK the probe, whether the window starved its sender, and the path's round trip it measured", () => {
    expect(decode(encode.bdpPing(3, 0xffff_ffff), wireSeqs)).toEqual({
      tag: TAG.BDP_PING,
      index: 3,
      probe: 0xffff_ffff,
    })
    expect(decode(encode.bdpPingAck(3, 7, true, 42.5), wireSeqs)).toEqual({
      tag: TAG.BDP_PING_ACK,
      index: 3,
      probe: 7,
      starved: true,
      pathRtt: 42.5,
    })
    // Where it measured none.
    expect(decode(encode.bdpPingAck(3, 7, false, Infinity), wireSeqs)).toEqual({
      tag: TAG.BDP_PING_ACK,
      index: 3,
      probe: 7,
      starved: false,
      pathRtt: Infinity,
    })
    // Under a microsecond, as on loopback, it still says it measured one.
    expect(decode(encode.bdpPingAck(3, 7, false, 0.0001), wireSeqs)).toMatchObject({ pathRtt: 0.001 })
  })

  test('WINDOW round-trips its limit and the last seq its receiver has', () => {
    expect(decode(encode.window(3, 1_024, 0xffff_fffe), wireSeqs)).toEqual({
      tag: TAG.WINDOW,
      index: 3,
      bytes: 1_024,
      lastSeq: 0xffff_fffe,
    })
  })

  test('a BARRIER round-trips at one entry and at the largest shape the caps admit', () => {
    const one: BarrierPayload = { sessionId: 'sess-0', upgradeId: 'upg-1', open: goodOpen }
    expect(decode(encode.barrier(one), wireSeqs)).toEqual({ tag: TAG.BARRIER, payload: one })
    const open = Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, ix) => ({
      id: String(ix).padStart(UPGRADE_MAX_ID_BYTES, 'x'),
      ix: 0xffff - ix,
      lastSeq: Number.MAX_SAFE_INTEGER,
      initial: true as const,
      broadcast: { text: false, binary: false },
      probe: 0xffff_ffff,
    }))
    const max: BarrierPayload = { sessionId: 'x'.repeat(64), upgradeId: 'y'.repeat(64), open }
    const encoded = encode.barrier(max)
    // The byte cap is derived from the entry caps precisely so this frame is admissible: a cap
    // that refuses the largest legal barrier would fail every client that hit the entry cap.
    expect(encoded.byteLength).toBeGreaterThan(MAX_CHANNELS_PER_CONNECTION * UPGRADE_MAX_ID_BYTES)
    expect(encoded.byteLength).toBeLessThanOrEqual(WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    expect(decodeClientFrame(encoded, WIRE_MAX_CONN_CTRL_FRAME_BYTES, wireSeqs)).toEqual({
      tag: TAG.BARRIER,
      payload: max,
    })
  })

  test('a RECONCILED round-trips the commit upgradeId', () => {
    const payload = reconciled({ open: [{ ix: 0, lastSeq: 3 }], upgradeId: 'upg-1' })
    expect(decode(encode.reconciled(payload), wireSeqs)).toEqual({ tag: TAG.RECONCILED, payload })
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
    ['an entry with an overflowing lastSeq', { sessionId: 's', open: [{ id: 'A', ix: 0, lastSeq: 2 ** 53 }] }],
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
      { open: [{ id: 'A', ix: 0xffff, lastSeq: Number.MAX_SAFE_INTEGER }] },
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
    expect(() => decodeClientFrame(oversize, WIRE_MAX_CONN_CTRL_FRAME_BYTES, wireSeqs)).toThrow(
      'upgrade frame over byte cap',
    )

    const legal = encode.barrier({ sessionId: 's', upgradeId: 'u', open: goodOpen })
    expect(decodeClientFrame(legal, WIRE_MAX_CONN_CTRL_FRAME_BYTES, wireSeqs).tag).toBe(TAG.BARRIER)
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

describe('heartbeat', () => {
  test("a PING names each channel the page ended with its seq, and a PONG answers each with the server's seq or none", () => {
    const ended = [
      { ix: 3, lastSeq: 7 },
      { ix: 65_535, lastSeq: 2 ** 31 - 1 },
    ]
    expect(clientFrame(encode.ping(ended))).toEqual({ tag: TAG.PING, ended })
    expect(clientFrame(encode.ping())).toEqual({ tag: TAG.PING, ended: [] })
    const answers = [
      { ix: 3, lastSeq: 5 },
      { ix: 4, lastSeq: null },
    ]
    expect(decode(encode.pong(answers), wireSeqs)).toEqual({ tag: TAG.PONG, ended: answers })
  })

  test('a PING whose payload splits an entry is a violation', () => {
    const ragged = encode.ping([{ ix: 1, lastSeq: 1 }]).slice(0, 12)
    expect(() => clientFrame(ragged)).toThrow(ProtocolViolationError)
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
    ['RECONCILED', encode.reconciled(reconciled())],
    ['ATTACH_RESULT', encode.attachResult(0, 0)],
  ]
  test.each(serverOnly)('a client-sent %s is refused', (_name, frame) => {
    expect(() => clientFrame(frame)).toThrow(ProtocolViolationError)
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
    ['ERROR', encode.error(0, ERROR_REASON.LOST, 1)],
    ['WINDOW', encode.window(0, 1_024, 7)],
    ['MSG_WINDOW', encode.msgWindow(0, 8)],
    ['BDP_PING', encode.bdpPing(0, 1)],
    ['BDP_PING_ACK', encode.bdpPingAck(0, 1, true, 50)],
    ['BROADCAST_SUB', encode.broadcastSub(0, false)],
    ['BROADCAST_UNSUB', encode.broadcastUnsub(0, false)],
  ]
  test.each(clientLegal)('control: a client-sent %s passes', (_name, frame) => {
    expect(clientFrame(frame).tag).toBe(frame[0])
  })
})

describe('seqs past 32 bits', () => {
  /** A receiver whose highest seq of what its peer sent is `received`, and whose last seq sent is `sent`. */
  const standing = (received: number, sent: number): SeqReader => ({ received: () => received, sent: () => sent })
  const around = (boundary: number) => Array.from({ length: 7 }, (_, n) => boundary - 3 + n)
  const boundaries = [2 ** 31, 2 ** 32, 5 * 2 ** 32, 2 ** 40]

  const sequenced: [string, (seq: number) => Uint8Array<ArrayBuffer>][] = [
    ['TEXT', (seq) => encode.text(3, '"x"', seq)],
    ['BINARY', (seq) => encode.binary(3, new Uint8Array([1]), seq)],
    ['TEXT_ACK_REQ', (seq) => encode.textAckReq(3, '"x"', seq)],
    ['BINARY_ACK_REQ', (seq) => encode.binaryAckReq(3, new Uint8Array([1]), seq)],
    ['ACK_RES', (seq) => encode.ackRes(3, seq, 1, '"x"')],
    ['PUBLISH', (seq) => encode.publish(3, encodePublishText('"x"', { seq: 1, timestamp: 2 }), seq)],
    ['PUBLISH_ACK_REQ', (seq) => encode.publishAckReq(3, '"x"', seq)],
    [
      'PUBLISH_BINARY',
      (seq) => encode.publishBinary(3, encodePublishBinary(new Uint8Array([1]), { seq: 1, timestamp: 2 }), seq),
    ],
    ['PUBLISH_BINARY_ACK_REQ', (seq) => encode.publishBinaryAckReq(3, new Uint8Array([1]), seq)],
    ['CLOSE', (seq) => encode.close(3, 1_000, seq)],
    ['CLOSE_ACK', (seq) => encode.closeAck(3, seq)],
    ['ABORT', (seq) => encode.abort(3, '"x"', seq)],
    ['ERROR', (seq) => encode.error(3, ERROR_REASON.LOST, seq)],
  ]
  test.each(sequenced)("a %s's seq reads whole across 2^31 and 2^32, from the highest its receiver has", (_, build) => {
    for (const seq of boundaries.flatMap(around)) {
      // The next it expects, one a replay repeats, and one past a gap a lost replay leaves.
      const received = [seq - 1, seq + 1_000, seq - 1_000_000]
      for (const highest of received) expect(decode(build(seq), standing(highest, 0))).toMatchObject({ seq })
    }
  })

  test('an acknowledgement reads whole across 2^31 and 2^32, from the last seq its receiver sent', () => {
    for (const lastSeq of boundaries.flatMap(around)) {
      // All that was sent, or all but what a wire or a replay holds.
      for (const sent of [lastSeq, lastSeq + 1_000_000]) {
        const seqs = standing(0, sent)
        expect(decode(encode.window(3, 1_024, lastSeq), seqs)).toMatchObject({ lastSeq })
        expect(decode(encode.attachResult(3, lastSeq), seqs)).toMatchObject({ lastSeq })
        expect(decode(encode.ping([{ ix: 3, lastSeq }]), seqs)).toMatchObject({ ended: [{ ix: 3, lastSeq }] })
        expect(decode(encode.pong([{ ix: 3, lastSeq }]), seqs)).toMatchObject({ ended: [{ ix: 3, lastSeq }] })
      }
    }
  })

  test('an ACK_RES carries the seq it answers whole, however many seqs its receiver sent since', () => {
    for (const ackedSeq of [0, 1, ...boundaries.flatMap(around), Number.MAX_SAFE_INTEGER])
      for (const sent of [ackedSeq, ackedSeq + 2 ** 32 + 5, Number.MAX_SAFE_INTEGER])
        expect(decode(encode.ackRes(3, 1, ackedSeq, '"x"'), standing(0, sent))).toMatchObject({ ackedSeq })
  })

  test("a binary publish carries its key's seq as a text publish does, past 2^32", () => {
    for (const seq of [0, 1, ...boundaries.flatMap(around), Number.MAX_SAFE_INTEGER]) {
      const info = { seq, timestamp: 1_700_000_000_000 }
      const binary = encode.publishBinary(3, encodePublishBinary(new Uint8Array([7]), info), 1)
      const text = encode.publish(3, encodePublishText('"x"', info), 1)
      expect(decode(binary, wireSeqs)).toMatchObject({ info, data: new Uint8Array([7]) })
      expect(decode(text, wireSeqs)).toMatchObject({ info, text: '"x"' })
    }
  })

  test('a seq never reads below 0, whatever its bits', () => {
    for (const bits of [0, 1, 2 ** 31 - 1, 2 ** 31, 2 ** 31 + 1, 2 ** 32 - 1])
      for (const at of [0, 1, 7]) {
        const seqs = standing(at, at)
        expect((decode(encode.text(3, '"x"', bits), seqs) as { seq: number }).seq).toBeGreaterThanOrEqual(0)
        expect((decode(encode.window(3, 1_024, bits), seqs) as { lastSeq: number }).lastSeq).toBeGreaterThanOrEqual(0)
      }
  })

  test('a RECONCILE carries a lastSeq past 2^32 whole', () => {
    const open = [{ id: 'A', ix: 0, lastSeq: 2 ** 32 + 5 }]
    expect(clientFrame(encode.reconcile({ open }))).toEqual({ tag: TAG.RECONCILE, payload: { open } })
  })
})
