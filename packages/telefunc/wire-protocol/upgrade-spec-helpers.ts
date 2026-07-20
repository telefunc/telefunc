// Fixtures shared by the server-side upgrade specs. Extracted because three files had grown their
// own near-identical `connectSse`, two had their own `connectSecondSse`, and two kept separate
// sentinel counters on DIFFERENT bases — the kind of drift where a helper is fixed in one file and
// silently stays wrong in the others.

export { EMPTY_STAGE, connectSse, connectSecondSse, prepare, barrier, reconciledOn, finsOn, readysOn, makeSentinel }
export type { Harness, HarnessWire }

import { expect } from 'vitest'

import { createMuxHarness, prepareFrame, reconcileFrame, textFrame } from './upgrade-mux-harness.js'
import { TAG, type DecodedFrame } from './shared-ws.js'

type Harness = ReturnType<typeof createMuxHarness>
type HarnessWire = Harness['sse']

/** No stage in either direction, and no bytes charged. */
const EMPTY_STAGE = { records: 0, reverseRecords: 0, bytes: 0 }

/** A live SSE connection with one registered channel, proven to deliver before any test acts. */
async function connectSse(h: Harness) {
  const chA = h.registerChannel('A')
  await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const s0 = h.sse.sessionId()
  expect(s0).toBeTypeOf('string')
  await h.sse.deliver(textFrame(0, 1, 111))
  expect(chA.received).toEqual([111]) // sentinel: this wire delivers
  return { chA, s0: s0! }
}

/** A second, fully independent SSE connection with its own session. */
async function connectSecondSse(h: Harness) {
  const sse2 = h.makeWire('sse-conn-2')
  h.registerChannel('B')
  await sse2.deliver(reconcileFrame({ open: [{ id: 'B', ix: 0, lastSeq: 0, initial: true }] }))
  expect(sse2.sessionId()).toBeTypeOf('string')
  return sse2
}

const prepare = (sessionId: string, upgradeId = 'upg-1') => prepareFrame({ upgradeId, sessionId })

/** The old wire's FINAL frame: authoritative membership plus barrier-fresh cursors. */
const barrier = (sessionId: string, upgradeId = 'upg-1', lastSeq = 1) =>
  reconcileFrame({ sessionId, barrier: true, upgradeId, open: [{ id: 'A', ix: 0, lastSeq }] })

const reconciledOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.RECONCILED)
const finsOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.FIN)
const readysOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.READY)

/** Live-sentinel driver. Seqs start well clear of every in-test frame so a sentinel can never be
 *  dropped as a duplicate by the channel's `_lastClientSeq` dedup and misread as a routing failure.
 *  Each spec file makes its own, so the counters cannot bleed between files. */
function makeSentinel(seqBase: number, valueBase: number) {
  let seq = seqBase
  let value = valueBase
  return {
    reset: () => {
      seq = seqBase
      value = valueBase
    },
    /** The wire still routes a FRESH payload to its real listener. */
    expectDelivers: async (
      wire: { deliver: (f: Uint8Array<ArrayBuffer>) => Promise<void> },
      chA: { received: number[] },
    ) => {
      const next = ++value
      await wire.deliver(textFrame(0, ++seq, next))
      expect(chA.received.at(-1)).toBe(next)
    },
  }
}
