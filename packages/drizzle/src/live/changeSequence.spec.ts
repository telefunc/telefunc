// THE ORDERING AUTOMATON AS A TABLE.
//
// These rules used to live inside the subscription's delivery callback, reachable only by standing up a
// transport, a publisher, a receiver and a graph registry, then inferring the decision from whether a router
// ingested a coarse marker or a row. Every case below was therefore expensive to state and several were
// never stated at all — a mutation that stopped advancing the watermark on an in-order envelope, and one
// that kept stale watermarks across a detach, both survived the whole suite.
//
// As a pure transition the same rules are a table of inputs and outputs, so the exhaustive cases are cheap
// and the integration specs are free to test what only they can (that a decision reaches the graphs).
//
// `unknownBelow` is the outstanding BASELINE BET: the first envelope from an unknown origin is taken
// precisely, and everything below it is recorded as unaccounted-for until it either never arrives (it
// predates admission and is already in the snapshot) or turns up late as a straggler and coarsens.

import { describe, expect, it } from 'vitest'
import { observeEnvelopeSequence, withBaselineBetClosed } from './changeSequence.js'

describe('changeSequence — an origin we have never heard from', () => {
  it('bets PRECISE and records what it bet on', () => {
    // Not a coarsen: coarsening every first message would pay on every resubscribe against a busy peer, and
    // `graph.coarsen()` is terminal. The bet is what makes that cost avoidable, and `unknownBelow` is what
    // makes it correctable.
    expect(observeEnvelopeSequence(undefined, { seq: 7 })).toEqual({
      decision: 'apply',
      next: { last: 7, unknownBelow: 7 },
    })
  })
})

describe('changeSequence — an origin in flight', () => {
  it('applies the NEXT sequence and ADVANCES the watermark', () => {
    // The advance is the load-bearing half. Leaving `last` where it was makes every later envelope compare
    // as "at or below the watermark" and drop as a duplicate — a silent, permanent missed invalidation from
    // that origin onward. A mutation removing it survived the entire suite before this case existed.
    expect(observeEnvelopeSequence({ last: 4, unknownBelow: 0 }, { seq: 5 })).toEqual({
      decision: 'apply',
      next: { last: 5, unknownBelow: 0 },
    })
  })

  it('carries an OPEN bet across an in-order apply rather than quietly closing it', () => {
    // The bet is only closed by something that actually accounts for the region — a coarsen, a gap, or a
    // coarse-all. An ordinary in-order delivery accounts for nothing below itself.
    expect(observeEnvelopeSequence({ last: 7, unknownBelow: 7 }, { seq: 8 })).toEqual({
      decision: 'apply',
      next: { last: 8, unknownBelow: 7 },
    })
  })

  it('COARSENS on a gap and moves the watermark up, closing any outstanding bet', () => {
    expect(observeEnvelopeSequence({ last: 5, unknownBelow: 3 }, { seq: 9 })).toEqual({
      decision: 'coarsen',
      next: { last: 9, unknownBelow: 0 },
    })
  })
})

describe('changeSequence — at or below the watermark', () => {
  it('DROPS a redelivery of something already handled', () => {
    // At-least-once delivery means duplicates are ordinary traffic. A duplicate is evidence of nothing, so
    // coarsening on one would turn a healthy transport into a reseed storm.
    expect(observeEnvelopeSequence({ last: 6, unknownBelow: 0 }, { seq: 6 })).toEqual({ decision: 'drop' })
    expect(observeEnvelopeSequence({ last: 6, unknownBelow: 0 }, { seq: 2 })).toEqual({ decision: 'drop' })
  })

  it('COARSENS on a straggler that lands inside an open bet — the proof the bet was wrong', () => {
    // seq 2 is below `unknownBelow` 5, so it is one of the sequences the baseline assumed it would never
    // see. Seeing it disproves the assumption, and one coarsen covers the whole unaccounted region.
    expect(observeEnvelopeSequence({ last: 9, unknownBelow: 5 }, { seq: 2 })).toEqual({
      decision: 'coarsen',
      next: { last: 9, unknownBelow: 0 },
    })
  })

  it('does NOT rewind the watermark when it coarsens for a straggler', () => {
    // The straggler is older than what we have already applied; taking its seq as the position would make
    // everything between it and `last` deliverable a second time.
    const { next } = observeEnvelopeSequence({ last: 9, unknownBelow: 5 }, { seq: 2 })
    expect(next?.last).toBe(9)
  })

  it('a straggler AT the bet boundary is a duplicate, not a straggler', () => {
    // `unknownBelow` is exclusive: it is the first sequence we DID see. The boundary is where the two
    // readings meet, so it is the case an off-by-one would flip.
    expect(observeEnvelopeSequence({ last: 9, unknownBelow: 5 }, { seq: 5 })).toEqual({ decision: 'drop' })
    expect(observeEnvelopeSequence({ last: 9, unknownBelow: 5 }, { seq: 4 })).toEqual({
      decision: 'coarsen',
      next: { last: 9, unknownBelow: 0 },
    })
  })
})

describe('changeSequence — era cut', () => {
  it('COARSENS rather than betting precise, and takes the cut sequence as the watermark', () => {
    // Across a rotation the deferred baseline's dichotomy fails: the publisher's previous-era messages were
    // published after we were admitted but onto a transport we were never on, so no straggler can ever
    // arrive to correct a wrong bet. The hole would be permanent rather than brief.
    expect(observeEnvelopeSequence(undefined, { seq: 2, eraCut: true })).toEqual({
      decision: 'coarsen',
      next: { last: 2, unknownBelow: 0 },
    })
  })

  it('DROPS a redelivered cut it already acted on', () => {
    expect(observeEnvelopeSequence({ last: 2, unknownBelow: 0 }, { seq: 2, eraCut: true })).toEqual({
      decision: 'drop',
    })
  })

  it('still coarsens for a cut ABOVE the watermark — a second, genuine rotation', () => {
    // The redelivery guard must key on the sequence, not on "we have seen a cut before", or a db that
    // rotates twice would bet precise across the second one.
    expect(observeEnvelopeSequence({ last: 2, unknownBelow: 0 }, { seq: 6, eraCut: true })).toEqual({
      decision: 'coarsen',
      next: { last: 6, unknownBelow: 0 },
    })
  })

  it('COARSENS for a cut OVERTAKEN under an open bet, keeping the higher watermark', () => {
    // The row that separates "redelivery" from "the bet was wrong". No FIFO is owed, so the publisher's
    // post-rotation seq 6 can arrive before the cut at seq 5: we first-see 6, bet precise on everything
    // under it, and the cut lands BELOW the watermark. Reading that as a duplicate is what leaves the
    // receiver permanently without the previous era — nothing under 6 is deliverable, because it was
    // published onto a transport this receiver was never subscribed to.
    //
    // Coarsen closes the bet; the watermark stays at 6, because sequences up to 6 have already been applied
    // and moving it down to 5 would re-apply 6.
    expect(observeEnvelopeSequence({ last: 6, unknownBelow: 6 }, { seq: 5, eraCut: true })).toEqual({
      decision: 'coarsen',
      next: { last: 6, unknownBelow: 0 },
    })
  })

  it('and DROPS that same cut once the bet is closed — the discrimination is the bet, not the sequence', () => {
    // The control for the case above: identical `last` and identical cut sequence, differing ONLY in whether
    // the bet is open. With it closed the region below is already accounted for, so the cut is a genuine
    // at-least-once redelivery and coarsening again would buy a redundant reseed — or, mid-reseed, risk the
    // storm guard's terminal demotion. Without this pair, "always coarsen a cut" would pass the case above.
    expect(observeEnvelopeSequence({ last: 6, unknownBelow: 0 }, { seq: 5, eraCut: true })).toEqual({
      decision: 'drop',
    })
  })
})

describe('changeSequence — closing a bet without moving', () => {
  it('withBaselineBetClosed keeps the watermark and clears only the bet', () => {
    expect(withBaselineBetClosed({ last: 8, unknownBelow: 3 })).toEqual({ last: 8, unknownBelow: 0 })
  })
})
