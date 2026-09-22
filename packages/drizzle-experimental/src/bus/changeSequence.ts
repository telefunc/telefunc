export { observeEnvelopeSequence, openBet, advance, coarsenClosingBet }
export type { OriginSequence, SequenceHeader, SequenceDecision, SequenceTransition }

// THE PER-ORIGIN ORDERING AUTOMATON — a pure transition, deliberately kept out of the I/O callback that
// delivers envelopes. Given what we have seen from one publisher and the header of one arriving envelope, it
// decides whether that envelope is applied precisely, dropped, or coarsened over. It reads and writes no
// state of its own: the caller holds the map and stores what this returns, which is what makes every rule
// below assertable as a table of inputs rather than only through a delivered payload.
//
// The adapter owes us AT-LEAST-ONCE delivery of the exact payload and nothing more (see changeTransport.ts),
// so an envelope may be a duplicate, may be out of order, and may follow a gap. `seq` is contiguous per
// origin, which makes all three decidable:
//   seq === expected  → in order, apply precisely
//   seq  >  expected  → something was lost or is late; we cannot know what, so COARSEN and move the
//                       watermark up. Anything below it is then already covered.
//   seq <=  last      → a duplicate, or a straggler the coarsen above already accounted for → DROP
//
// DEFERRED BASELINE. A receiver that subscribes — or RE-subscribes, which clears the watermarks — has no
// position for a publisher already running, and cannot tell "this is simply my next one" from "this
// overtook the one before it". Coarsening pre-emptively for that would pay on EVERY resubscribe against a
// busy peer, and `graph.coarsen()` is terminal — the graphs alive at that moment would stay coarse for the
// rest of their lives. So instead the first message from an unknown origin is taken PRECISELY, and the
// sequences below it are recorded as unaccounted-for (`unknownBelow`).
//
// That bet rests on the adapter contract's NO-BACKLOG clause (changeTransport.ts): a subscription only
// receives what was published after it was admitted. Given that, the sequences below the first one we see
// fall into exactly two cases:
//   - published BEFORE our admission → the readiness barrier means our snapshot was read after admission,
//     so they are already IN the data, and they will never be delivered to us. Nothing to apply.
//   - published AFTER admission → the adapter owes us at-least-once, so each one MUST still be delivered.
//     It arrives late and below the watermark, and THAT is the signal we coarsen on.
// A reorder is therefore always eventually observable, so we can wait for proof instead of guessing.
//
// A REPLAY/BACKLOG transport breaks this, which is why the clause is written down rather than assumed: a
// pre-admission payload delivered after admission is already in the snapshot, and taking it as the
// baseline would apply it a second time. It is indistinguishable from a legitimate first message — an
// unknown origin's opening sequence looks the same either way — so there is no cheap runtime detection to
// fall back on, and the obligation is carried by the contract and pinned against the shipped default
// transport instead. (Once an origin HAS a watermark, a late pre-admission payload is a straggler and
// coarsens like any other; only the very first message from an origin is exposed.)
//
// The cost of a wrong bet, stated honestly: between the precise baseline and the straggler's arrival, a
// precise graph is serving a result that is missing one delta — briefly INCORRECT BY OMISSION, not merely
// stale — and the straggler's coarsen corrects it. Same class as the documented drop-after-readiness
// limit, and unlike that one it closes deterministically rather than waiting for the next write.
//
// ERA CUT. A rotation to a different transport breaks the dichotomy the bet rests on. Messages from the
// publisher's PREVIOUS era are neither pre-admission (already in our snapshot) nor still owed to us: they
// were published after we were admitted, but onto a transport we were never subscribed to, so no straggler
// can ever arrive to correct a wrong bet and the hole would be permanent rather than brief. The publisher
// therefore MARKS the first envelope of a new era, and a marked envelope is never applied precisely — it is
// treated exactly as a gap is, for exactly the same reason. The cost is one reseed per watched graph per
// rotation, and rotation only happens at a quiescent boundary, so it is rare by construction. Coarse is
// recoverable; a permanently missing delta is not.
//
// THE INVARIANT: a change is applied at most once; anything not applied precisely is over-fired; and any
// sequence skipped by a baseline bet is either already in the snapshot or still owed to us by the adapter.

/** What we know about one publishing origin: the highest sequence seen (`last`), and the sequence below
 *  which nothing has been accounted for yet (`unknownBelow` — the outstanding baseline bet, 0 once a
 *  coarsen has covered it). */
type OriginSequence = { last: number; unknownBelow: number }

// The three ways an origin's position can move, named — because the two fields are only meaningful as a
// pair, and every rule below is one of these three sentences rather than an arrangement of numbers.

/** First contact: apply this one precisely, and record everything under it as unaccounted for. */
function openBet(seq: number): OriginSequence {
  return { last: seq, unknownBelow: seq }
}

/** The next contiguous sequence: the watermark moves, and an outstanding bet stays outstanding. */
function advance(tracked: OriginSequence, seq: number): OriginSequence {
  return { last: seq, unknownBelow: tracked.unknownBelow }
}

/** A coarsen rebuilds every watched graph from the database, which accounts for every lower sequence as
 *  surely as an in-order delivery does — so it CLOSES any outstanding bet, and a later straggler must not
 *  trigger a second one (a redundant reseed at best; at worst, if it lands mid-reseed, the storm guard's
 *  terminal demotion).
 *
 *  The watermark is passed rather than derived because the two callers differ on it: a coarsen triggered by
 *  something ABOVE what we have seen moves it up to that sequence, while one triggered by a straggler must
 *  leave it exactly where it is — moving it down would re-apply everything in between. */
function coarsenClosingBet(watermark: number): OriginSequence {
  return { last: watermark, unknownBelow: 0 }
}

/** Whether a sequence at or below the watermark lies in the region an open bet never accounted for. This is
 *  what tells a straggler that DISPROVES the bet from a plain at-least-once redelivery, which is evidence of
 *  nothing and must not coarsen. */
function belowOpenBet(tracked: OriginSequence, seq: number): boolean {
  return seq < tracked.unknownBelow
}

/** The ordering-relevant part of an arriving envelope. */
type SequenceHeader = { seq: number; eraCut?: true }

/** `apply` — in order, feed the payload to the graphs. `coarsen` — over-fire every watched table instead.
 *  `drop` — a duplicate or an already-accounted-for straggler; it says nothing and must change nothing. */
type SequenceDecision = 'apply' | 'coarsen' | 'drop'

/** The decision, and the origin's state AFTER it. `next` is absent only for a `drop`, which leaves the
 *  state exactly as it was — so a caller that stores it unconditionally cannot rewind a watermark. */
type SequenceTransition = { decision: SequenceDecision; next?: OriginSequence }

/** Decide what to do with one envelope from one origin. `tracked` is what we last knew about that origin,
 *  or `undefined` if we have never heard from it (a fresh subscription, or one that re-subscribed and
 *  cleared its watermarks). */
function observeEnvelopeSequence(tracked: OriginSequence | undefined, header: SequenceHeader): SequenceTransition {
  const { seq, eraCut } = header

  // FIRST CONTACT. An era cut is not a baseline we may bet on — nothing below it is deliverable to us at
  // all — so it coarsens instead, taking this sequence as the watermark and accounting for both eras below.
  if (tracked === undefined) {
    if (eraCut) return { decision: 'coarsen', next: coarsenClosingBet(seq) }
    return { decision: 'apply', next: openBet(seq) }
  }

  // AT OR BELOW THE WATERMARK — a straggler, or a redelivery of something already handled. The open bet is
  // what tells them apart, and a cut is no different here: it is simply the one straggler that could never
  // have been corrected by a later one. (No FIFO is owed, so a post-rotation seq n+1 can overtake the cut at
  // seq n — we first-see n+1, bet on everything under it, and the cut arrives below the watermark. Reading
  // that as a duplicate would leave the bet open forever, which is the permanent hole eraCut exists to
  // close.) The watermark does NOT move: this envelope is older than what we have already seen.
  if (seq <= tracked.last) {
    if (!belowOpenBet(tracked, seq)) return { decision: 'drop' }
    return { decision: 'coarsen', next: coarsenClosingBet(tracked.last) }
  }

  // ABOVE THE WATERMARK. A gap means something was lost or is late and applying this first would apply a
  // delta out of order; a cut means the ordering evidence itself does not carry across the rotation. Neither
  // can be applied precisely, and one coarsen accounts for everything below either way.
  if (eraCut || seq !== tracked.last + 1) return { decision: 'coarsen', next: coarsenClosingBet(seq) }

  return { decision: 'apply', next: advance(tracked, seq) }
}
