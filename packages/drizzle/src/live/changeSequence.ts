export { observeEnvelopeSequence, withBaselineBetClosed }
export type { OriginSequence, SequenceHeader, SequenceDecision, SequenceTransition }

// THE PER-ORIGIN ORDERING AUTOMATON — a pure transition, deliberately separated from the I/O callback that
// used to contain it. Given what we have seen from one publisher and the header of one arriving envelope, it
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
// THE INVARIANT: a change is applied at most once; anything not applied precisely is over-fired; and any
// sequence skipped by a baseline bet is either already in the snapshot or still owed to us by the adapter.

/** What we know about one publishing origin: the highest sequence seen (`last`), and the sequence below
 *  which nothing has been accounted for yet (`unknownBelow` — the outstanding baseline bet, 0 once a
 *  coarsen has covered it). */
type OriginSequence = { last: number; unknownBelow: number }

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
  // ERA CUT — the publisher changed transport, and it is telling us so because we could not have worked it
  // out. The deferred baseline rests on a dichotomy that a rotation breaks: a sequence under the first one we
  // see is either pre-admission (already in our snapshot) or still owed to us by an at-least-once adapter.
  // Messages from the publisher's PREVIOUS era are neither — published after we were admitted, but onto a
  // transport we were never subscribed to, so no straggler can ever arrive to correct a wrong bet and the
  // hole would be permanent rather than brief. So we do NOT bet precise across a cut: coarsen once, and take
  // this sequence as the watermark. Everything below it is thereby accounted for, from either era.
  //
  // The cost is one reseed per watched graph per rotation, and rotation only happens at a quiescent boundary
  // — rare by construction. Coarse is recoverable; a permanently missing delta is not.
  if (header.eraCut) {
    // A redelivery of a cut we already acted on: at-least-once means we may see it twice, and coarsening
    // again would buy a redundant reseed (or, mid-reseed, risk the storm guard's terminal demotion).
    if (tracked !== undefined && header.seq <= tracked.last) return { decision: 'drop' }
    return { decision: 'coarsen', next: { last: header.seq, unknownBelow: 0 } }
  }

  if (tracked === undefined) {
    // The bet. Everything below this sequence is unaccounted for until it either proves irrelevant (never
    // arrives, because it predates admission) or arrives as a straggler.
    return { decision: 'apply', next: { last: header.seq, unknownBelow: header.seq } }
  }

  if (header.seq <= tracked.last) {
    // At or below the watermark: either a sequence we skipped, or a redelivery of one we already handled.
    // `unknownBelow` is what tells them apart — a duplicate is not evidence of anything and must not coarsen.
    if (header.seq >= tracked.unknownBelow) return { decision: 'drop' } // already seen → duplicate → drop
    // The bet was wrong; one coarsen covers the whole unaccounted region. The watermark does NOT move: this
    // envelope is older than what we have already seen.
    return { decision: 'coarsen', next: { last: tracked.last, unknownBelow: 0 } }
  }

  if (header.seq !== tracked.last + 1) {
    // A real gap. Applying this before the ones it overtook would apply a delta out of order, so coarsen —
    // which also accounts for everything below it, closing any outstanding bet.
    return { decision: 'coarsen', next: { last: header.seq, unknownBelow: 0 } }
  }

  return { decision: 'apply', next: { last: header.seq, unknownBelow: tracked.unknownBelow } }
}

/** Close an outstanding baseline bet without moving the watermark.
 *
 *  Used when an APPLIED envelope turns out to coarsen everything anyway (a `coarseAll` announcement):
 *  coarsening rebuilds every watched graph from the database, which accounts for every lower sequence as
 *  surely as a gap does — so a later straggler must not trigger a second one. Leaving the bet open would buy
 *  a redundant reseed at best, and at worst a terminal demotion, if that straggler landed while this reseed
 *  was still in flight. */
function withBaselineBetClosed(entry: OriginSequence): OriginSequence {
  return { last: entry.last, unknownBelow: 0 }
}
