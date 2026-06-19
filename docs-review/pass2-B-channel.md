# Pass-2 report — `docs/pages/channel/+Page.mdx` (reviewer B, channel)

### [34] `docs/pages/channel/+Page.mdx` — validation note (line 131, sentence 1)
- **Original:** "Both generic parameters also drive runtime validation: Telefunc auto-generates shields that check every message and ack arriving at the server against its declared type."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "drive runtime validation" slightly abstract; singular "its" after the plural pairing "every message and ack" is mildly loose (number disagreement between the distributive "every … its").
- **Candidates:**
  1. "Both generic parameters also power runtime validation: …against its declared type." — C 8 / N 9 / Overall 8 — "power" is no clearer than "drive"; the "its" looseness is untouched.
  2. "Both generic parameters also drive runtime validation: …check every incoming message and ack against the type it declares." — C 9 / N 9 / Overall 9 — fixes agreement, but "incoming" silently drops the meaningful "at the server" locator.
  3. "Both generic parameters also drive runtime validation: …check each message and ack arriving at the server against the type it declares." — C 9 / N 9 / Overall 9 — "each … it … it declares" agrees in number; keeps "arriving at the server"; "the type it declares" reads cleaner than the loose "its declared type".
  4. "Both generic parameters also feed runtime validation: …shields that validate every message and ack…against its declared type." — C 8 / N 8 / Overall 8 — "validation"/"validate" repetition; "its" unfixed.
  5. "Both generic parameters do more than type the API — they also drive runtime validation: …against its declared type." — C 8 / N 8 / Overall 8 — longer, and the documented "its" looseness remains.
- **Decision:** Applied → "Both generic parameters also drive runtime validation: Telefunc auto-generates shields that check each message and ack arriving at the server against the type it declares." (new Overall 9)
- **Why:** "each … the type it declares" resolves the singular/plural looseness cleanly while preserving the meaning, the "at the server" fact, and the established "drive runtime validation" / "auto-generates shields" voice.

### [39] `docs/pages/channel/+Page.mdx` — `Broadcast` intro (line 203, sentence 1)
- **Original:** "`Broadcast` is a keyed pub/sub **bus**: a message published to a `key` reaches every subscriber of that `key` — server-side subscribers (via `Broadcast.subscribe()`) and clients bridged into the key (via a `BroadcastChannel`)."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — well-structured but the colon + em-dash carry a lot; one nit: "the key" (no backticks) vs `key` everywhere else is mildly inconsistent.
- **Candidates:**
  1. "…and clients bridged into the `key` (via a `BroadcastChannel`)." — C 9 / N 9 / Overall 9 — fixes the inconsistency exactly; everything else preserved.
  2. "…and clients bridged into that `key` (via a `BroadcastChannel`)." — C 9 / N 9 / Overall 9 — correct, but "that" re-echoes the earlier "that `key`", a slight repetition.
  3. "…and clients bridged in via a `BroadcastChannel`." — C 8 / N 8 / Overall 8 — drops the key reference, breaking parallelism with "server-side subscribers (via …)".
- **Decision:** Applied → "…server-side subscribers (via `Broadcast.subscribe()`) and clients bridged into the `key` (via a `BroadcastChannel`)." (new Overall 9)
- **Why:** Backticking `key` removes the documented inconsistency with every other `key` in the sentence; zero meaning change.

### [68] `docs/pages/channel/+Page.mdx` — Fundamentally (line 332)
- **Original:** "Fundamentally, the difference is what a message is addressed to: a channel message is addressed to *someone*, a broadcast message is addressed to *a topic* (the `key`)."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the leading "Fundamentally," restates the "### Fundamentally" heading directly above it, a mild redundancy.
- **Candidates:**
  1. "The difference is what a message is addressed to: a channel message is addressed to *someone*, a broadcast message is addressed to *a topic* (the `key`)." — C 9 / N 9 / Overall 9 — drops the redundant adverb; reads as a clean topic sentence under the heading.
  2. "At bottom, the difference is what a message is addressed to: …" — C 8 / N 8 / Overall 8 — swaps one adverbial lead-in for another; still echoes the heading's intent and reads less naturally.
  3. "The core difference is what a message is addressed to: …" — C 9 / N 8 / Overall 8 — "core" re-states the heading's "fundamental" sense, the same redundancy in different words.
- **Decision:** Applied → "The difference is what a message is addressed to: a channel message is addressed to *someone*, a broadcast message is addressed to *a topic* (the `key`)." (new Overall 9)
- **Why:** Removing "Fundamentally, " eliminates the documented redundancy with the heading while leaving the strong "addressed to" framing and emphasis intact.

### [89] `docs/pages/channel/+Page.mdx` — Custom transport (line 365, sentence 2)
- **Original:** "Telefunc wraps it with subscriber multiplexing and same-node delivery, so each `key` only opens one upstream subscription no matter how many local subscribers attach."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "subscriber multiplexing and same-node delivery" is dense jargon; the trailing clause does explain the payoff, which rescues it.
- **Candidates:**
  1. "Telefunc wraps it with subscriber multiplexing and local fan-out, so each `key` only opens one upstream subscription…" — C 8 / N 9 / Overall 8 — "local fan-out" is marginally friendlier but vaguer than "same-node delivery"; still jargon.
  2. "Telefunc wraps it to multiplex subscribers and deliver to same-node listeners, so each `key` only opens one upstream subscription…" — C 8 / N 8 / Overall 8 — verbifying helps slightly but "same-node listeners" is still dense and the sentence grows.
  3. "Telefunc wraps it with subscriber multiplexing and same-node delivery, so each `key` opens just one upstream subscription…" — C 8 / N 9 / Overall 8 — "only opens" → "opens just" is cosmetic; jargon unaddressed.
  4. "Telefunc adds subscriber multiplexing and same-node delivery on top, so each `key` only opens one upstream subscription…" — C 8 / N 9 / Overall 8 — "wraps it with" → "adds … on top" is a lateral move.
  5. "Telefunc layers subscriber multiplexing and same-node delivery over it, so each `key` only opens one upstream subscription…" — C 8 / N 9 / Overall 8 — "layers … over it" is slightly clearer about the wrapping, but the documented jargon stays.
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** "subscriber multiplexing" and "same-node delivery" are precise, load-bearing terms; every paraphrase either loses precision (e.g. "local fan-out") or just rewords the verb. The same sentence's "so … one upstream subscription … local subscribers attach" already operationalizes the jargon, so no rewrite genuinely raises clarity.

### [107] `docs/pages/channel/+Page.mdx` — Error table, `ChannelClosedError` / When (line 428)
- **Original:** "`send()` on a closed channel (thrown synchronously); also rejects pending `ack` sends orphaned by close or close timeout"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "orphaned by close or close timeout" is dense; meaning is recoverable but it packs two failure modes into one metaphorical clause.
- **Candidates:**
  1. "…also rejects pending `ack` sends left unresolved when the channel closes or its close times out" — C 9 / N 8 / Overall 8 — unpacks "orphaned" but "its close times out" is slightly awkward and the cell grows.
  2. "…also rejects pending `ack` sends still waiting when the channel closes or the close times out" — C 9 / N 9 / Overall 9 — "still waiting" replaces the metaphor; "closes or the close times out" lays the two modes out in parallel; fits the table register.
  3. "…also rejects pending `ack` sends orphaned by a close or close timeout" — C 8 / N 9 / Overall 8 — a bare article tweak; the density stays.
  4. "…also rejects any pending `ack` send still awaiting acknowledgement when the channel closes or times out" — C 8 / N 8 / Overall 8 — "still awaiting acknowledgement" is verbose for a cell.
  5. "…also rejects pending `ack` sends interrupted by close or close timeout" — C 8 / N 9 / Overall 8 — "interrupted" ≈ "orphaned"; the two modes are still compressed.
- **Decision:** Applied → "`send()` on a closed channel (thrown synchronously); also rejects pending `ack` sends still waiting when the channel closes or the close times out" (new Overall 9)
- **Why:** "still waiting when the channel closes or the close times out" makes the two failure modes parallel and concrete while keeping the terse table voice and the exact meaning (pending acks unresolved at close, or when the close itself times out).

## Summary
- **Targets:** 5
- **Applied:** 4 — [34], [39], [68], [107]
- **Retained:** 1 — [89] (jargon is precise/load-bearing and already explained by the sentence's payoff clause; no candidate beat Overall 8)
- **New score distribution:** [34] 8 → 9 · [39] 8 → 9 · [68] 8 → 9 · [89] 8 (retained) · [107] 8 → 9. Four of five now at Overall 9; one held at 8.
- All edits changed prose wording only; inline code, `<Link/>`, emphasis (`*…*`), the table's four-column structure, anchors, and code logic are unchanged (verified by read-back).
