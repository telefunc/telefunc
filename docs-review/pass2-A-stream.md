# Pass-2 report — `docs/pages/stream/+Page.mdx` (reviewer A)

### [1] `docs/pages/stream/+Page.mdx` — intro line (overview)
- **Original:** "Telefunc supports streaming (one-way stream) and real-time (two-way stream) use cases with:"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — the parenthetical glosses are helpful, but pairing "streaming" with "(one-way stream)" is mildly circular since the gloss reuses the word being defined.
- **Candidates:**
  1. "Telefunc supports streaming (one-way data flow) and real-time (two-way data flow) use cases with:" — C 10 / N 9 / Overall 10 — the gloss no longer reuses "stream"; "data flow" names the underlying concept and keeps the one-way/two-way contrast.
  2. "Telefunc supports streaming (one direction) and real-time (two directions) use cases with:" — C 9 / N 8 / Overall 8 — terser but "two directions" reads slightly oddly.
  3. "Telefunc supports streaming (server-to-client) and real-time (bidirectional) use cases with:" — C 9 / N 8 / Overall 8 — accurate but loses the clean one-way/two-way parallelism and over-specifies direction.
- **Decision:** Applied → "Telefunc supports streaming (one-way data flow) and real-time (two-way data flow) use cases with:" (new Overall 10)
- **Why:** "data flow" removes the circular self-reference while preserving the one-way/two-way contrast and voice.

### [8] `docs/pages/stream/+Page.mdx` — Integrations → `@telefunc/rxjs` link description
- **Original:** "reactive operators."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — very terse; assumes the reader knows RxJS.
- **Candidates:**
  1. "streams as reactive operators." — C 9 / N 9 / Overall 9 — ties the gloss back to the page topic (streams) and mirrors the neighboring TanStack line's noun-phrase shape.
  2. "RxJS reactive operators." — C 9 / N 8 / Overall 8 — clearer for non-RxJS readers but redundant next to the `@telefunc/rxjs` label.
  3. "streams exposed as reactive operators." — C 9 / N 8 / Overall 8 — accurate but wordier; "exposed as" adds little.
- **Decision:** Applied → "streams as reactive operators." (new Overall 9)
- **Why:** Anchors the two-word gloss to the page's subject (streams) without padding, matching the parallel TanStack description.

### [12] `docs/pages/stream/+Page.mdx` — Seamless DX checklist, Transport
- **Original:** "Transport — automatically picks the most performant available transport (HTTP, SSE, WebSocket)."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — clear; "most performant available" stacks two qualifiers but parses fine.
- **Candidates:**
  1. "...automatically picks the fastest available transport (HTTP, SSE, WebSocket)." — C 9 / N 9 / Overall 8 — single-syllable "fastest" de-stacks the qualifiers, but narrows "performant" (latency/overhead) to raw speed — a meaning shift.
  2. "...automatically picks the best available transport (HTTP, SSE, WebSocket)." — C 8 / N 9 / Overall 8 — "best" is vaguer than "most performant"; loses the precise reason for the choice.
  3. "...automatically picks the most performant transport available (HTTP, SSE, WebSocket)." — C 8 / N 8 / Overall 8 — reordering "available" after the noun is blocked by the `<Link>` that wraps `transport (HTTP, SSE, WebSocket)`; would require splitting the link.
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** "most performant" is the precise, intentional term; every reordering either shifts meaning or would split the existing `<Link>`.

### [15] `docs/pages/stream/+Page.mdx` — `AsyncGenerator` section, intro
- **Original:** "An `async function*` returns an `AsyncGenerator`: the usual choice for structured values that arrive one piece at a time — AI tokens, notifications, progress updates."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — the colon-then-appositive plus em-dash list is a little dense.
- **Candidates:**
  1. "An `async function*` returns an `AsyncGenerator` — the usual choice for structured values that arrive one piece at a time, such as AI tokens, notifications, or progress updates." — C 9 / N 9 / Overall 9 — one em-dash for the appositive, "such as ... or" for the examples; removes the colon+em-dash double punctuation.
  2. "An `async function*` returns an `AsyncGenerator`, the usual choice for structured values that arrive one piece at a time: AI tokens, notifications, progress updates." — C 9 / N 8 / Overall 8 — swaps which mark goes where but keeps two different expansion marks in one sentence.
  3. "An `async function*` returns an `AsyncGenerator`. It's the usual choice for structured values that arrive one piece at a time — AI tokens, notifications, progress updates." — C 9 / N 8 / Overall 8 — splitting into two sentences reads choppy for a section opener.
- **Decision:** Applied → "An `async function*` returns an `AsyncGenerator` — the usual choice for structured values that arrive one piece at a time, such as AI tokens, notifications, or progress updates." (new Overall 9)
- **Why:** Replaces the dense colon+em-dash combination with a single em-dash and a clean "such as" list; both MDX links preserved.

### [16] `docs/pages/stream/+Page.mdx` — `AsyncGenerator` section, second example lead-in
- **Original:** "For example, return an `async function*` and read each value as it arrives:"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "an `async function*`" as a returnable object is slightly loose (you return the generator it produces).
- **Candidates:**
  1. "For example, write an `async function*` and read each value as it arrives:" — C 9 / N 9 / Overall 9 — "write" matches the server code (which defines `export async function* onCountdown`); removes the "return an async function*" looseness.
  2. "For example, call an `async function*` and read each value as it arrives:" — C 8 / N 9 / Overall 8 — describes the client side only, and the lead-in introduces the full (server + client) example.
  3. "For example, return the result of an `async function*` and read each value as it arrives:" — C 9 / N 8 / Overall 8 — precise but wordy, and the result isn't literally `return`ed in the example.
- **Decision:** Applied → "For example, write an `async function*` and read each value as it arrives:" (new Overall 9)
- **Why:** "write" is accurate (you author the generator function) and natural, resolving the loose "return … function*" without changing meaning.

### [28] `docs/pages/stream/+Page.mdx` — `ReadableStream`, Client → server label
- **Original:** "**Client → server** — pass a `ReadableStream` as an argument to stream bytes up to the server:"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "up to the server" momentarily reads like a quantity ("up to N") before resolving as direction.
- **Candidates:**
  1. "**Client → server** — pass a `ReadableStream` as an argument to stream bytes to the server:" — C 9 / N 9 / Overall 9 — drops the garden-path "up"; the `→` and "to the server" still convey direction.
  2. "**Client → server** — pass a `ReadableStream` as an argument to upload bytes to the server:" — C 9 / N 9 / Overall 8 — "upload" reads well but risks implying the dedicated file-upload feature, which this section doesn't cover.
  3. "**Client → server** — pass a `ReadableStream` as an argument to stream bytes upward to the server:" — C 9 / N 8 / Overall 8 — unambiguous but "upward to the server" is heavy/redundant with the arrow.
- **Decision:** Applied → "**Client → server** — pass a `ReadableStream` as an argument to stream bytes to the server:" (new Overall 9)
- **Why:** Removes the momentary "up to = quantity" misread; the `→` label already carries the direction.

### [32] `docs/pages/stream/+Page.mdx` — `ReadableStream`, ffmpeg cleanup blockquote
- **Original:** "A spawned process outlives the call and would keep burning CPU if the client navigated away mid-encode — so it's released in `onClose()`."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — tense mix ("outlives" present / "would keep … navigated" conditional) is acceptable but a minor blemish.
- **Candidates:**
  1. "A spawned process outlives the call and keeps burning CPU if the client navigates away mid-encode — so it's released in `onClose()`." — C 9 / N 9 / Overall 9 — consistent present tense (outlives / keeps / navigates); reads more direct.
  2. "A spawned process outlives the call and would keep burning CPU if the client navigated away mid-encode — so it's released in `onClose()`." (keep) — Overall 8 — the flagged tense mix remains.
  3. "A spawned process outlives the call — and burns CPU if the client navigates away mid-encode — so it's released in `onClose()`." — C 9 / N 8 / Overall 8 — extra em-dash makes the sentence busier; "burns" loses the "keeps … going" sense.
- **Decision:** Applied → "A spawned process outlives the call and keeps burning CPU if the client navigates away mid-encode — so it's released in `onClose()`." (new Overall 9)
- **Why:** A first-conditional in consistent present tense removes the tense mix while preserving the hypothetical cause/effect and the colloquial-but-on-voice "keeps burning CPU."

### [33] `docs/pages/stream/+Page.mdx` — `Channel` section, intro
- **Original:** "For ongoing two-way and broadcast messaging, Telefunc has two primitives and one composition:"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "one composition" is precise but slightly abstract on first read.
- **Candidates:**
  1. "For ongoing two-way and broadcast messaging, Telefunc has two primitives, plus a third that composes them:" — C 9 / N 9 / Overall 9 — "a third that composes them" names the relationship (the third is built from the two) before the bullets, removing the abstractness.
  2. "For ongoing two-way and broadcast messaging, Telefunc has two primitives plus one that composes them:" — C 9 / N 8 / Overall 8 — same idea but "one that composes them" is slightly clunkier than "a third."
  3. "For ongoing two-way and broadcast messaging, Telefunc has two primitives and a combination of the two:" — C 8 / N 8 / Overall 8 — "combination" is less precise and "of the two" is wordier than "them."
- **Decision:** Applied → "For ongoing two-way and broadcast messaging, Telefunc has two primitives, plus a third that composes them:" (new Overall 9)
- **Why:** Spells out that the third item is a composition of the first two, which the bullets then confirm, so "composition" is no longer abstract on first read.

### [36] `docs/pages/stream/+Page.mdx` — `Channel`, bullet `new BroadcastChannel()`
- **Original:** "**`new BroadcastChannel()`** — a `Channel` bridged onto a `Broadcast` key: chat rooms, live feeds, presence."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "bridged onto a `Broadcast` key" is dense but defined by context.
- **Candidates:**
  1. "**`new BroadcastChannel()`** — a `Channel` connected to a `Broadcast` key: chat rooms, live feeds, presence." — C 9 / N 9 / Overall 8 — "connected to" is simpler, but drops the deliberate "bridge" concept (a per-client Channel bridged to a shared Broadcast).
  2. "**`new BroadcastChannel()`** — a `Channel` backed by a `Broadcast` key: chat rooms, live feeds, presence." — C 8 / N 9 / Overall 8 — "backed by" implies the Broadcast stores/persists, a subtle meaning shift.
  3. "**`new BroadcastChannel()`** — a per-client `Channel` bridged onto a `Broadcast` key: chat rooms, live feeds, presence." — C 9 / N 8 / Overall 8 — "per-client" adds precision but partly repeats the `new Channel()` bullet (one server / one client).
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** "bridged" is load-bearing Telefunc terminology (a BroadcastChannel *bridges* Channel and Broadcast); the simpler verbs lose that nuance, and target [33]'s "composes them" now primes the reader for it.

### [44] `docs/pages/stream/+Page.mdx` — Authorization, re-check blockquote, sentence 1
- **Original:** "**Consider re-checking.** For sensitive operations, consider re-checking authorization with the captured `user` each time the callback runs."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — repeated "consider" (bold lead + sentence) is slightly redundant.
- **Candidates:**
  1. "**Consider re-checking.** For sensitive operations, re-check authorization with the captured `user` each time the callback runs." — C 9 / N 9 / Overall 9 — the bold lead already frames it as optional, so the sentence can be a direct imperative; removes the duplicate "consider."
  2. "**Consider re-checking.** For sensitive operations, you may want to re-verify authorization with the captured `user` each time the callback runs." — C 9 / N 8 / Overall 8 — avoids both repeats but "you may want to" is wordier and softer than needed.
  3. "**Consider re-checking.** For sensitive operations, re-validate authorization with the captured `user` each time the callback runs." — C 8 / N 8 / Overall 8 — "re-validate" varies the verb but reads slightly more formal than the rest of the doc.
- **Decision:** Applied → "**Consider re-checking.** For sensitive operations, re-check authorization with the captured `user` each time the callback runs." (new Overall 9)
- **Why:** The bold lead carries the "consider," so the sentence drops the redundant second "consider" and the following "Most use cases don't need this." keeps it optional.

### [51] `docs/pages/stream/+Page.mdx` — Cleanup, channels/broadcasts paragraph
- **Original:** "`<Link text="Channels" .../>` and broadcasts expose `onClose()` (and `onOpen()`) directly on the instance — e.g. `dashboard.onClose(() => clearInterval(interval))` in the `<Link text="Channel example" .../>` above; see `<Link href="/channel#lifecycle" />`."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — the example-then-semicolon-then-"see" tail is slightly busy.
- **Candidates:**
  1. "…directly on the instance — e.g. `dashboard.onClose(() => clearInterval(interval))` in the `<Link text="Channel example" .../>` above. See `<Link href="/channel#lifecycle" />`." — C 9 / N 9 / Overall 9 — replaces the busy "; see" tail with a clean separate sentence, matching the "… See <Link/>." pattern used elsewhere on the page.
  2. "…directly on the instance (e.g. `dashboard.onClose(() => clearInterval(interval))` in the `<Link text="Channel example" .../>` above); see `<Link href="/channel#lifecycle" />`." — C 9 / N 8 / Overall 8 — parenthesizing the example helps, but the "; see" tail still trails.
  3. "See `<Link href="/channel#lifecycle" />`: `<Link text="Channels" .../>` and broadcasts expose `onClose()` (and `onOpen()`) directly on the instance — e.g. `dashboard.onClose(() => clearInterval(interval))` in the `<Link text="Channel example" .../>` above." — C 8 / N 7 / Overall 7 — front-loading the cross-reference reads unnaturally.
- **Decision:** Applied → "…directly on the instance — e.g. `dashboard.onClose(() => clearInterval(interval))` in the <Link text="Channel example" href="#channel" /> above. See <Link href="/channel#lifecycle" />." (new Overall 9)
- **Why:** A period before "See" separates the cross-reference from the example, de-cluttering the tail; all three links preserved verbatim.

### [57] `docs/pages/stream/+Page.mdx` — Scaling, sentence 1
- **Original:** "You can scale Telefunc horizontally (multiple instances/containers/machines) by adding sticky sessions and a cross-instance broadcast transport."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "cross-instance broadcast transport" is dense jargon, but appropriate for the topic and linked out.
- **Candidates:**
  1. "You can scale Telefunc horizontally — across multiple instances, containers, or machines — by adding sticky sessions and a cross-instance broadcast transport." — C 9 / N 9 / Overall 8 — converts the slash cluster to an em-dash list, but doesn't touch the flagged jargon, and the parenthetical was already judged to define "horizontally" well.
  2. "You can scale Telefunc horizontally (multiple instances/containers/machines) by adding sticky sessions and a broadcast transport that works across instances." — C 8 / N 8 / Overall 8 — unpacks the jargon but is wordier, and "cross-instance broadcast transport" is the canonical term linked to /stream/scale.
  3. "To scale Telefunc horizontally (multiple instances/containers/machines), add sticky sessions and a cross-instance broadcast transport." — C 8 / N 8 / Overall 8 — imperative recast changes the voice without addressing the flagged jargon.
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** The only documented weakness is the term "cross-instance broadcast transport," which is intentional, canonical, and linked out; the parenthetical was already deemed clear, so reworking it would be a forced change rather than a fix.

## Summary
- **Targets:** 12
- **Applied:** 9 — [1], [8], [15], [16], [28], [32], [33], [44], [51]
- **Retained:** 3 — [12], [36], [57]
- **New score distribution (Overall):** [1] 10; [8] 9; [12] 8 (retained); [15] 9; [16] 9; [28] 9; [32] 9; [33] 9; [36] 8 (retained); [44] 9; [51] 9; [57] 8 (retained).
  - 10/10: 1 target
  - 9/10: 8 targets
  - 8/10: 3 targets (retained)
- All edits changed prose wording only; meaning, technical facts, code logic, and all MDX/JSX (`<Link/>`, `<StreamingBeta/>`, `<NeedsLongRunningServer/>`, inline code, emphasis, trailing line-break spaces, URLs, anchors) preserved and verified by reading back each changed region.
