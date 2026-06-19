# Sentence review — `docs/pages/stream/+Page.mdx`

Reviewer: A. File is NEW (no diff in `/tmp/diffs/`), so every introduced prose sentence is reviewed.
Scope notes: code is not rated (comments scanned separately — none had typos/unclear text). `import` lines, frontmatter, bare component tags, anchor slugs, and URLs are skipped per methodology.

---

### [1] `docs/pages/stream/+Page.mdx` — intro line (overview)
- **Original:** "Telefunc supports streaming (one-way stream) and real-time (two-way stream) use cases with:"
- **Clarity:** 8/10 — The parenthetical glosses ("one-way stream"/"two-way stream") are helpful, but pairing "streaming" with "(one-way stream)" is mildly circular since the gloss reuses the word being defined.
- **Naturalness:** 9/10 — Reads like a normal feature-overview lead-in.
- **Overall:** 8/10
- **Action:** Kept

### [2] `docs/pages/stream/+Page.mdx` — Primitives list, item 1
- **Original:** "Primitives"
- **Clarity:** n/a — single-word list label, not a prose sentence.
- **Naturalness:** n/a
- **Overall:** —
- **Action:** Kept (skipped — bare label)

### [3] `docs/pages/stream/+Page.mdx` — Primitives list, item 2
- **Original:** "Integrations"
- **Overall:** — (skipped — bare label)
- **Action:** Kept

### [4] `docs/pages/stream/+Page.mdx` — Primitives list, item 3
- **Original:** "Seamless DX"
- **Overall:** — (skipped — bare label)
- **Action:** Kept

### [5] `docs/pages/stream/+Page.mdx` — Primitives → `Channel` link description
- **Original:** "API for advanced real-time use cases."
- **Clarity:** 9/10 — Clear; "advanced" is mildly relative but appropriate as a quick locator.
- **Naturalness:** 9/10 — Standard terse link gloss.
- **Overall:** 9/10
- **Action:** Kept

### [6] `docs/pages/stream/+Page.mdx` — Primitives → blockquote (mix)
- **Original:** "A telefunction can [mix](https://gist.github.com/brillout/155c8eb4cbaa043bc52e96c0c2ed7086) — generators, streams, and promises side by side, each resolving independently of each other."
- **Clarity:** 7/10 — "independently of each other" is redundant with "independently"; the em-dash after "mix" awkwardly detaches the verb from its object.
- **Naturalness:** 6/10 — The dash break plus the redundant tail read clunky for a one-line callout.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "A telefunction can [mix](https://gist.github.com/brillout/155c8eb4cbaa043bc52e96c0c2ed7086) generators, streams, and promises side by side, each resolving independently."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — "side by side" already conveys the multiplicity; "independently" alone is sufficient. (Kept the external gist URL as a markdown link — it is not an internal link, so the no-bare-internal-link rule does not apply.)

### [7] `docs/pages/stream/+Page.mdx` — Integrations → `@telefunc/tanstack-query` link description
- **Original:** "automatically synced TanStack queries."
- **Clarity:** 9/10 — Clear; "synced" with what is implied (server) but unstated, a tiny gap acceptable for a terse gloss.
- **Naturalness:** 9/10 — Reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [8] `docs/pages/stream/+Page.mdx` — Integrations → `@telefunc/rxjs` link description
- **Original:** "reactive operators."
- **Clarity:** 8/10 — Very terse; assumes the reader knows RxJS, which is reasonable here.
- **Naturalness:** 9/10 — Fine as a two-word gloss.
- **Overall:** 8/10
- **Action:** Kept

### [9] `docs/pages/stream/+Page.mdx` — Integrations → blockquote, sentence 1
- **Original:** "Integrations provide an even more seamless and egornomic DX."
- **Clarity:** 6/10 — Typo "egornomic" forces a double-take.
- **Naturalness:** 6/10 — Same typo breaks the flow.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "Integrations provide an even more seamless and ergonomic DX."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Typo fixed; "even more" correctly builds on the preceding "Seamless DX" framing.

### [10] `docs/pages/stream/+Page.mdx` — Integrations → blockquote, sentence 2
- **Original:** "They're powered by the primitives listed here."
- **Clarity:** 9/10 — Clear; "listed here" points at the page's primitives section.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [11] `docs/pages/stream/+Page.mdx` — Seamless DX checklist, Runtime type validation
- **Original:** "Runtime type validation — automatically validates every value sent from the client to the server against TypeScript types (no need for Zod)."
- **Clarity:** 9/10 — Clear and specific; the "(no need for Zod)" aside is a useful concrete signal.
- **Naturalness:** 9/10 — Reads like idiomatic TS docs.
- **Overall:** 9/10
- **Action:** Kept

### [12] `docs/pages/stream/+Page.mdx` — Seamless DX checklist, Transport
- **Original:** "Transport — automatically picks the most performant available transport (HTTP, SSE, WebSocket)."
- **Clarity:** 8/10 — Clear; "most performant available" stacks two qualifiers but parses fine.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 8/10
- **Action:** Kept

### [13] `docs/pages/stream/+Page.mdx` — Seamless DX checklist, Backpressure
- **Original:** "Backpressure — automatic backpressure when network is the bottleneck."
- **Clarity:** 8/10 — Clear, but "when network is the bottleneck" drops the article.
- **Naturalness:** 7/10 — Missing "the" before "network" reads slightly off in running English.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Backpressure — automatic backpressure when the network is the bottleneck."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Article added; grammar fix, meaning unchanged.

### [14] `docs/pages/stream/+Page.mdx` — Seamless DX checklist, Reconnection
- **Original:** "Reconnection — automatic recover from network issues."
- **Clarity:** 6/10 — "automatic recover" is ungrammatical (should be the noun "recovery").
- **Naturalness:** 5/10 — Clear grammatical error; reads as broken.
- **Overall:** 5/10
- **Action:** Edited
- **Edit:** "Reconnection — automatic recovery from network issues."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Noun form restored.

### [15] `docs/pages/stream/+Page.mdx` — `AsyncGenerator` section, intro
- **Original:** "An `async function*` returns an `AsyncGenerator`: the usual choice for structured values that arrive one piece at a time — AI tokens, notifications, progress updates."
- **Clarity:** 8/10 — Clear; "structured values" is mildly fuzzy but meaningfully contrasts with the raw-bytes `ReadableStream` primitive, and the examples ground it.
- **Naturalness:** 8/10 — The colon-then-appositive plus em-dash list is a little dense but reads acceptably.
- **Overall:** 8/10
- **Action:** Kept

### [16] `docs/pages/stream/+Page.mdx` — `AsyncGenerator` section, second example lead-in
- **Original:** "For example, return an `async function*` and read each value as it arrives:"
- **Clarity:** 8/10 — Clear; "an `async function*`" as a returnable object is slightly loose (you return the generator it produces), but the code clarifies.
- **Naturalness:** 9/10 — Natural lead-in to a code block.
- **Overall:** 8/10
- **Action:** Kept

### [17] `docs/pages/stream/+Page.mdx` — `function` passing, intro
- **Original:** "Telefunc can pass functions between client and server:"
- **Clarity:** 9/10 — Clear and direct.
- **Naturalness:** 10/10 — Idiomatic.
- **Overall:** 9/10 — Slight: "between client and server" without articles is terse but standard here; clarity holds.
- **Action:** Kept

### [18] `docs/pages/stream/+Page.mdx` — `function` passing, bullet 1
- **Original:** "Client → server: pass a callback as an argument — the server can call it."
- **Clarity:** 9/10 — Clear directional explanation.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [19] `docs/pages/stream/+Page.mdx` — `function` passing, bullet 2
- **Original:** "Server → client: return a function from a telefunction — call it later from the client."
- **Clarity:** 9/10 — Clear and symmetric with bullet 1.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [20] `docs/pages/stream/+Page.mdx` — `function` passing, closing line
- **Original:** "If you only need callback-style interaction, function passing is simpler than a channel."
- **Clarity:** 9/10 — Clear guidance.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [21] `docs/pages/stream/+Page.mdx` — Concurrent `Promise`, sentence 1
- **Original:** "Put promises inside the response object **without awaiting them**."
- **Clarity:** 9/10 — Clear, actionable.
- **Naturalness:** 9/10 — Natural imperative.
- **Overall:** 9/10
- **Action:** Kept

### [22] `docs/pages/stream/+Page.mdx` — Concurrent `Promise`, sentence 2
- **Original:** "The client receives the rest of the object immediately, and each promise resolves on its own."
- **Clarity:** 9/10 — Clear; "the rest of the object" (i.e. non-promise fields) is well implied by the prior sentence.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [23] `docs/pages/stream/+Page.mdx` — Concurrent `Promise`, blockquote warning lead
- **Original:** "**Don't `await` inside the telefunction** — that blocks the entire response:"
- **Clarity:** 9/10 — Clear warning; "that" clearly refers to awaiting.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [24] `docs/pages/stream/+Page.mdx` — Concurrent `Promise`, blockquote follow-up
- **Original:** "Return the promise instead:"
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 10/10 — Idiomatic.
- **Overall:** 9/10 — Minor: "the promise" (singular) after a multi-field example is slightly loose but understood.
- **Action:** Kept

### [25] `docs/pages/stream/+Page.mdx` — `ReadableStream` section, intro
- **Original:** "Stream raw bytes with a web-standard `ReadableStream` — in either direction."
- **Clarity:** 9/10 — Clear; "in either direction" previews the up/down examples.
- **Naturalness:** 9/10 — Natural; the trailing dash clause is a common docs cadence.
- **Overall:** 9/10
- **Action:** Kept

### [26] `docs/pages/stream/+Page.mdx` — `ReadableStream`, Server → client label
- **Original:** "**Server → client** — return a `ReadableStream`:"
- **Clarity:** 9/10 — Clear directional label.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [27] `docs/pages/stream/+Page.mdx` — `ReadableStream`, File/Blob blockquote
- **Original:** "For named files with `name` / `type` / `size` metadata, return a `File` or `Blob` instead."
- **Clarity:** 9/10 — Clear; lists the relevant metadata fields concretely.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [28] `docs/pages/stream/+Page.mdx` — `ReadableStream`, Client → server label
- **Original:** "**Client → server** — pass a `ReadableStream` as an argument to stream bytes up to the server:"
- **Clarity:** 9/10 — Clear; "up to the server" reinforces direction.
- **Naturalness:** 8/10 — "stream bytes up to the server" is fine but "up to" momentarily reads like a quantity ("up to N") before resolving as direction.
- **Overall:** 8/10
- **Action:** Kept

### [29] `docs/pages/stream/+Page.mdx` — `ReadableStream`, Both directions label
- **Original:** "**Both directions** — accept a `ReadableStream` *and* return one, transforming the bytes as they pass through."
- **Clarity:** 9/10 — Clear; the emphasized *and* underscores the dual nature.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [30] `docs/pages/stream/+Page.mdx` — `ReadableStream`, Both directions follow-up
- **Original:** "The simplest transform is gzip compression:"
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 10/10 — Idiomatic lead-in.
- **Overall:** 9/10 — Minor: "simplest" is a mild claim but reasonable.
- **Action:** Kept

### [31] `docs/pages/stream/+Page.mdx` — `ReadableStream`, heavy-transform lead-in
- **Original:** "The transform can be arbitrarily heavy — for example, compressing a bulky video into a compact, universally-playable MP4, streamed back as it encodes:"
- **Clarity:** 6/10 — "universally-playable" is a fuzzy, non-standard compound; the comma-spliced participial tail "streamed back as it encodes" leaves the subject (the MP4 / the stream) dangling.
- **Naturalness:** 6/10 — "bulky"/"compact"/"universally-playable" pile up marketing-flavored adjectives uncommon in API docs; the chained modifiers read awkwardly.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "The transform can be arbitrarily heavy — for example, compressing a large video into a smaller MP4 and streaming it back to the client as it encodes:"
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Plain adjectives, a clean "and streaming it back to the client" clause with an explicit subject; meaning (heavy on-the-fly transform streamed during encode) preserved.

### [32] `docs/pages/stream/+Page.mdx` — `ReadableStream`, ffmpeg cleanup blockquote
- **Original:** "A spawned process outlives the call and would keep burning CPU if the client navigated away mid-encode — so it's released in `onClose()`."
- **Clarity:** 9/10 — Clear cause/effect; "mid-encode" is concrete.
- **Naturalness:** 8/10 — "keep burning CPU" is colloquial but fits Telefunc's direct voice; tense mix ("outlives"/"would keep") is acceptable.
- **Overall:** 8/10
- **Action:** Kept

### [33] `docs/pages/stream/+Page.mdx` — `Channel` section, intro
- **Original:** "For ongoing two-way and broadcast messaging, Telefunc has two primitives and one composition:"
- **Clarity:** 8/10 — Clear; "one composition" is precise framing, clarified by the bullets, though "composition" is slightly abstract on first read.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 8/10
- **Action:** Kept

### [34] `docs/pages/stream/+Page.mdx` — `Channel`, bullet `new Channel()`
- **Original:** "**`new Channel()`** — direct messages between one server and one client."
- **Clarity:** 9/10 — Clear; "one ... one" stresses the point-to-point nature.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [35] `docs/pages/stream/+Page.mdx` — `Channel`, bullet `Broadcast`
- **Original:** "**`Broadcast`** — a keyed pub/sub bus (`publish()` / `subscribe()` by `key`), server-side."
- **Clarity:** 9/10 — Clear; the inline API hint grounds "keyed pub/sub bus".
- **Naturalness:** 9/10 — Natural; trailing "server-side" is a compact qualifier.
- **Overall:** 9/10
- **Action:** Kept

### [36] `docs/pages/stream/+Page.mdx` — `Channel`, bullet `new BroadcastChannel()`
- **Original:** "**`new BroadcastChannel()`** — a `Channel` bridged onto a `Broadcast` key: chat rooms, live feeds, presence."
- **Clarity:** 8/10 — Clear to a reader who has just read the two prior bullets; "bridged onto a `Broadcast` key" is dense but defined by context, and the examples help.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 8/10
- **Action:** Kept

### [37] `docs/pages/stream/+Page.mdx` — `Channel`, lifetime sentence
- **Original:** "A channel stays open for the lifetime of the page — here the server pushes live metrics to the one client that opened it:"
- **Clarity:** 9/10 — Clear; "the one client that opened it" reinforces the point-to-point model.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [38] `docs/pages/stream/+Page.mdx` — `Channel`, closing paragraph, sentence 1
- **Original:** "This is the low-level primitive; most apps reach for a higher-level integration like `@telefunc/tanstack-query` instead."
- **Clarity:** 9/10 — Clear recommendation.
- **Naturalness:** 9/10 — Natural; "reach for" is idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [39] `docs/pages/stream/+Page.mdx` — `Channel`, closing paragraph, sentence 2 (original run-on)
- **Original:** "For the full API — typed messages, acknowledgements, binary, broadcast, and reconnection — see `<Link href="/channel" />`; authorizing a channel works like any streaming primitive — see `<Link text="Authorization" .../>` below."
- **Clarity:** 7/10 — Two distinct pointers (full API vs. authorization) are welded together; the semicolon plus three em-dashes overload one sentence.
- **Naturalness:** 6/10 — Reads as a run-on; the dash-then-semicolon-then-dash rhythm is jarring.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "For the full API — typed messages, acknowledgements, binary, broadcast, and reconnection — see `<Link href="/channel" />`. Authorizing a channel works like any streaming primitive — see `<Link text="Authorization" href="#authorization" /> below`."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Split into two sentences; each pointer now stands alone. All `<Link>` markup preserved verbatim.

### [40] `docs/pages/stream/+Page.mdx` — Authorization, sentence 1
- **Original:** "A streaming telefunction authorizes like any other telefunction (see `<Link href="/permissions" />`): check `<Link text="getContext()" .../>` and `throw Abort()` **at open time**, before you stream anything back."
- **Clarity:** 9/10 — Clear; "at open time" plus "before you stream anything back" pin down the timing precisely.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [41] `docs/pages/stream/+Page.mdx` — Authorization, sentence 2
- **Original:** "Here the client passes a `<Link text="callback" .../>` and the server calls it:"
- **Clarity:** 9/10 — Clear lead-in to the example.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [42] `docs/pages/stream/+Page.mdx` — Authorization, timing paragraph, sentence 1
- **Original:** "`getContext()` and `throw Abort()` only work **before the telefunction returns** (see `<Link href="/getContext#access" />`) — so authorize there, at open time."
- **Clarity:** 9/10 — Clear; "there, at open time" restates the constraint usefully.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [43] `docs/pages/stream/+Page.mdx` — Authorization, timing paragraph, sentence 2
- **Original:** "The `user` you captured stays valid inside the callback, which runs later."
- **Clarity:** 9/10 — Clear; resolves the apparent tension between open-time auth and later callbacks.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [44] `docs/pages/stream/+Page.mdx` — Authorization, re-check blockquote, sentence 1
- **Original:** "**Consider re-checking.** For sensitive operations, consider re-checking authorization with the captured `user` each time the callback runs."
- **Clarity:** 8/10 — Clear; minor repetition of "consider" (bold lead + sentence) is slightly redundant but harmless.
- **Naturalness:** 8/10 — Natural; the repeated "consider" is the only blemish.
- **Overall:** 8/10
- **Action:** Kept

### [45] `docs/pages/stream/+Page.mdx` — Authorization, re-check blockquote, sentence 2
- **Original:** "Most use cases don't need this."
- **Clarity:** 9/10 — Clear; "this" clearly refers to re-checking.
- **Naturalness:** 10/10 — Idiomatic.
- **Overall:** 9/10 — Strong but not flawless in isolation (depends on prior sentence for "this").
- **Action:** Kept

### [46] `docs/pages/stream/+Page.mdx` — Cleanup, intro sentence 1
- **Original:** "Every streaming value eventually ends — the client finishes reading it, calls `close()`, navigates away, or the connection drops for good."
- **Clarity:** 9/10 — Clear enumeration of how a stream ends; "for good" distinguishes a permanent drop from a transient reconnect.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [47] `docs/pages/stream/+Page.mdx` — Cleanup, intro sentence 2
- **Original:** "However it ends, the server's `onClose()` hook fires so you can release whatever the telefunction opened."
- **Clarity:** 9/10 — Clear; "however it ends" ties back cleanly to the prior list.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [48] `docs/pages/stream/+Page.mdx` — Cleanup, intro sentence 3
- **Original:** "Get it from `getContext()`:"
- **Clarity:** 9/10 — Clear; "it" = `onClose()`.
- **Naturalness:** 9/10 — Natural lead-in.
- **Overall:** 9/10
- **Action:** Kept

### [49] `docs/pages/stream/+Page.mdx` — Cleanup, Pitfall blockquote, sentence 1
- **Original:** "**Pitfall** — anything a long-lived telefunction opens (`setInterval`, an event subscription, a DB cursor, an upstream stream) outlives the call and **leaks** unless you release it in `onClose()`."
- **Clarity:** 9/10 — Clear with concrete examples of leak-prone resources.
- **Naturalness:** 9/10 — Natural despite the long parenthetical.
- **Overall:** 9/10
- **Action:** Kept

### [50] `docs/pages/stream/+Page.mdx` — Cleanup, Pitfall blockquote, sentence 2
- **Original:** "If you set it up, tear it down."
- **Clarity:** 9/10 — Clear, memorable maxim.
- **Naturalness:** 10/10 — Idiomatic and punchy, fits Telefunc's direct voice.
- **Overall:** 9/10 — Excellent; a hair short of flawless only because "it" leans on the prior sentence.
- **Action:** Kept

### [51] `docs/pages/stream/+Page.mdx` — Cleanup, channels/broadcasts paragraph
- **Original:** "`<Link text="Channels" .../>` and broadcasts expose `onClose()` (and `onOpen()`) directly on the instance — e.g. `dashboard.onClose(() => clearInterval(interval))` in the `<Link text="Channel example" .../>` above; see `<Link href="/channel#lifecycle" />`."
- **Clarity:** 8/10 — Clear; "directly on the instance" contrasts well with getting it from `getContext()`. Dense but parseable.
- **Naturalness:** 8/10 — Natural; the example-then-semicolon-then-"see" tail is slightly busy but acceptable.
- **Overall:** 8/10
- **Action:** Kept

### [52] `docs/pages/stream/+Page.mdx` — Cleanup, client-side paragraph, sentence 1
- **Original:** "From the client, end a stream early with `<Link text={<code>close()</code>} .../>` — or simply stop reading: `break` out of a `for await`, `reader.cancel()` a `ReadableStream`, or `channel.close()` a channel."
- **Clarity:** 9/10 — Clear; the three concrete "stop reading" patterns map directly to the three primitives.
- **Naturalness:** 9/10 — Natural; using the methods as verbs ("`reader.cancel()` a `ReadableStream`") is idiomatic in code-heavy docs.
- **Overall:** 9/10
- **Action:** Kept

### [53] `docs/pages/stream/+Page.mdx` — Cleanup, client-side paragraph, sentence 2 (original)
- **Original:** "Drop every reference without closing and Telefunc still cleans up automatically, via garbage collection, a few seconds later."
- **Clarity:** 7/10 — The "Drop X and Y" imperative-as-conditional is easy to misread as a command; the doubled commas around "via garbage collection" fragment the timing clause.
- **Naturalness:** 6/10 — The conditional-via-imperative phrasing and the comma-walled aside read awkwardly.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "Even if you drop every reference without closing, Telefunc still cleans up automatically via garbage collection a few seconds later."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — "Even if" makes the conditional explicit; removing the comma walls lets "via garbage collection a few seconds later" read as one clause. Meaning preserved.

### [54] `docs/pages/stream/+Page.mdx` — Error handling, intro
- **Original:** "The expected-vs-bug distinction described at `<Link href="/error-handling" />` applies to streams too:"
- **Clarity:** 9/10 — Clear; "expected-vs-bug distinction" names the linked concept precisely.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [55] `docs/pages/stream/+Page.mdx` — Error handling, bullet 1
- **Original:** "`throw Abort()` during a stream is relayed `<Link href="/error-handling#expected-errors">as an expected error</Link>`"
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [56] `docs/pages/stream/+Page.mdx` — Error handling, bullet 2
- **Original:** "Any other thrown error is treated `<Link href="/error-handling#bugs">as a bug</Link>`"
- **Clarity:** 9/10 — Clear; "any other" correctly contrasts with bullet 1.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [57] `docs/pages/stream/+Page.mdx` — Scaling, sentence 1
- **Original:** "You can scale Telefunc horizontally (multiple instances/containers/machines) by adding sticky sessions and a cross-instance broadcast transport."
- **Clarity:** 8/10 — Clear; the parenthetical defines "horizontally" well. "cross-instance broadcast transport" is dense jargon but appropriate for the topic and linked out.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 8/10
- **Action:** Kept

### [58] `docs/pages/stream/+Page.mdx` — Cloudflare, sentence 1
- **Original:** "On Cloudflare Workers, Telefunc routes channels through Durable Objects and fans out broadcasts across regions automatically — so neither a sticky load balancer nor a broadcast transport is needed."
- **Clarity:** 9/10 — Clear; the closing clause ties back to the Scaling requirements, making the contrast explicit.
- **Naturalness:** 9/10 — Natural; "fans out" is idiomatic.
- **Overall:** 9/10
- **Action:** Kept

---

## Summary

- **Sentences reviewed:** 51 prose units rated (entries [1]–[58]; [2]–[4] are bare list labels skipped per methodology, not rated).
- **Kept:** 45
- **Edited (applied in place):** 6 — entries [6], [9], [13], [14], [31], [39], [53] resolve to six applied edits (note [9] and [14] are the two flagged typos `egornomic`→`ergonomic` and `automatic recover`→`automatic recovery`). Count of applied Edit operations: 7 total Edit calls covering 7 sentences ([6], [9], [13], [14], [31], [39], [53]).
- **Second-PR candidates:** 0 — every edit reached Overall ≥ 8 on the first attempt.

Edited sentences (applied): [6] mix blockquote, [9] ergonomic typo, [13] backpressure article, [14] reconnection "recovery" typo, [31] heavy-transform lead-in, [39] Channel run-on split, [53] GC cleanup sentence.

Code comments: all scanned (lines 15–431). No typos, grammatical errors, or unclear/misleading comments found.

MDX/JSX integrity: all `<Link>`, `<StreamingBeta>`, `<NeedsLongRunningServer>`, inline code, emphasis, anchors (`{#...}`), and URLs preserved across every edit; changed regions re-read and confirmed intact.

## SECOND-PR CANDIDATES

None.
