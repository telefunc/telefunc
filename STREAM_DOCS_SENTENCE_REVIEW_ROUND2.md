# Sentence-by-sentence review of the Telefunc Stream docs — Round 2

This is the **second** review pass over every documentation page introduced by the Telefunc Stream
feature ([PR #264](https://github.com/telefunc/telefunc/pull/264)). The first pass is recorded in
[`STREAM_DOCS_SENTENCE_REVIEW.md`](./STREAM_DOCS_SENTENCE_REVIEW.md) and shipped in
[PR #413](https://github.com/telefunc/telefunc/pull/413). Round 2 re-rates **every** prose sentence on
the now round-1-edited pages, with fresh reviewers, to catch what the first pass missed and to lift any
remaining weak sentences.

Same two axes (1–10):

- **Clarity** — crystal clear, **zero ambiguity**, no fuzzy/vague words, the reader never second-guesses the meaning.
- **Naturalness** — reads like idiomatic, professional JavaScript/TypeScript documentation.

## Round-2 method

- A sentence was **edited** when its clarity *or* naturalness was **≤7**.
- Additionally, round 2 allowed lifting a current **8** to **9–10** with a *low-risk, meaning-identical* rewrite — but only conservatively, with no churn of already-good sentences and no risk of regression.
- Every applied edit was re-rated; all landed **≥8 on both axes**.
- The *second-PR* rule (defer an edit only if its **best** wording still rates ≤7) was triggered by **no** edit — every applied change reached ≥8, so this is a single PR.
- Code inside fenced blocks, inline code identifiers, JSX tags/props, URLs/`href`s, and code-fence comments are out of scope. Edits preserve meaning, MDX/JSX, links, tables, and code **byte-for-byte except the prose words changed**.
- Docs lint (`node docs/check-docs.mjs`) passes after editing: 53 pages, 3 components, 113 internal anchor links resolve.

## Scope and tally

| # | Page | Prose units | Round-2 edits |
|---|------|-------------|---------------|
| 1 | `docs/pages/stream/+Page.mdx` | 60 | 7 |
| 2 | `docs/pages/channel/+Page.mdx` | 96 | 3 |
| 3 | `docs/pages/transport/+Page.mdx` | 37 | 1 |
| 4 | `docs/pages/onClose/+Page.mdx` | 27 | 1 |
| 5 | `docs/pages/close/+Page.mdx` | 29 | 2 |
| 6 | `docs/pages/withContext/+Page.mdx` | 12 | 1 |
| 7 | `docs/pages/serve/+Page.mdx` | 21 | 2 |
| 8 | `docs/pages/Telefunc/+Page.mdx` | 23 | 2 |
| 9 | `docs/pages/provideTelefuncContext/+Page.mdx` | 6 | 0 |
| 10 | `docs/pages/testing/+Page.mdx` | 10 | 0 |
| 11 | `docs/pages/file-download/+Page.mdx` | 45 | 0 |
| 12 | `docs/pages/tanstack-query/+Page.mdx` | 36 | 3 |
| 13 | `docs/pages/rxjs/+Page.mdx` | 21 | 1 |
| 14 | `docs/pages/redis/+Page.mdx` | 10 | 2 |
| 15 | `docs/pages/stream/scale/+Page.mdx` | 31 | 2 |
| 16 | `docs/pages/stream/cloudflare/+Page.mdx` | 60 | 1 |
| 17 | `docs/pages/channel-config/+Page.mdx` | 6 | 0 |
| 18 | `docs/components/NeedsLongRunningServer.mdx` | 3 | 0 |
| | **Total** | **~533** | **28** |

**28 edits applied across 13 files. 0 sentences deferred to a second PR.** Because round 1 had already
removed the broken grammar and typos, round-2 yield is lower and the surviving fixes are subtler:
vague filler words, garden-path constructions, one-off undefined terms, mixed tense/timing, and
terminology drift.

---

# 1. `docs/pages/stream/+Page.mdx` — 7 edits

Most sentences sit at 9/9 after round 1. The seven lifted in round 2:

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 3 | "The word *stream* **denotes** — broadly speaking **and** [as Wikipedia defines it] — **not only** a `ReadableStream`…" → "**Here the** word *stream* **means** — broadly speaking**,** [as Wikipedia defines it] — **not just** a `ReadableStream`…" | C9/N9 | "denotes" is stiff; the "and" linking two parentheticals was awkward; "not just" is plainer than "not only". |
| 18 | "…callbacks **don't have such a** completion signal.)" → "…callbacks **have no such** completion signal.)" | C9/N9 | "don't have such a" is clumsy; "have no such" is tighter. |
| 19 | "…never-ending streams**, if you want** — the only deciding factor is DX." → "…never-ending streams — the only deciding factor is DX." | C9/N9 | "if you want" is filler that weakens the sentence. |
| 26 | "The underlying stream **automatically closes itself** when…" → "The underlying stream **closes itself automatically** when…" | C9/N9 | adverb reads more naturally after the verb phrase. |
| 27 | "…same lifecycle as a channel, **just exposed with a nice** JavaScript DX." → "…same lifecycle as a channel, **only exposed with a nicer** JavaScript DX." | C9/N9 | removes the second adjacent "just"; "nicer" reads as a comparison to the raw channel. |
| 48 | "…before you stream anything back." → "…before you stream anything back **to the client**." | C9/N9 | makes the direction explicit. |
| 57 | "…horizontally (multiple **instances/containers/machines**) by…" → "…horizontally (**across multiple instances, containers, or machines**) by…" | C9/N9 | replaces slash-stacked note-style notation with prose. |

#### Sentence ratings (representative; all 60 rated)
The bullets, primitive labels, "Seamless DX" checklist, and the per-primitive example intros all rate
9/10 on both axes (a label/heading is rarely a perfect 10 because of terseness). Lowest pre-edit
scores were sentences 3 (C7/N6), 18 (N7), 19 (N7), and 57 (N7) — all edited above. No sentence below 8
remains.

**Tally:** 60 reviewed · 7 edited · 0 deferred.

---

# 2. `docs/pages/channel/+Page.mdx` — 3 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 2 | "…primitives enable **all kinds of** stream use cases." → "…primitives enable **every kind of** stream use case." | C9/N9 | "all kinds of" is loose/colloquial; "every kind of" is tighter and more confident. |
| 16 | "…both ends can `send()` and `listen()` until **one side** closes it." → "…until **either side** closes it." | C10/N9 | "either side" precisely conveys that closing by one of the two ends ends it, matching the later "ends when either side closes it". |
| 50 | "`seq` is monotonic per `key`, **useful for** ordering and gap detection." → "`seq` is monotonic per `key`, **which is useful for** ordering and gap detection." | C10/N9 | the clipped trailing appositive becomes a smooth relative clause. |

#### Sentence ratings (highlights; all 96 rated)
This is the longest page and the strongest: the method tables, the generic-type explanation, the
Broadcast/BroadcastChannel prose, the security callouts, and the phone-call/radio-frequency analogies
all rate 9–10. Two exploratory edits ("server-side-only", "client-side too") were reverted as pure
churn. No sentence below 8 remains.

**Tally:** 96 reviewed · 3 edited · 0 deferred.

---

# 3. `docs/pages/transport/+Page.mdx` — 1 edit

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 18 | "…and which **backend** `config.stream.transport = 'channel'` uses." → "…and which **transport** `config.stream.transport = 'channel'` uses." | C10/N9 | "backend" was a one-off undefined term; "transport" is the page's established vocabulary (heading "Channel transport", value `config.channel.transports`). |

The streaming-transport and channel-transport tables, the comparison legend, the "When to use what"
guidance, and the recommended-setup table all rate 9–10. **Tally:** 37 reviewed · 1 edited · 0 deferred.

---

# 4. `docs/pages/onClose/+Page.mdx` — 1 edit

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 15 | "The `onClose()` hook (defined on `context`) is called **when** *all* the streams opened by a telefunction call **close**." → "…is called **once** *all* the streams opened by a telefunction call **have closed**." | C10/N9 | "once … have closed" pins the single-fire-after-the-last-stream timing, removing the "per-stream vs. final" ambiguity of "when … close". |

The `context.onClose()`, `channel.onClose()`, and `context.signal` sub-sections otherwise rate 9–10.
One sentence — "You can listen when Telefunc streams close to:" — was rated **8/8** (the
"listen when … close to:" split momentarily garden-paths, but the list immediately disambiguates).
It sits above the ≤7 edit threshold and every conservative reword either drifted the meaning or added
churn, so it is a deliberate **KEEP**, not an edit.

**Tally:** 27 reviewed · 1 edited · 0 deferred.

---

# 5. `docs/pages/close/+Page.mdx` — 2 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 13 | "…close as soon as possible, **then make sure to manually close the stream –** return `clear` in this example." → "…close as soon as possible, **manually close it yourself —** return `clear` in this example." | C10/N9 | drops the clunky "then make sure to", removes the repeated "stream", and normalizes the en-dash to a spaced em dash. |
| 16 | "There's one catch-all API, `close()`, plus several **per-type** APIs." → "…plus several **per-stream-type** APIs." | C10/N9 | "per-type" was ambiguous ("type of what?"); the table rows are keyed to stream type. |

> **Non-prose flag (not edited; documented for a maintainer):** on the "How to close" line, the
> `close()` link uses `href="/onClose"` while every other `close()` reference points to `/close`. This
> looks like a wrong link target. It was left byte-identical because it is a link `href`, not prose,
> and the correct destination (`/close`, a `/close` anchor, or removing the link) is ambiguous — worth
> a maintainer's confirmation rather than a guess.

**Tally:** 29 reviewed · 2 edited · 0 deferred.

---

# 6. `docs/pages/withContext/+Page.mdx` — 1 edit

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 11 | "Set a key to **isolate calls onto** separate connections:…" → "Set a key to **split calls across** separate connections:…" | C9/N9 | "isolate … onto" is awkward and mildly redundant; "split … across" pairs the verb and preposition naturally. |

Ten alternatives were weighed ("isolate calls onto their own connections", "route calls onto separate
connections", "place calls on separate connections", "give calls their own connections", "separate
calls onto distinct connections", "fan calls out across separate connections", "distribute calls
across separate connections", "put calls on their own connections", "assign calls to separate
connections", "split calls across separate connections") — the last won for the most natural
verb–preposition pairing with zero meaning change. **Tally:** 12 reviewed · 1 edited · 0 deferred.

---

# 7. `docs/pages/serve/+Page.mdx` — 2 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 4 | "It runs in any runtime **without an adapter**." → "It runs in any runtime, **with no adapter required**." | C9/N9 | "without an adapter" briefly garden-paths as "won't run unless…"; the affirmative form reads cleanly. |
| 6 | "**Because** `new Telefunc()` has full-fledged support for Telefunc Stream, whereas `serve()` doesn't support the following:" → "`new Telefunc()` has full-fledged support for Telefunc Stream, whereas `serve()` doesn't support the following:" | C9/N9 | the original opened with "Because…whereas…" and never landed a main clause — a dangling fragment. Dropping "Because" makes it a complete sentence; it still reads as the reason, directly following the recommendation. |

> Edit 6 was initially flagged by the reviewer as a *second-PR candidate* out of caution about flow,
> but its best wording rates 9/9 — well above the ≤7 deferral threshold — so it belongs in this PR.

**Tally:** 21 reviewed · 2 edited · 0 deferred.

---

# 8. `docs/pages/Telefunc/+Page.mdx` — 2 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 10 | "…set in a separate function from the request handler. **(Because it runs in a Durable Object, whereas the request handler doesn't.)**" → "…set in a separate function from the request handler**, because it runs in a Durable Object whereas the request handler doesn't.**" | C10/N9 | folds the standalone "(Because …)" sentence-fragment parenthetical into the main clause. |
| 16 | "Idempotent **(calling it multiple times is safe)**. (The Node.js adapter auto-detects…)" → "Idempotent**: calling it multiple times is safe.** (The Node.js adapter auto-detects…)" | C10/N9 | converting the first parenthetical to a colon gloss removes the jarring back-to-back `(…) (…)` pattern. |

The setup prose, performance tip, methods table, and return section otherwise rate 9–10.
**Tally:** 23 reviewed · 2 edited · 0 deferred.

---

# 9. `docs/pages/provideTelefuncContext/+Page.mdx` — 0 edits

All 6 prose units rate 9–10 on both axes after round 1. The only borderline phrasing — "such as
server-side rendering or unit tests" eliding "during/in" — reads cleanly and was left as **KEEP**.
**Tally:** 6 reviewed · 0 edited · 0 deferred.

---

# 10. `docs/pages/testing/+Page.mdx` — 0 edits

All 10 prose units rate 9–10. "No server, no HTTP, no mocking." is an effective rhetorical fragment
(KEEP). The wire-protocol sentence with its double em-dash interruption parses correctly and its
cadence is deliberate. **Tally:** 10 reviewed · 0 edited · 0 deferred.

---

# 11. `docs/pages/file-download/+Page.mdx` — 0 edits

All 45 prose units rate 8–10. The single 8/8 — "These APIs check for an internal marker that only real
`File` / `Blob` instances have." — keeps its deliberately hand-wavy "internal marker"; every rewrite
traded the hand-wave for either inaccuracy or churn. One trial edit (em-dash → comma in the
"Limitations" paragraph) was tested and reverted as a non-improvement; the file is byte-identical to
its round-1 state. **Tally:** 45 reviewed · 0 edited · 0 deferred.

---

# 12. `docs/pages/tanstack-query/+Page.mdx` — 3 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 7 | "…keys prefixed with `global:` **are special because they** invalidate globally:…" → "…keys prefixed with `global:` invalidate globally:…" | C9/N9 | "are special because" is an empty hedge; the sentence now states the behavior directly. |
| 9/10 | "…sent to the server **then broadcasted to the clients that use a query key that matches**. Learn more **at:** `<Link/>`." → "…sent to the server **and then broadcast to every client whose query key matches**. Learn more **at** `<Link/>`." | C9/N9 | "broadcasted" → "broadcast" (term used elsewhere); untangles the double-relative "the clients that use a query key that matches"; adds "and"; drops the stray "at:" colon. |
| 29 | "…on the current client **for matching local keys**." → "…on the current client **for the matching local keys**." | C9/N9 | the definite article makes it a concrete set rather than a vague category. |

**Tally:** 36 reviewed · 3 edited · 0 deferred.

---

# 13. `docs/pages/rxjs/+Page.mdx` — 1 edit

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 1 | "…directly between client and server, **in both directions and with all operators**." → "…directly between client and server **— in both directions, and every RxJS operator works across the boundary**." | C9/N9 | the terse trailing "with all operators" momentarily reads as "operators of what?"; the em-dash split names "RxJS operator" and "across the boundary" explicitly. |

The remaining 20 units (validation callout, multicast notes, Angular section) rate 9–10.
**Tally:** 21 reviewed · 1 edited · 0 deferred.

---

# 14. `docs/pages/redis/+Page.mdx` — 2 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 2 | "…fan-out across server instances — `publish()` on **one server instance** reaches `subscribe()` on **all other server instances**." → "…fan-out across server instances — a `publish()` on **any instance** reaches every `subscribe()` on **all the others**." | C9/N9 | removes the threefold "server instance" repetition while keeping the fan-out semantics. |
| 6 | "…set custom options such as TLS or **retry strategy**." → "…set custom options such as TLS or **a retry strategy**." | C9/N9 | conservative article fix lifting a current-8. |

**Tally:** 10 reviewed · 2 edited · 0 deferred.

---

# 15. `docs/pages/stream/scale/+Page.mdx` — 2 edits

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 17 | "Most teams pair Telefunc with a **regular** long-running server tier when…" → "Most teams pair Telefunc with a long-running server tier when…" | C9/N9 | "regular" is vague filler; "long-running" already supplies the contrast with serverless. |
| 29 | "[Cloudflare Workers] **is fundamentally different**: `telefunc/cloudflare` routes channels through Durable Objects…" → "[Cloudflare Workers] **takes a different approach**: …" | C9/N9 | "fundamentally different" is a vague intensifier; "takes a different approach" names the concrete contrast. |

The sticky-sessions deep-dive, the AWS target-group steps, and the `BroadcastTransport` interface notes
rate 9–10. **Tally:** 31 reviewed · 2 edited · 0 deferred.

---

# 16. `docs/pages/stream/cloudflare/+Page.mdx` — 1 edit

| # | Before → After | New rating | Reason |
|---|----------------|-----------|--------|
| 16 | "…it must live on the **same Durable Object as** the WebSocket connection." → "…it must live on the **same Durable Object that holds** the WebSocket connection." | C10/N10 | "same … as" reads as comparing two objects rather than asserting co-location; "that holds" makes co-location explicit and matches the "holds the connection" wording already used in the Setup blockquote. |

This is the densest architecture page; the regions table, session-affinity walkthrough, distributed-
broadcast steps, delivery-guarantee tables, and hibernation notes all rate 9–10. **Tally:** 60 reviewed
· 1 edited · 0 deferred.

---

# 17. `docs/pages/channel-config/+Page.mdx` — 0 edits

All 6 prose units rate 9 on both axes; nothing fell to ≤7 and no low-risk lift to 10 was available
without churn. The troubleshooting bullets ("Slow/flaky clients dropping with `NetworkError` → raise
`reconnectTimeout`", etc.) are terse by design and read cleanly. **Tally:** 6 reviewed · 0 edited · 0 deferred.

---

# 18. `docs/components/NeedsLongRunningServer.mdx` — 0 edits

All 3 prose units (the bold lead, the serverless explanation, the cross-reference) rate 9 on both axes.
**Tally:** 3 reviewed · 0 edited · 0 deferred.

---

# Borderline items, resolved

- **`serve` "Because…whereas…" fragment** — flagged as a possible second-PR candidate, but its best
  wording rates 9/9, so it was fixed in this PR (drop "Because").
- **`onClose` "listen when … close to:"** — rated 8/8; above the edit threshold, and no reword beat the
  original without risk, so it is a deliberate KEEP (not an edit, not a deferral).
- **`close` `close()` link → `/onClose`** — a non-prose link-target issue, left untouched and flagged
  above for a maintainer because the intended destination is ambiguous.

# Why so few 10/10 ratings (still)

As in round 1, most kept sentences sit at **9, not 10**. A 10 is reserved for sentences impossible to
improve on either axis — almost always the short, unambiguous labels and section headings
("`## See also`", "The mutation succeeds.", "Messages are delivered in order."). Everything longer
carries at least a one-point reason: a trailing modifier, a compact anaphor, a telegraphic table cell,
a mild bit of jargon, or em-dash density. Those are the honest gap between *good* and *flawless*, not
defects — which is why they are documented rather than churned.

# Second PR

No edit's best wording stayed ≤7, so — as in round 1 — there are **no deferred changes**. This round
ships as a single PR.
