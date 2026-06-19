# Sentence review — H: redis, testing, components

Reviewer scope (all NEW files, whole-file prose review):
- `docs/pages/redis/+Page.mdx`
- `packages/redis/README.md`
- `docs/pages/testing/+Page.mdx`
- `docs/components/NeedsLongRunningServer.mdx`
- `docs/components/StreamingBeta.mdx`

Methodology: `/home/user/telefunc/docs-review/METHODOLOGY.md`. Every introduced prose sentence is rated Clarity / Naturalness / Overall (1–10). Reasons given for any score < 10. Edits applied in place when Overall ≤ 7 and the edit reaches ≥ 8.

---

## `docs/pages/redis/+Page.mdx`

### [1] `docs/pages/redis/+Page.mdx` — top label
- **Original:** "**Environment**: server."
- **Clarity:** 10/10
- **Naturalness:** 9/10 — Terse label fragment rather than a full sentence; standard recurring convention across Telefunc pages, so acceptable, but not prose-flawless.
- **Overall:** 9/10
- **Action:** Kept

### [2] `docs/pages/redis/+Page.mdx` — intro
- **Original:** "Redis-backed broadcast fan-out across Telefunc instances — `publish()` on one instance reaches `subscribe()` on every other instance subscribed to the same key."
- **Clarity:** 9/10 — Dense but precise; "subscribed to the same key" at the very end forces a slight re-parse of the long subject "`subscribe()` on every other instance."
- **Naturalness:** 9/10 — Reads like idiomatic API docs; the noun-phrase opener (no verb) is a common doc-blurb style.
- **Overall:** 9/10
- **Action:** Kept

### [3] `docs/pages/redis/+Page.mdx` — Setup, after code
- **Original:** "That swaps the default in-memory broadcast transport for one backed by Redis Pub/Sub."
- **Clarity:** 9/10 — "That" refers to the preceding `installRedis(redis)` call; antecedent is clear from context but pronominal.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [4] `docs/pages/redis/+Page.mdx` — Setup, after code
- **Original:** "All subscribers across the cluster observe the same publish order for a given key."
- **Clarity:** 9/10 — "observe the same publish order" is precise; "for a given key" scoping is clear. Minor: "publish order" is a compact technical noun phrase a newcomer may pause on.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [5] `docs/pages/redis/+Page.mdx` — Sharing an existing client
- **Original:** "Pass an existing [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options such as TLS or retry strategy."
- **Clarity:** 9/10 — Clear; "Redis or Cluster instance" maps to ioredis's two client classes, which a reader unfamiliar with ioredis might not immediately recognize as type names.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [6] `docs/pages/redis/+Page.mdx` — Sharing an existing client, after code
- **Original:** "Internally, Telefunc calls `duplicate()` on the client to open a dedicated subscriber connection — your instance is never mutated or disconnected."
- **Clarity:** 9/10 — Clear; "your instance" = the ioredis client you passed in, evident from context.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [7] `docs/pages/redis/+Page.mdx` — Sharing an existing client, after code
- **Original:** "You can continue to use it alongside Telefunc without interference."
- **Clarity:** 9/10 — Clear; "without interference" slightly overlaps the prior clause ("never mutated or disconnected"), so it adds reassurance more than new information.
- **Naturalness:** 9/10 — Natural, but mildly redundant given the preceding sentence.
- **Overall:** 9/10
- **Action:** Kept

### [8] `docs/pages/redis/+Page.mdx` — Channels
- **Original:** "`Channel` is per-instance."
- **Clarity:** 9/10 — Compact; "per-instance" is exact but assumes the reader knows "instance" means a server process/node.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [9] `docs/pages/redis/+Page.mdx` — Channels
- **Original:** "Reconnects must land on the instance holding the channel's state, so multi-instance deployments need sticky sessions at the load balancer."
- **Clarity:** 9/10 — Clear cause→effect; "must land on" is vivid and precise.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [10] `docs/pages/redis/+Page.mdx` — Channels
- **Original:** "See <Link href="/stream/scale" />."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10 — Standard cross-reference; flawless.
- **Action:** Kept

---

## `packages/redis/README.md`

### [11] `packages/redis/README.md` — intro
- **Original:** "Redis-backed broadcast fan-out for Telefunc — publishes on any instance reach subscribers on every other instance."
- **Clarity:** 6/10 — "publishes" used as a plural noun ("publish operations") is unclear; the bare-verb-as-noun reads like a typo and forces a re-read.
- **Naturalness:** 6/10 — "publishes ... reach subscribers" is grammatically awkward; clashes with the page version which says "`publish()` on one instance reaches `subscribe()`".
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "Redis-backed broadcast fan-out for Telefunc — a `publish()` on any instance reaches subscribers on every other instance."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Mirrors the page intro's clearer phrasing; "a `publish()` ... reaches" is unambiguous. Not 10 because it stays a verb-less blurb opener.

### [12] `packages/redis/README.md` — Setup, after code
- **Original:** "That swaps Telefunc's default in-memory broadcast transport for Redis Pub/Sub."
- **Clarity:** 9/10 — "That" = the `installRedis(redis)` call; pronominal but clear in context.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [13] `packages/redis/README.md` — Setup, after code
- **Original:** "All subscribers across the cluster observe the same publish order for a given key."
- **Clarity:** 9/10 — Same as [4]: "publish order" is a compact technical phrase a newcomer may pause on; otherwise precise.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [14] `packages/redis/README.md` — Setup, after code
- **Original:** "`Channel` is per-instance — its reconnects need to land on the instance holding the channel's state."
- **Clarity:** 8/10 — "its reconnects" (a channel's reconnects) is slightly loose; the page version's "Reconnects must land on the instance holding the channel's state" reads cleaner.
- **Naturalness:** 8/10 — "its reconnects need to land" is acceptable but a touch awkward versus "must land."
- **Overall:** 8/10
- **Action:** Kept (Overall > 7; would only lightly improve, kept to respect the edit threshold)

### [15] `packages/redis/README.md` — Setup, after code
- **Original:** "Pair this package with sticky sessions at the load balancer; see [Scaling](https://telefunc.com/scaling)."
- **Clarity:** 9/10 — Clear directive; "Pair this package with sticky sessions" is concise.
- **Naturalness:** 9/10 — Natural README phrasing; semicolon-joined cross-reference is idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [16] `packages/redis/README.md` — Sharing an existing client
- **Original:** "Pass an [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options (TLS, retry strategy, etc):"
- **Clarity:** 9/10 — Clear; "Redis or Cluster instance" references ioredis's client classes.
- **Naturalness:** 8/10 — "(TLS, retry strategy, etc):" with "etc" unpunctuated and a trailing colon is slightly informal, but standard for a README intro to a code block.
- **Overall:** 8/10
- **Action:** Kept

---

## `docs/pages/testing/+Page.mdx`

### [17] `docs/pages/testing/+Page.mdx` — top label
- **Original:** "**Environment**: server."
- **Clarity:** 10/10
- **Naturalness:** 9/10 — Recurring label fragment, not a full sentence; conventional across pages.
- **Overall:** 9/10
- **Action:** Kept

### [18] `docs/pages/testing/+Page.mdx` — intro
- **Original:** "Telefunctions are plain functions — unit-test them by importing and calling them directly."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10 — Direct, concise, idiomatic; flawless.
- **Action:** Kept

### [19] `docs/pages/testing/+Page.mdx` — intro
- **Original:** "No server, no HTTP, no mocking."
- **Clarity:** 9/10 — Punchy fragment; meaning ("none of these are needed") is obvious from context.
- **Naturalness:** 10/10 — The rhetorical triple-negative fragment is a natural, common docs flourish.
- **Overall:** 9/10
- **Action:** Kept

### [20] `docs/pages/testing/+Page.mdx` — Providing context
- **Original:** "If a telefunction reads <Link text="getContext()" href="/getContext" /> (for the user, headers, …), provide the context in your test setup with <Link text={<code>provideTelefuncContext()</code>} href="/provideTelefuncContext" /> before calling it:"
- **Clarity:** 8/10 — Clear overall; the parenthetical "(for the user, headers, …)" is a compressed list with an ellipsis, slightly telegraphic but understandable.
- **Naturalness:** 9/10 — Reads naturally; long sentence but well-structured around the two links.
- **Overall:** 8/10
- **Action:** Kept

### [21] `docs/pages/testing/+Page.mdx` — Providing context, after code
- **Original:** "This is also how you test <Link href="/permissions" /> — set up a context that should (or shouldn't) pass, then assert the telefunction returns or `throw Abort()`s accordingly."
- **Clarity:** 7/10 — "`throw Abort()`s" appends a plural "s" to a code span used as a verb, which is confusing and reads like a code typo; "returns or [verb]s" parallelism is broken because `throw Abort()` is not a verb.
- **Naturalness:** 6/10 — Verb-ifying a code statement with a trailing "s" is awkward and unidiomatic.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "This is also how you test <Link href="/permissions" /> — set up a context that should (or shouldn't) pass, then assert the telefunction returns or throws `Abort()` accordingly."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Moves the "s" onto the real English verb "throws" and keeps `Abort()` as inline code; "returns or throws `Abort()`" is grammatical and parallel. Meaning and code preserved. Not 10 because the sentence is still fairly compact/dense.

### [22] `docs/pages/testing/+Page.mdx` — Channels & the wire protocol
- **Original:** "A telefunction that opens a <Link text={<code>Channel</code>} href="/channel" /> or `BroadcastChannel` is still a plain function — call it directly to assert it authorizes correctly and wires up its listeners."
- **Clarity:** 9/10 — Clear; "wires up its listeners" is precise. "it authorizes correctly" assumes the reader knows the telefunction performs authorization.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [23] `docs/pages/testing/+Page.mdx` — Channels & the wire protocol
- **Original:** "The wire protocol itself, though — reconnection, multi-client broadcast, transport upgrades — only exists over a real connection, so cover it with an **end-to-end test** against a running server."
- **Clarity:** 8/10 — Meaning is clear, but "only exists over a real connection" is a loose way to say the protocol only operates/runs over a live connection.
- **Naturalness:** 7/10 — "only exists over a real connection" is slightly off; a protocol "operating" or "coming into play" over a connection reads more naturally than it "existing" over one.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "The wire protocol itself, though — reconnection, multi-client broadcast, transport upgrades — only comes into play over a real connection, so cover it with an **end-to-end test** against a running server."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — "only comes into play over a real connection" is the natural idiom for "only operates when a live connection exists." Meaning, the em-dash interjection, and the bold `**end-to-end test**` are preserved. Not 10 because the dashed mid-sentence list keeps the sentence long.

### [24] `docs/pages/testing/+Page.mdx` — code comment (`hello.telefunc.test.ts`)
- **Original (comment):** "// hello.telefunc.test.ts" / "// Environment: server"
- **Note:** Code comments are filename/environment markers — no typos, grammar, or unclear wording. Not rated.

---

## `docs/components/NeedsLongRunningServer.mdx`

### [25] `docs/components/NeedsLongRunningServer.mdx` — callout
- **Original:** "**Needs a long-running server.**"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10 — Concise callout heading sentence; flawless.
- **Action:** Kept

### [26] `docs/components/NeedsLongRunningServer.mdx` — callout
- **Original:** "A channel holds a connection open for its entire lifetime, so channels and broadcasts don't work on most serverless platforms (e.g. Vercel, AWS Lambda), which terminate a connection after a short time limit."
- **Clarity:** 8/10 — Clear cause→effect; "for its entire lifetime" (the channel's lifetime) is precise. Minor: "after a short time limit" is slightly redundant ("after a limit" vs "after a timeout") but understandable.
- **Naturalness:** 9/10 — Reads naturally; long but well-formed with a parenthetical example.
- **Overall:** 8/10
- **Action:** Kept

### [27] `docs/components/NeedsLongRunningServer.mdx` — callout
- **Original:** "See <Link href="/stream/scale" /> for running multiple instances, or <Link href="/stream/cloudflare" /> for Cloudflare's Durable Objects."
- **Clarity:** 9/10 — Clear pair of pointers; "for running multiple instances" and "for Cloudflare's Durable Objects" each describe their link's purpose well.
- **Naturalness:** 9/10 — Natural cross-reference sentence; the "X, or Y" parallel is slightly terse but fine.
- **Overall:** 9/10
- **Action:** Kept

---

## `docs/components/StreamingBeta.mdx`

### [28] `docs/components/StreamingBeta.mdx` — callout
- **Original:** "**Beta** — Telefunc Stream is in beta: breaking changes may occur in any version update."
- **Clarity:** 9/10 — Clear; "in any version update" precisely conveys that no version is guaranteed stable. Minor: "Beta — ... is in beta" repeats "beta," though the bold label vs. prose use is conventional.
- **Naturalness:** 9/10 — Standard beta-notice phrasing; the "label — sentence: clause" structure reads naturally.
- **Overall:** 9/10
- **Action:** Kept

---

## Summary

- **Sentences reviewed:** 27 prose units (across 5 files) + 1 non-rated code-comment note.
- **Kept:** 24
- **Edited (applied in place):** 3
  - `packages/redis/README.md` intro — "publishes ... reach subscribers" → "a `publish()` ... reaches subscribers".
  - `docs/pages/testing/+Page.mdx` — "returns or `throw Abort()`s accordingly" → "returns or throws `Abort()` accordingly".
  - `docs/pages/testing/+Page.mdx` — "only exists over a real connection" → "only comes into play over a real connection".
- **Second-PR candidates:** none (every edit reached Overall ≥ 8).

All edits change prose wording only; meaning, technical facts, code logic, inline code, `<Link>`/`<code>` JSX, bold emphasis, em-dashes, and URLs/anchors were preserved. Changed regions were re-read to confirm MDX/JSX and README markdown are intact. American English throughout.

### Note (out of scope — not edited)
- `docs/components/NeedsLongRunningServer.mdx` links to `/stream/scale` and `/stream/cloudflare`; the redis page and README also use `/stream/scale` and `/scaling`. A later commit (#350) reportedly reorganized these under `/streaming`. URL/anchor correctness is outside prose-wording scope per the methodology (preserve URLs exactly), so links were left untouched. Flagging in case the link-routing reviewer needs to reconcile `/stream/*` vs `/streaming/*`.
