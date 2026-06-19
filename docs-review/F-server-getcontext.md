# Sentence review — F: server & getContext

Reviewer F. Files reviewed (both MODIFIED — only added/changed prose rated):
- `docs/pages/server/+Page.mdx`
- `docs/pages/getContext/+Page.mdx`

Per METHODOLOGY.md: only sentences on lines this PR added/changed are rated. Code is not
rated, but code comments were scanned for typos/clarity. No introduced sentence scored
Overall ≤ 7, so no edits were applied; every < 10 score is justified below.

---

## `docs/pages/server/+Page.mdx`

### [1] `docs/pages/server/+Page.mdx` — intro, line 3 (sentence 1)
- **Original:** "Telefunc integrates with any server."
- **Clarity:** 9/10 — "any server" is a broad absolute, but the intended meaning (broad compatibility) is unambiguous.
- **Naturalness:** 9/10 — reads as a slightly marketing-style opener; otherwise idiomatic docs prose.
- **Overall:** 9/10
- **Action:** Kept

### [2] `docs/pages/server/+Page.mdx` — intro, line 3 (sentence 2)
- **Original:** "For production deployments with streaming, channels, and WebSocket support, use `new Telefunc()`."
- **Clarity:** 8/10 — the phrase bundles two ideas (production deployments AND the streaming/channels/WebSocket features) so it can momentarily read as "deployments that already have streaming/channels/WS" rather than "use this to get them"; still parseable from context.
- **Naturalness:** 9/10 — idiomatic, concise.
- **Overall:** 8/10
- **Action:** Kept

### [3] `docs/pages/server/+Page.mdx` — upgrade callout, line 5 (lead-in)
- **Original:** "**Upgrading from `telefunc()`?**"
- **Clarity:** 9/10 — a bold question lead-in; clear, though it's a fragment by design.
- **Naturalness:** 9/10 — standard callout pattern.
- **Overall:** 9/10
- **Action:** Kept

### [4] `docs/pages/server/+Page.mdx` — upgrade callout, line 5 (sentence 2)
- **Original:** "`telefunc()` was renamed to <Link text="serve()" href="/serve" /> — it still works but is deprecated."
- **Clarity:** 9/10 — unambiguous; the em-dash clause cleanly states the deprecation status.
- **Naturalness:** 9/10 — natural, matches Telefunc's em-dash voice.
- **Overall:** 9/10
- **Action:** Kept

### [5] `docs/pages/server/+Page.mdx` — upgrade callout, line 5 (sentence 3)
- **Original:** "Use `serve()` for a low-level request handler, or `new Telefunc()` (below) for the full runtime: streaming, channels, WebSocket."
- **Clarity:** 8/10 — clear, but the trailing colon list "streaming, channels, WebSocket" is a fragment appended to the sentence, and "(below)" leans on the reader to map it to the example that follows.
- **Naturalness:** 9/10 — concise and idiomatic.
- **Overall:** 8/10
- **Action:** Kept

### [6] `docs/pages/server/+Page.mdx` — Node callout, line 60 (sentence 1)
- **Original:** "The Node.js adapter auto-detects your HTTP server from the request socket."
- **Clarity:** 9/10 — precise and technical; reader knows exactly what happens.
- **Naturalness:** 9/10 — idiomatic Node docs phrasing.
- **Overall:** 9/10
- **Action:** Kept

### [7] `docs/pages/server/+Page.mdx` — Node callout, line 60 (sentence 2)
- **Original:** "`installWebSocket()` is idempotent — calling it multiple times is safe."
- **Clarity:** 9/10 — the em-dash gloss explains "idempotent" well; no ambiguity.
- **Naturalness:** 9/10 — natural, technical.
- **Overall:** 9/10
- **Action:** Kept

### [8] `docs/pages/server/+Page.mdx` — performance tip, line 62 (sentence 1)
- **Original:** "**Performance tip**: prefer `{ req, res }` over `{ request }` when you have access to the raw Node.js objects (Express, raw `http`, Connect-style middleware)."
- **Clarity:** 9/10 — clear recommendation with concrete examples of "raw Node.js objects".
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [9] `docs/pages/server/+Page.mdx` — performance tip, line 62 (sentence 2)
- **Original:** "The `req`/`res` path reads the request body directly from the Node `Readable` and writes the response straight to the Node `Writable`, skipping the Web Streams conversion layer."
- **Clarity:** 9/10 — technically precise; long but well-structured and unambiguous.
- **Naturalness:** 9/10 — reads like solid Node performance docs.
- **Overall:** 9/10
- **Action:** Kept

### [10] `docs/pages/server/+Page.mdx` — performance tip, line 62 (sentence 3)
- **Original:** "Node's Web Streams implementation is slower than its internal streams; see [nodejs/performance#134](https://github.com/nodejs/performance/issues/134)."
- **Clarity:** 9/10 — clear justification with a citation.
- **Naturalness:** 9/10 — natural; the bare markdown link is an external URL (allowed), not an internal `<Link>`.
- **Overall:** 9/10
- **Action:** Kept

### [11] `docs/pages/server/+Page.mdx` — Node "With context" callout, line 78
- **Original:** "Access `context` inside telefunctions via <Link href="/getContext">`getContext()`</Link>."
- **Clarity:** 9/10 — unambiguous imperative.
- **Naturalness:** 9/10 — concise, fits Telefunc's direct voice (tightened from the prior "You can access ... your telefunctions").
- **Overall:** 9/10
- **Action:** Kept

### [12] `docs/pages/server/+Page.mdx` — Cloudflare, line 143
- **Original:** "Add the Durable Object and KV bindings to your `wrangler.jsonc`:"
- **Clarity:** 9/10 — clear instruction; the code block that follows resolves "bindings".
- **Naturalness:** 9/10 — standard setup-step phrasing.
- **Overall:** 9/10
- **Action:** Kept

### [13] `docs/pages/server/+Page.mdx` — Cloudflare callout, line 161
- **Original:** "See <Link text="Cloudflare Workers" href="/stream/cloudflare" /> for scaling, distributed broadcast, delivery guarantees, and Durable Object configuration."
- **Clarity:** 9/10 — clear pointer with a precise list of topics.
- **Naturalness:** 9/10 — idiomatic "See X for Y" reference.
- **Overall:** 9/10
- **Action:** Kept

### [14] `docs/pages/server/+Page.mdx` — Return value, line 167 (lead-in)
- **Original:** "`telefunc.serve()` returns:"
- **Clarity:** 9/10 — minimal colon lead-in into the table; unambiguous.
- **Naturalness:** 9/10 — standard; too minimal to be "flawless" prose on its own.
- **Overall:** 9/10
- **Action:** Kept

### [15] `docs/pages/server/+Page.mdx` — Return-value table, "Meaning" cells (`Response | undefined` rows)
- **Original:** "`undefined` if not a Telefunc request"
- **Clarity:** 9/10 — clear; reads as a precise condition. (Identical cell repeated for the `{ request }`, Bun, Deno, and Cloudflare rows.)
- **Naturalness:** 9/10 — concise table-cell phrasing.
- **Overall:** 9/10
- **Action:** Kept

### [16] `docs/pages/server/+Page.mdx` — Return-value table, "Meaning" cell (`{ req, res }` row)
- **Original:** "`true` if handled, `false` if not a Telefunc request"
- **Clarity:** 9/10 — clear parallel conditions.
- **Naturalness:** 9/10 — concise; idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [17] `docs/pages/server/+Page.mdx` — Return value callout, line 177
- **Original:** "`telefunc.serve()` returns `undefined` (or `false`) for non-Telefunc requests, allowing you to chain it with other handlers."
- **Clarity:** 9/10 — clear; "(or `false`)" correctly accounts for the `{ req, res }` variant.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10
- **Action:** Kept

### [18] `docs/pages/server/+Page.mdx` — Low-level API, line 182
- **Original:** "If you need direct control over the HTTP response, you can use the core `serve()` function:"
- **Clarity:** 9/10 — clear conditional guidance.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [19] `docs/pages/server/+Page.mdx` — Low-level API, line 196
- **Original:** "See <Link href="/serve" /> for details."
- **Clarity:** 9/10 — standard, unambiguous pointer.
- **Naturalness:** 9/10 — idiomatic; boilerplate by nature so not "flawless"-distinctive.
- **Overall:** 9/10
- **Action:** Kept

### Code comments scanned — `docs/pages/server/+Page.mdx`
- "Enable WebSocket support for channels" (lines 33, 56) — clear, no typo.
- "Pass telefunc.websocket to enable WebSocket channels" (line 92) — clear, no typo.
- No comment typos or unclear comments found in introduced code blocks.

---

## `docs/pages/getContext/+Page.mdx`

### [20] `docs/pages/getContext/+Page.mdx` — Provide, line 31
- **Original:** "Before you can use `getContext()`, you must provide the `context` object when calling `telefunc.serve()`, see <Link href="/server" />."
- **Clarity:** 9/10 — clear prerequisite statement.
- **Naturalness:** 8/10 — the trailing ", see <Link>" tacked on with a comma is mildly comma-splicey, though it matches the established Telefunc "…, see <Link>" convention used elsewhere in these docs.
- **Overall:** 8/10
- **Action:** Kept

### [21] `docs/pages/getContext/+Page.mdx` — Provide callout, line 121
- **Original:** "**Outside `serve()`** (server-side rendering, unit tests): provide the context with <Link text={<code>provideTelefuncContext()</code>} href="/provideTelefuncContext" />."
- **Clarity:** 8/10 — clear, but compact: the bold lead-in plus parenthetical use-cases plus colon plus imperative packs three moves into one line; the reader infers that SSR/unit tests are the "outside `serve()`" cases.
- **Naturalness:** 9/10 — fits the callout style.
- **Overall:** 8/10
- **Action:** Kept

### [22] `docs/pages/getContext/+Page.mdx` — onClose(), line 216 (sentence 1)
- **Original:** "The context object returned by `getContext()` also includes `onClose()`, a callback that fires exactly once when the call ends — whether the client received everything, the client disconnected, or an error occurred."
- **Clarity:** 9/10 — precise; the appositive and the em-dash enumeration leave no ambiguity about when it fires.
- **Naturalness:** 9/10 — well-structured technical sentence; long but flows.
- **Overall:** 9/10
- **Action:** Kept

### [23] `docs/pages/getContext/+Page.mdx` — onClose(), line 216 (sentence 2)
- **Original:** "Use it to release resources."
- **Clarity:** 9/10 — clear purpose statement.
- **Naturalness:** 9/10 — concise; "Use it" is slightly generic, keeping it from flawless.
- **Overall:** 9/10
- **Action:** Kept

### [24] `docs/pages/getContext/+Page.mdx` — signal, line 231 (sentence 1)
- **Original:** "The context object also includes a `signal` (an `AbortSignal`) that aborts when the call ends."
- **Clarity:** 9/10 — precise; the parenthetical types it exactly.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [25] `docs/pages/getContext/+Page.mdx` — signal, line 231 (sentence 2)
- **Original:** "Pass it to APIs that accept one (`fetch`, database clients, etc.) so in-flight work cancels when the client disconnects."
- **Clarity:** 8/10 — "accept one" is a slightly colloquial back-reference to "an `AbortSignal`"; and it names only "when the client disconnects" as the trigger, whereas the abort fires whenever the call ends — a minor narrowing relative to sentence [24]. Still understandable.
- **Naturalness:** 9/10 — reads naturally.
- **Overall:** 8/10
- **Action:** Kept

### Code comments scanned — `docs/pages/getContext/+Page.mdx`
- "Clean up server-side resources" (line 220) — clear, no typo.
- "Provide the context object here:" (relocated above `telefunc.serve()` in both Hono and Express blocks) — text unchanged, clear, no typo.
- Pre-existing comments in unchanged regions not rated.

---

## Summary

- **Sentences reviewed:** 25 prose units (19 in `server/+Page.mdx`, 6 in `getContext/+Page.mdx`), plus code-comment scans in both files.
- **Kept:** 25
- **Edited:** 0
- **Second-PR candidates:** 0

No introduced sentence scored Overall ≤ 7, so no edits met the apply threshold. The strongest
recurring reasons for not awarding 10: idea-bundling/compression in a few callouts (entries
[2], [5], [21]), the comma-spliced "…, see <Link>" convention ([20]), and a minor reference
narrowing in the `signal` description ([25]). All are sound, clear, and on-voice as written.
