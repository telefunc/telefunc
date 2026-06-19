# Sentence review — close / serve / Telefunc / withContext / provideTelefuncContext

Reviewer G. Files (all NEW — whole-file prose reviewed):
- `docs/pages/close/+Page.mdx`
- `docs/pages/serve/+Page.mdx`
- `docs/pages/Telefunc/+Page.mdx`
- `docs/pages/withContext/+Page.mdx`
- `docs/pages/provideTelefuncContext/+Page.mdx`

Confirmed NEW: none of the five appear in `/tmp/diffs/` (that directory holds only modified files), so every prose sentence is in scope.

---

## `docs/pages/close/+Page.mdx`

### [1] `docs/pages/close/+Page.mdx` — intro (line 6)
- **Original:** "`close()` and `onClose()` manage the lifecycle of the streams and connections a telefunction returns or accepts — so the server can stop producing and release resources."
- **Clarity:** 8/10 — "returns or accepts" assumes the reader knows a telefunction can also *accept* incoming streams (e.g. file uploads); without that context "accepts" is slightly underspecified.
- **Naturalness:** 9/10 — reads well; em-dash + "so" is on-voice.
- **Overall:** 8/10
- **Action:** Kept

### [2] `docs/pages/close/+Page.mdx` — intro blockquote (line 8)
- **Original:** "Applies to all <Link href="/stream">streaming and real-time</Link> values — `AsyncGenerator`, `ReadableStream`, files, `Channel`, `BroadcastChannel` — they all close the same way."
- **Clarity:** 8/10 — the trailing clause "they all close the same way" is appended after the closing em-dash, so the list's grammatical role is muddied.
- **Naturalness:** 7/10 — the em-dashes look like a parenthetical insert, but the second dash actually splices in a new independent clause; reads as a near comma-splice.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Applies to all <Link href="/stream">streaming and real-time</Link> values: `AsyncGenerator`, `ReadableStream`, files, `Channel`, and `BroadcastChannel` all close the same way."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — colon now introduces the list, which becomes the subject of "all close the same way"; clean and unambiguous. Not a 10 only because the noun-list-as-subject is still a touch dense.

### [3] `docs/pages/close/+Page.mdx` — "At a glance" (line 13, sentence 1)
- **Original:** "There's one catch-all API, `close()`, plus per-type APIs."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — "plus per-type APIs" is very terse; fits the voice but borders on telegraphic.
- **Overall:** 8/10
- **Action:** Kept

### [4] `docs/pages/close/+Page.mdx` — "At a glance" (line 13, sentence 2)
- **Original:** "They all signal the server to stop producing and release resources — the difference is **graceful** (flush buffered data, then close) vs **immediate** (cancel in-flight work):"
- **Clarity:** 9/10 — the graceful/immediate contrast is well drawn.
- **Naturalness:** 8/10 — "vs" is informal but consistent with the table heading; parentheticals are tidy.
- **Overall:** 8/10
- **Action:** Kept

### [5] `docs/pages/close/+Page.mdx` — table cell (line 17, "Closes")
- **Original:** "Everything in the returned value (walked recursively)"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — "walked recursively" is jargon-y but apt for the audience.
- **Overall:** 8/10
- **Action:** Kept

### [6] `docs/pages/close/+Page.mdx` — table cell (line 24, "Closes")
- **Original:** "Fires when the value ends, however it ends"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — the deliberate "ends … ends" echo is stylish but slightly repetitive.
- **Overall:** 8/10
- **Action:** Kept
- _Note:_ remaining "Closes"/"Side" table cells (lines 18–23) are bare noun phrases (e.g. "One `AsyncGenerator`", "The in-flight call and any channels it opened") — clear and idiomatic; not individually rated below 9.

### [7] `docs/pages/close/+Page.mdx` — "At a glance" closer (line 26)
- **Original:** "The rest of this page covers `close()`, `abort()`, and `onClose()` in detail; channel-specific closing is covered on <Link href="/channel#lifecycle" />."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 7/10 — "covers … in detail; … is covered on" repeats "cover", and "covered on" is the wrong preposition (one is documented *in* a page).
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "The rest of this page covers `close()`, `abort()`, and `onClose()` in detail; channel-specific closing is documented on <Link href="/channel#lifecycle" />."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — drops the "covers/covered" repetition; "documented on" reads naturally with the page link. Not a 10 because the sentence is still fairly long.

### [8] `docs/pages/close/+Page.mdx` — "`close()` (client)" (line 31, sentence 1)
- **Original:** "`close()` from `telefunc/client` gracefully closes any value returned by a telefunction."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "any value" is broad, but the next sentence pins it down.
- **Action:** Kept

### [9] `docs/pages/close/+Page.mdx` — "`close()` (client)" (line 31, sentence 2)
- **Original:** "In one call, it closes every stream, channel, and file nested anywhere in the return value."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — essentially flawless; held just below 10 as it slightly restates the "one call" idea from sentence 1.
- **Action:** Kept

### [10] `docs/pages/close/+Page.mdx` — "Other ways to close" (line 50)
- **Original:** "For specific value types, you can also use the standard JS APIs directly:"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "directly" is mildly redundant after "use … APIs", but harmless.
- **Action:** Kept

### [11] `docs/pages/close/+Page.mdx` — after code block (line 68)
- **Original:** "All of these signal the server to stop producing and release resources."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — third occurrence of "stop producing and release resources" on the page; each is in a distinct context, so kept, but the repetition is noticeable.
- **Overall:** 8/10
- **Action:** Kept

### [12] `docs/pages/close/+Page.mdx` — blockquote (line 70)
- **Original:** "`close()` ends a value **gracefully** (buffered data is flushed). To stop a call **immediately** instead, pass the pending call to `abort()` — the request is cancelled, and the pending call rejects with an `Abort` error (or, if you're mid-stream, the next read does):"
- **Clarity:** 8/10 — clear overall; the closing "(or … the next read does)" relies on the reader carrying "rejects" forward, which is slightly elliptical.
- **Naturalness:** 8/10 — a long compound sentence, but well punctuated and on-voice.
- **Overall:** 8/10
- **Action:** Kept

### [13] `docs/pages/close/+Page.mdx` — "`onClose()` (server)" (line 83)
- **Original:** "Use `onClose()` from <Link href="/getContext" text="getContext()" /> on the server to detect when the response ends — whether the value was consumed, the client disconnected, or an error occurred:"
- **Clarity:** 8/10 — clear; "on the server" floats between "getContext()" and "to detect", so for a beat it's unclear whether it qualifies the import or the usage.
- **Naturalness:** 8/10 — the three-way "whether … , … , or …" list is idiomatic.
- **Overall:** 8/10
- **Action:** Kept

### [14] `docs/pages/close/+Page.mdx` — Warning (line 104, sentence 1)
- **Original:** "**Pitfall** — anything a long-lived telefunction opens (`setInterval`, an event subscription, a DB cursor, an upstream stream) outlives the call and **leaks** unless you release it in `onClose()`."
- **Clarity:** 9/10 — clear and concrete.
- **Naturalness:** 9/10 — vivid, on-voice.
- **Overall:** 9/10 — minor: the subject "anything … opens" and verb "outlives" are far apart across a long parenthetical, very slightly taxing.
- **Action:** Kept

### [15] `docs/pages/close/+Page.mdx` — Warning (line 104, sentence 2)
- **Original:** "If you set it up, tear it down here."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — punchy, idiomatic.
- **Overall:** 9/10 — "here" leans on the preceding `onClose()` reference; fine in context, just not fully self-contained.
- **Action:** Kept

### [16] `docs/pages/close/+Page.mdx` — "Automatic cleanup (GC)" (line 111, sentence 1)
- **Original:** "If the client drops all references to a returned value without explicitly closing it, the underlying resources are cleaned up automatically via garbage collection."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: a long conditional; "automatically via garbage collection" is slightly belt-and-suspenders.
- **Action:** Kept

### [17] `docs/pages/close/+Page.mdx` — "Automatic cleanup (GC)" (line 111, sentence 2)
- **Original:** "There is a short delay (typically a few seconds) between the value becoming unreachable and the cleanup firing."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "the cleanup firing" is light jargon, but clear.
- **Action:** Kept

### [18] `docs/pages/close/+Page.mdx` — "Automatic cleanup (GC)" (line 113, sentence 1)
- **Original:** "Explicit `close()` triggers cleanup immediately."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — concise; held below 10 only by the page's heavy reuse of "cleanup".
- **Action:** Kept

### [19] `docs/pages/close/+Page.mdx` — "Automatic cleanup (GC)" (line 113, sentence 2)
- **Original:** "GC cleanup is the fallback for when you forget to close explicitly."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — "the fallback for when" is a touch conversational for reference docs.
- **Overall:** 8/10
- **Action:** Kept

_Code comments (close): "Read what you need…", "Then close everything at once", "AsyncGenerator — break out of the loop", "Or call return() directly", "ReadableStream — cancel the reader", "Channel — close explicitly", "cancels the in-flight call", "Release resources, cancel upstream work" — all correct, no typos._

---

## `docs/pages/serve/+Page.mdx`

### [20] `docs/pages/serve/+Page.mdx` — intro (line 5)
- **Original:** "Low-level function that processes telefunction requests and returns an HTTP response. It's a pure function: stateless and has no side effects."
- **Clarity:** 8/10 — clear.
- **Naturalness:** 6/10 — the colon list is non-parallel: "stateless" (adjective) and "has no side effects" (verb phrase) don't match.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Low-level function that processes telefunction requests and returns an HTTP response. It's a pure function: stateless and side-effect-free."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — two parallel adjectives after the colon; tighter. (Only the first sentence's wording was preserved; the edit targets the second sentence.) Not a 10 because "stateless and side-effect-free" mildly restates "pure function".

### [21] `docs/pages/serve/+Page.mdx` — intro blockquote (line 7)
- **Original:** "For <Link href="/stream">streaming and real-time</Link> setups, use the runtime-specific `new Telefunc()` from `telefunc/node`, `telefunc/bun`, `telefunc/deno`, or `telefunc/cloudflare` instead."
- **Clarity:** 9/10 — clear; "instead" correctly contrasts with `serve()`.
- **Naturalness:** 8/10 — a long four-item import list mid-sentence, but readable.
- **Overall:** 8/10
- **Action:** Kept
- _Trailing "See <Link href="/server" />." is a standard pointer — fine._

### [22] `docs/pages/serve/+Page.mdx` — "Basic usage" (line 11)
- **Original:** "Pass the web-standard [`Request`](https://developer.mozilla.org/en-US/docs/Web/API/Request) object:"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic; the bare markdown link is allowed (external MDN URL, not an internal link).
- **Overall:** 9/10 — minor: "object" is slightly redundant after the `Request` type, but conventional.
- **Action:** Kept

### [23] `docs/pages/serve/+Page.mdx` — "Basic usage" (line 21)
- **Original:** "Convert the result to a [`Response`](https://developer.mozilla.org/en-US/docs/Web/API/Response):"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "the result" leans on the prior code block for its referent, fine in flow.
- **Action:** Kept

### [24] `docs/pages/serve/+Page.mdx` — "With context" (line 33)
- **Original:** "Pass a `context` object to make request-scoped data available inside telefunctions via <Link href="/getContext">`getContext()`</Link>:"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: dense (three prepositional phrases), but each is necessary.
- **Action:** Kept

### [25] `docs/pages/serve/+Page.mdx` — "With context" blockquote (line 44)
- **Original:** "The `context` parameter is optional — only needed if you use `getContext()`."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic; the em-dash elision ("only needed") is on-voice.
- **Overall:** 9/10 — essentially flawless; held below 10 only as it restates the "optional" idea twice (optional / only needed).
- **Action:** Kept
- _Trailing "See <Link href="/getContext#provide" />." — standard pointer, fine._

### [26] `docs/pages/serve/+Page.mdx` — "Node.js `req`" (line 49)
- **Original:** "For [Express](https://expressjs.com), [Fastify](https://fastify.dev), or any Node.js framework, pass the `req` readable stream:"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic; external links are allowed.
- **Overall:** 9/10 — minor: "readable stream" as a bare descriptor of `req` is slightly informal, but accurate.
- **Action:** Kept

### [27] `docs/pages/serve/+Page.mdx` — "Response object" (line 64)
- **Original:** "`httpResponse` contains everything needed to send the response:"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "everything needed" is a touch loose for a reference intro, but fine.
- **Action:** Kept

### [28] `docs/pages/serve/+Page.mdx` — table cell (line 73, "Description" for `err`)
- **Original:** "The error thrown by your telefunction, if any (otherwise `undefined`) — see <Link href="/error-handling" />"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "if any (otherwise `undefined`)" is slightly belt-and-suspenders, but unambiguous.
- **Action:** Kept
- _Other Description cells (lines 70–72: "Web-standard stream (Hono, Cloudflare, Deno, etc.)", "Pipe to Node.js writable (Express, Fastify)", "Full body as string (non-streaming only)") are clear, idiomatic noun phrases; "HTTP status code" / "Response headers" are bare labels. Not individually rated below 9._

_Code comments (serve): only "// Environment: server" and placeholder `/* ... */` — fine._

---

## `docs/pages/Telefunc/+Page.mdx`

### [29] `docs/pages/Telefunc/+Page.mdx` — intro (line 6)
- **Original:** "Use `new Telefunc()` to embed Telefunc into your server, with full-fledged support for <Link href="/stream">Telefunc Stream</Link>."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — the trailing ", with full-fledged support for …" is a loosely attached modifier; reads slightly tacked-on.
- **Overall:** 8/10
- **Action:** Kept

### [30] `docs/pages/Telefunc/+Page.mdx` — intro blockquote (line 37, sentence 1)
- **Original:** "For per-runtime setup see <Link href="/server" />, and <Link href="/stream/cloudflare" /> for Cloudflare specifics."
- **Clarity:** 7/10 — the two halves have mismatched shapes ("For X see Y" then "and Z for X"), so the second pointer's purpose registers late.
- **Naturalness:** 6/10 — the comma-joined, asymmetric clauses read awkwardly.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "For per-runtime setup see <Link href="/server" />; for Cloudflare specifics see <Link href="/stream/cloudflare" />."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — parallel "For X see Y; for Z see W" structure; semicolon separates the two pointers cleanly. Not a 10 only because two consecutive "for … see" clauses are slightly repetitive (acceptable for parallelism).

### [31] `docs/pages/Telefunc/+Page.mdx` — intro blockquote (line 37, sentence 2)
- **Original:** "For a low-level request handler without channels, use <Link href="/serve" />."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic and parallel with the surrounding pointers.
- **Overall:** 9/10 — minor: "without channels" is terse but clear in context.
- **Action:** Kept

### [32] `docs/pages/Telefunc/+Page.mdx` — Methods table (line 44, `serve(input)`)
- **Original:** "Process a request and return the response. The input shape (`{ request }`, `{ req, res }`, `{ request, env, ctx }`, …) and return type vary by runtime — see <Link href="/server#return-value" />."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 8/10 — a long sentence with a big inline parenthetical for a table cell, but well formed.
- **Overall:** 8/10
- **Action:** Kept

### [33] `docs/pages/Telefunc/+Page.mdx` — Methods table (line 45, `installWebSocket`)
- **Original:** "Enable WebSocket channels on your HTTP server. Idempotent."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — "Idempotent." as a one-word sentence is conventional in API reference.
- **Overall:** 9/10 — minor: the terse one-word follow-up assumes the reader knows the term.
- **Action:** Kept

### [34] `docs/pages/Telefunc/+Page.mdx` — Methods table (line 46, `websocket`)
- **Original:** "The WebSocket handler — pass it to `Bun.serve({ websocket })`."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: noun-phrase-then-instruction join is slightly compressed, but clear.
- **Action:** Kept

### [35] `docs/pages/Telefunc/+Page.mdx` — Methods table (line 47, `TelefuncDurableObject`)
- **Original:** "The Durable Object class to export — see <Link href="/stream/cloudflare" />."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "to export" is terse but unambiguous for the audience.
- **Action:** Kept
- _Runtime cells ("all", "Node", "Bun", "Cloudflare") are bare labels — skipped._

### [36] `docs/pages/Telefunc/+Page.mdx` — "Context" (line 52)
- **Original:** "Provide the <Link text="getContext()" href="/getContext" /> data per request — on Node/Bun/Deno via `serve({ context })`, on Cloudflare via the `context` constructor option:"
- **Clarity:** 8/10 — clear, but "the getContext() data" is shorthand for "the data getContext() returns".
- **Naturalness:** 7/10 — using a function name as an adjective for its return value ("the getContext() data") is informal for reference docs.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Provide the data for <Link text="getContext()" href="/getContext" /> per request — on Node/Bun/Deno via `serve({ context })`, on Cloudflare via the `context` constructor option:"
- **Edit rating:** Clarity 9/10, Naturalness 8/10, Overall 8/10 — "the data for getContext()" reads naturally and states the relationship (you supply the data that getContext() exposes). Held at 8 because the sentence is still long with two parallel runtime branches.

### [37] `docs/pages/Telefunc/+Page.mdx` — "Context" closer (line 62)
- **Original:** "See <Link href="/getContext#provide" />."
- **Clarity:** 9/10 — standard pointer.
- **Naturalness:** 9/10 — standard pointer.
- **Overall:** 9/10 — fine; not a full sentence, so reserved from 10.
- **Action:** Kept

### [38] `docs/pages/Telefunc/+Page.mdx` — "Configuration (Cloudflare)" (line 67)
- **Original:** "On Cloudflare the constructor also takes `bindingName`, `kvBindingName`, `scale`, `locationFallback`, `jurisdiction`, … — see <Link href="/stream/cloudflare#configuration" />."
- **Clarity:** 9/10 — clear; the "…" signals a non-exhaustive list.
- **Naturalness:** 8/10 — a long inline option list ending in "… —" is slightly clunky but acceptable.
- **Overall:** 8/10
- **Action:** Kept

_Code comments (Telefunc): "// Environment: server", "// Node / Bun / Deno", "// Cloudflare" — fine._

---

## `docs/pages/withContext/+Page.mdx`

### [39] `docs/pages/withContext/+Page.mdx` — intro (line 8, sentence 1)
- **Original:** "`withContext(telefunction, context)` from `telefunc/client` wraps a telefunction with **per-call client context** — applied to that one call instead of the global `config`."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic; em-dash clause is on-voice.
- **Overall:** 9/10 — minor: "applied to that one call" is a dangling participial referring to "context"; reads fine but isn't strictly attached.
- **Action:** Kept

### [40] `docs/pages/withContext/+Page.mdx` — intro (line 8, sentence 2)
- **Original:** "Use it for an `AbortSignal`, extra headers, a URL override, or per-call <Link href="/transport">transport</Link> overrides."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic list.
- **Overall:** 9/10 — minor: "a URL override … or per-call transport overrides" repeats "override(s)" within one list.
- **Action:** Kept

### [41] `docs/pages/withContext/+Page.mdx` — blockquote (line 30)
- **Original:** "`withContext()` returns a wrapped function with the same signature — call it exactly like the original telefunction."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "the same signature" and "exactly like the original" slightly overlap in meaning.
- **Action:** Kept

### [42] `docs/pages/withContext/+Page.mdx` — Options table (line 37, `signal`)
- **Original:** "Cancel this call and any channels it opens — see <Link href="/close" />."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: imperative cell vs. noun-phrase cells elsewhere; harmless.
- **Action:** Kept

### [43] `docs/pages/withContext/+Page.mdx` — Options table (line 38, `headers`)
- **Original:** "Extra HTTP headers for this call. Global default: <Link text={<code>config.headers</code>} href="/headers" />."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "Global default:" fragment is terse, but standard table style.
- **Action:** Kept

### [44] `docs/pages/withContext/+Page.mdx` — Options table (line 39, `telefuncUrl`)
- **Original:** "Override <Link text={<code>config.telefuncUrl</code>} href="/telefuncUrl" /> for this call and its channels."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "and its channels" relies on the reader knowing a call can spawn channels; fine in context.
- **Action:** Kept

### [45] `docs/pages/withContext/+Page.mdx` — Options table (line 40, `stream.transport`)
- **Original:** "Override `config.stream.transport` for streamed values."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: very terse, but appropriate for a table.
- **Action:** Kept

### [46] `docs/pages/withContext/+Page.mdx` — Options table (line 41, `channel.transports`)
- **Original:** "Override `config.channel.transports` for channels."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "for channels" is nearly self-evident given the option name, but harmless.
- **Action:** Kept

### [47] `docs/pages/withContext/+Page.mdx` — Options table (line 42, `channel.connectionKey`)
- **Original:** "Calls with the same key share one connection; different keys use separate connections."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — nicely parallel.
- **Overall:** 9/10 — essentially flawless; held below 10 only because "use separate connections" slightly anthropomorphizes "keys".
- **Action:** Kept

### [48] `docs/pages/withContext/+Page.mdx` — Options table (line 43, `channel.idleTimeout`)
- **Original:** "How long (ms) to keep the underlying connection open after all channels close. Default `60_000`; `0` closes it immediately."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10 — minor: "How long (ms) to keep …" as a sentence opener is a touch clipped, but standard.
- **Action:** Kept

_Code comments (withContext): "abort this call (and any channels it opens)", "extra HTTP headers, just for this call", "override config.telefuncUrl for this call", "override config.stream.transport", "override config.channel.transports" — all correct, no typos._

---

## `docs/pages/provideTelefuncContext/+Page.mdx`

### [49] `docs/pages/provideTelefuncContext/+Page.mdx` — intro (line 5)
- **Original:** "`provideTelefuncContext()` makes the `context` object available to telefunctions via <Link href="/getContext" text="getContext()" /> — useful when a telefunction runs **outside `telefunc.serve()`**, such as server-side rendering or unit tests."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic; em-dash clause on-voice.
- **Overall:** 9/10 — minor: "such as server-side rendering or unit tests" hangs off "runs outside serve()" rather than off a noun, so the examples attach slightly loosely.
- **Action:** Kept

### [50] `docs/pages/provideTelefuncContext/+Page.mdx` — usage note (line 16)
- **Original:** "Call it before the telefunction runs (e.g. in a test's setup, or your SSR request handler)."
- **Clarity:** 8/10 — clear.
- **Naturalness:** 7/10 — the comma before "or" in a two-item list is non-standard, and "a test's setup" (possessive) vs "your SSR request handler" mixes registers.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Call it before the telefunction runs (e.g. in your test setup or your SSR request handler)."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — drops the spurious comma and makes the pair parallel ("your test setup or your SSR request handler"). Not a 10 because the parenthetical example is necessarily brief.

### [51] `docs/pages/provideTelefuncContext/+Page.mdx` — blockquote (line 18, sentence 1)
- **Original:** "Through <Link text="serve()" href="/serve" /> / <Link text="new Telefunc()" href="/Telefunc" />, the context is provided for you — see <Link href="/getContext#provide" />."
- **Clarity:** 8/10 — clear.
- **Naturalness:** 7/10 — "Through X / Y, the context is provided for you" pairs an awkward "Through" lead-in with a passive clause; "With" fits better.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "With <Link text="serve()" href="/serve" /> / <Link text="new Telefunc()" href="/Telefunc" />, the context is provided for you — see <Link href="/getContext#provide" />."
- **Edit rating:** Clarity 9/10, Naturalness 8/10, Overall 8/10 — "With X / Y, the context is provided for you" reads more naturally. Held at 8 because the passive "is provided for you" and the slash-pair phrasing remain (kept to match house style and preserve both Links).

### [52] `docs/pages/provideTelefuncContext/+Page.mdx` — blockquote (line 18, sentence 2)
- **Original:** "Reach for `provideTelefuncContext()` only when a telefunction runs outside both."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — idiomatic ("Reach for X only when …") and on-voice.
- **Overall:** 9/10 — minor: "outside both" depends on the prior sentence for the referent ("serve() / new Telefunc()"); fine in flow.
- **Action:** Kept

_Code comment (provideTelefuncContext): "getContext() inside telefunctions now returns { user }" — correct, no typos._

---

## Summary

- **Sentences reviewed:** 52 (prose units across the five files; bare labels, anchor-only "See" links, pure-syntax/label table cells, imports, and frontmatter excluded).
- **Kept:** 45
- **Edited (applied in place):** 7
  1. `close/+Page.mdx` L8 — list em-dashes → colon ("…values: A, B, files, C, and D all close the same way.")
  2. `close/+Page.mdx` L26 — "covered on" → "documented on" (removed "covers/covered" repetition)
  3. `serve/+Page.mdx` L5 — "stateless and has no side effects" → "stateless and side-effect-free" (parallel adjectives)
  4. `Telefunc/+Page.mdx` L37 — asymmetric pointer → parallel "For X see Y; for Cloudflare specifics see Z."
  5. `Telefunc/+Page.mdx` L52 — "the getContext() data" → "the data for getContext()"
  6. `provideTelefuncContext/+Page.mdx` L16 — fixed comma + parallelism ("in your test setup or your SSR request handler")
  7. `provideTelefuncContext/+Page.mdx` L18 — "Through" → "With"
- **Second-PR candidates:** 0
- **Code-comment fixes:** none required (all comments correct).

All edits changed prose wording only; meaning, technical facts, code, and all MDX/JSX (`<Link>` components, inline code, em-dashes) were preserved, and changed regions were re-read to confirm integrity. American English throughout.
