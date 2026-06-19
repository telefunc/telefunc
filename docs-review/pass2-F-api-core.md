# Pass-2 report — F: API core docs

Second pass over PR #264 docs. Each target was previously rated Overall 7–8 with a documented
minor weakness; goal is to push to 10/10 via prose wording only, preserving meaning, technical
facts, and all MDX/JSX.

---

## `docs/pages/server/+Page.mdx`

### [2] `docs/pages/server/+Page.mdx` — intro, line 3 (sentence 2)
- **Original:** "For production deployments with streaming, channels, and WebSocket support, use `new Telefunc()`."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "with streaming, channels, and WebSocket support" bundles two ideas, so it can momentarily read as "deployments that already have these features" rather than "use this to get them."
- **Candidates:**
  1. "For production deployments that need streaming, channels, and WebSocket support, use `new Telefunc()`." — C 10 / N 9 / Overall 10 — "that need" frames the features as desired capabilities, not pre-existing ones; minimal change, keeps voice.
  2. "To add streaming, channels, and WebSocket support to a production deployment, use `new Telefunc()`." — C 10 / N 9 / Overall 9 — clear, but recasts the sentence and shifts emphasis away from "production deployments."
  3. "For production deployments with support for streaming, channels, and WebSockets, use `new Telefunc()`." — C 8 / N 9 / Overall 8 — mere reorder; retains the same ambiguity.
- **Decision:** Applied → "For production deployments that need streaming, channels, and WebSocket support, use `new Telefunc()`." (new Overall 10)
- **Why:** "that need" removes the "already-has-these-features" misread with a one-word change, preserving structure and voice.

### [5] `docs/pages/server/+Page.mdx` — upgrade callout, line 5 (sentence 3)
- **Original:** "Use `serve()` for a low-level request handler, or `new Telefunc()` (below) for the full runtime: streaming, channels, WebSocket."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — the trailing colon list is a fragment appended to the sentence, and "(below)" leans on the reader to map it to the example that follows.
- **Candidates:**
  1. "Use `serve()` for a low-level request handler, or `new Telefunc()` (shown below) for the full runtime, which adds streaming, channels, and WebSocket." — C 10 / N 9 / Overall 10 — "shown below" makes the reference explicit; "which adds …" turns the appended fragment into a proper relative clause.
  2. "Use `serve()` for a low-level request handler, or `new Telefunc()` (shown below) for the full runtime: streaming, channels, and WebSocket." — C 9 / N 9 / Overall 9 — fixes "(below)" and adds the Oxford comma, but keeps the colon fragment.
  3. "Use `serve()` for a low-level request handler, or the `new Telefunc()` runtime (shown below) for streaming, channels, and WebSocket." — C 9 / N 9 / Overall 9 — restructures; loses the explicit "full runtime" label.
- **Decision:** Applied → "Use `serve()` for a low-level request handler, or `new Telefunc()` (shown below) for the full runtime, which adds streaming, channels, and WebSocket." (new Overall 10)
- **Why:** Resolves both flagged issues — the dangling fragment becomes a clause and "(shown below)" makes the cross-reference explicit.

---

## `docs/pages/getContext/+Page.mdx`

### [20] `docs/pages/getContext/+Page.mdx` — Provide, line 31
- **Original:** "Before you can use `getContext()`, you must provide the `context` object when calling `telefunc.serve()`, see <Link href="/server" />."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the trailing ", see <Link>" tacked on with a comma is mildly comma-splicey (though it matches an established Telefunc convention).
- **Candidates:**
  1. "Before you can use `getContext()`, you must provide the `context` object when calling `telefunc.serve()` — see <Link href="/server" />." — C 9 / N 10 / Overall 10 — em-dash removes the comma splice and is squarely in Telefunc voice (em-dashes common).
  2. "Before you can use `getContext()`, you must provide the `context` object when calling `telefunc.serve()` (see <Link href="/server" />)." — C 9 / N 9 / Overall 9 — parenthetical is clean but slightly heavier with the period after `)`.
  3. "Before you can use `getContext()`, you must provide the `context` object when calling `telefunc.serve()`; see <Link href="/server" />." — C 9 / N 9 / Overall 9 — semicolon is correct but rarer in this voice.
- **Decision:** Applied → "Before you can use `getContext()`, you must provide the `context` object when calling `telefunc.serve()` — see <Link href="/server" />." (new Overall 10)
- **Why:** The em-dash fixes the comma splice while staying on-voice; `<Link>` preserved.

### [21] `docs/pages/getContext/+Page.mdx` — Provide callout, line 121
- **Original:** "**Outside `serve()`** (server-side rendering, unit tests): provide the context with <Link text={<code>provideTelefuncContext()</code>} href="/provideTelefuncContext" />."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — compact; the reader must infer that SSR/unit tests are examples of the "outside `serve()`" cases.
- **Candidates:**
  1. "**Outside `serve()`** (e.g. server-side rendering or unit tests): provide the context with <Link …/>." — C 9 / N 9 / Overall 10 — "e.g." signals the parenthetical lists examples of the outside-`serve()` case; "or" reads more naturally than the comma.
  2. "**Outside `serve()`** — for example during server-side rendering or in unit tests — provide the context with <Link …/>." — C 9 / N 9 / Overall 9 — spells out the relationship but expands the callout and changes its punctuation structure.
  3. "**Outside `serve()`** (such as server-side rendering or unit tests): provide the context with <Link …/>." — C 9 / N 9 / Overall 9 — "such as" achieves the same effect as "e.g.".
- **Decision:** Applied → "**Outside `serve()`** (e.g. server-side rendering or unit tests): provide the context with <Link text={<code>provideTelefuncContext()</code>} href="/provideTelefuncContext" />." (new Overall 10)
- **Why:** "e.g. … or …" makes the examples-of relationship explicit with the smallest possible change; `<Link>`/`<code>` preserved.

### [25] `docs/pages/getContext/+Page.mdx` — signal, line 231 (sentence 2)
- **Original:** "Pass it to APIs that accept one (`fetch`, database clients, etc.) so in-flight work cancels when the client disconnects."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "accept one" is a colloquial back-reference to "an `AbortSignal`"; and naming only "when the client disconnects" narrows the trigger relative to the prior sentence ("aborts when the call ends").
- **Candidates:**
  1. "Pass it to APIs that accept an `AbortSignal` (`fetch`, database clients, etc.) so in-flight work cancels when the call ends." — C 10 / N 9 / Overall 10 — names the type explicitly and broadens the trigger to match the prior sentence.
  2. "Pass it to APIs that accept a signal (`fetch`, database clients, etc.) so in-flight work cancels when the call ends." — C 9 / N 9 / Overall 9 — "a signal" is less explicit than the concrete `AbortSignal`.
  3. "Pass it to any API that accepts an `AbortSignal` (`fetch`, database clients, etc.) so in-flight work cancels when the call ends." — C 10 / N 9 / Overall 10 — equally clear; "any API … accepts" is smooth but changes "APIs" → "any API".
- **Decision:** Applied → "Pass it to APIs that accept an `AbortSignal` (`fetch`, database clients, etc.) so in-flight work cancels when the call ends." (new Overall 10)
- **Why:** Fixes both flagged issues at once — "accept one" becomes the explicit `AbortSignal`, and "when the call ends" matches sentence [24], removing the narrowing.

---

## `docs/pages/close/+Page.mdx`

### [1] `docs/pages/close/+Page.mdx` — intro (line 6)
- **Original:** "`close()` and `onClose()` manage the lifecycle of the streams and connections a telefunction returns or accepts — so the server can stop producing and release resources."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "returns or accepts" assumes the reader knows a telefunction can also *accept* incoming streams (e.g. file uploads); "accepts" is underspecified.
- **Candidates:**
  1. "… the streams and connections a telefunction returns or accepts as arguments — so the server can stop producing and release resources." — C 10 / N 9 / Overall 10 — "as arguments" pins down how a telefunction "accepts" streams (they come in as inputs).
  2. "… the streams and connections a telefunction returns (or receives) — …" — C 8 / N 9 / Overall 8 — "receives" is as vague as "accepts".
  3. "… the streams and connections a telefunction returns or takes as arguments — …" — C 9 / N 9 / Overall 9 — same idea; "takes" is slightly more colloquial.
- **Decision:** Applied → "`close()` and `onClose()` manage the lifecycle of the streams and connections a telefunction returns or accepts as arguments — so the server can stop producing and release resources." (new Overall 10)
- **Why:** "as arguments" removes the underspecification with two words, keeping the em-dash + "so" voice.

### [3] `docs/pages/close/+Page.mdx` — "At a glance" (line 13, sentence 1)
- **Original:** "There's one catch-all API, `close()`, plus per-type APIs."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "plus per-type APIs" is very terse, bordering on telegraphic.
- **Candidates:**
  1. "There's one catch-all API, `close()`, plus several per-type APIs." — C 9 / N 9 / Overall 9 — "several" supplies a natural quantifier without adding new claims; relieves the clipped feel.
  2. "There's one catch-all API, `close()`, plus a set of per-type APIs." — C 9 / N 9 / Overall 9 — "a set of" similarly softens; marginally wordier.
  3. "There's one catch-all API, `close()`, plus per-type APIs for finer control." — C 9 / N 9 / Overall 9 — reads well but "for finer control" adds a claim beyond the original.
- **Decision:** Applied → "There's one catch-all API, `close()`, plus several per-type APIs." (new Overall 9)
- **Why:** "several" eases the telegraphic ending without introducing meaning the original didn't carry.

### [4] `docs/pages/close/+Page.mdx` — "At a glance" (line 13, sentence 2)
- **Original:** "They all signal the server to stop producing and release resources — the difference is **graceful** (flush buffered data, then close) vs **immediate** (cancel in-flight work):"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "vs" is informal (though consistent with the table heading "Graceful / immediate").
- **Candidates:**
  1. "… the difference is **graceful** (flush buffered data, then close) versus **immediate** (cancel in-flight work):" — C 9 / N 9 / Overall 9 — "versus" removes the informality while keeping the bold adjectives that tie to the table column.
  2. "… the difference is whether they do so **gracefully** (flush buffered data, then close) or **immediately** (cancel in-flight work):" — C 10 / N 10 / Overall 9 — reads very naturally, but switches the bold terms from adjectives to adverbs, weakening the visual tie to the "Graceful / immediate" column.
  3. "… the difference is **graceful** (flush buffered data, then close) vs. **immediate** (cancel in-flight work):" — C 9 / N 9 / Overall 8 — adds the American period to "vs." but doesn't address the flagged informality.
  4. "… the difference is **graceful** (flush buffered data, then close) or **immediate** (cancel in-flight work):" — C 8 / N 8 / Overall 8 — "the difference is X or Y" reads slightly oddly versus "the difference *between* X and Y".
- **Decision:** Applied → "They all signal the server to stop producing and release resources — the difference is **graceful** (flush buffered data, then close) versus **immediate** (cancel in-flight work):" (new Overall 9)
- **Why:** "versus" resolves the documented informality while preserving the bold adjectives and the sentence shape.

### [5] `docs/pages/close/+Page.mdx` — table cell (line 17, "Closes")
- **Original:** "Everything in the returned value (walked recursively)"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "walked recursively" is jargon-y (but apt for the audience).
- **Candidates:**
  1. "Everything in the returned value (traversed recursively)" — C 9 / N 8 / Overall 8 — "traversed" is a lateral jargon swap for "walked"; doesn't remove the flagged jargon-y quality.
  2. "Everything in the returned value (recursively)" — C 8 / N 8 / Overall 8 — terser, but loses the tree-walking image.
  3. "Every stream nested anywhere in the returned value" — C 8 / N 8 / Overall 8 — narrows meaning (not just streams) and drops the recursion emphasis.
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** "walked recursively" is idiomatic for the developer audience (as the reviewer noted, "apt"); every alternative is either another jargon term or loses information. No genuine gain.

### [6] `docs/pages/close/+Page.mdx` — table cell (line 24, "Closes")
- **Original:** "Fires when the value ends, however it ends"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the deliberate "ends … ends" echo is stylish but slightly repetitive.
- **Candidates:**
  1. "Fires when the value ends, however that happens" — C 9 / N 10 / Overall 10 — keeps the exact meaning ("in whatever manner it ends") while removing the repeated "ends"; stays terse for a cell.
  2. "Fires whenever the value ends, no matter how" — C 9 / N 9 / Overall 9 — removes the echo but adds two changes ("whenever" + "no matter how").
  3. "Fires when the value ends, for any reason" — C 8 / N 9 / Overall 8 — "for any reason" shifts nuance from manner to cause.
- **Decision:** Applied → "Fires when the value ends, however that happens" (new Overall 10)
- **Why:** "however that happens" drops the repetition with no meaning change and no added length.

### [11] `docs/pages/close/+Page.mdx` — after code block (line 68)
- **Original:** "All of these signal the server to stop producing and release resources."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — third verbatim occurrence of "stop producing and release resources" on the page; the repetition is noticeable.
- **Candidates:**
  1. "All of these tell the server to stop producing and free its resources." — C 9 / N 9 / Overall 9 — varies both verbs ("tell", "free its resources") while staying faithful, reducing the verbatim echo.
  2. "All of these let the server stop producing and free up resources." — C 8 / N 9 / Overall 8 — "let … stop" shifts nuance from active signaling to permitting.
  3. "All of these prompt the server to stop producing and free its resources." — C 9 / N 9 / Overall 9 — "prompt" is also faithful; near-tie with candidate 1.
- **Decision:** Applied → "All of these tell the server to stop producing and free its resources." (new Overall 9)
- **Why:** Breaks the third verbatim repetition with faithful synonyms ("tell" for signal, "free its resources" for release).

### [12] `docs/pages/close/+Page.mdx` — blockquote (line 70)
- **Original:** "`close()` ends a value **gracefully** (buffered data is flushed). To stop a call **immediately** instead, pass the pending call to `abort()` — the request is cancelled, and the pending call rejects with an `Abort` error (or, if you're mid-stream, the next read does):"
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — the closing "(or … the next read does)" relies on the reader carrying "rejects" forward, which is slightly elliptical.
- **Candidates:**
  1. "… the pending call rejects with an `Abort` error (or, if you're mid-stream, that error surfaces on the next read instead):" — C 10 / N 9 / Overall 10 — restates the action ("that error surfaces on the next read") so no verb-carrying is needed; avoids a second em-dash.
  2. "… the pending call rejects with an `Abort` error (or, if you're mid-stream, the next read rejects instead):" — C 9 / N 9 / Overall 9 — spells out the verb but repeats "rejects" and "a read rejecting" is slightly awkward.
  3. "… the pending call rejects with an `Abort` error — or, if you're mid-stream, the next read throws it:" — C 9 / N 8 / Overall 8 — makes the verb explicit but introduces a second em-dash in the same sentence.
- **Decision:** Applied → "… pass the pending call to `abort()` — the request is cancelled, and the pending call rejects with an `Abort` error (or, if you're mid-stream, that error surfaces on the next read instead):" (new Overall 10)
- **Why:** Removes the ellipsis by naming what happens to the error mid-stream; meaning and the trailing code block are preserved.

### [13] `docs/pages/close/+Page.mdx` — "`onClose()` (server)" (line 83)
- **Original:** "Use `onClose()` from <Link href="/getContext" text="getContext()" /> on the server to detect when the response ends — whether the value was consumed, the client disconnected, or an error occurred:"
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "on the server" floats between "getContext()" and "to detect", so for a beat it's unclear whether it qualifies the import or the usage.
- **Candidates:**
  1. "On the server, use `onClose()` from <Link href="/getContext" text="getContext()" /> to detect when the response ends — whether the value was consumed, the client disconnected, or an error occurred:" — C 10 / N 9 / Overall 10 — fronting "On the server," removes the floating modifier and reads naturally.
  2. "Use `onClose()` from <Link … /> to detect, on the server, when the response ends — …" — C 8 / N 7 / Overall 7 — awkward mid-clause insertion.
  3. "Use the server-side `onClose()` from <Link … /> to detect when the response ends — …" — C 9 / N 9 / Overall 9 — clean, but "server-side `onClose()`" can imply a distinct variant.
- **Decision:** Applied → "On the server, use `onClose()` from <Link href="/getContext" text="getContext()" /> to detect when the response ends — whether the value was consumed, the client disconnected, or an error occurred:" (new Overall 10)
- **Why:** Fronting the locative resolves the ambiguity; `<Link>` and the three-way list preserved.

### [19] `docs/pages/close/+Page.mdx` — "Automatic cleanup (GC)" (line 113, sentence 2)
- **Original:** "GC cleanup is the fallback for when you forget to close explicitly."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "the fallback for when" is a touch conversational for reference docs.
- **Candidates:**
  1. "GC cleanup is the fallback when you forget to close explicitly." — C 9 / N 10 / Overall 10 — drops the conversational "for" while keeping the sentence otherwise intact.
  2. "GC cleanup is the fallback if you forget to close explicitly." — C 9 / N 10 / Overall 10 — "if" is equally clean; near-tie.
  3. "GC cleanup is the fallback for cases where you forget to close explicitly." — C 9 / N 9 / Overall 9 — more formal but wordier.
- **Decision:** Applied → "GC cleanup is the fallback when you forget to close explicitly." (new Overall 10)
- **Why:** Removing "for" eliminates the conversational phrasing with the smallest possible edit.

---

## `docs/pages/serve/+Page.mdx`

### [21] `docs/pages/serve/+Page.mdx` — intro blockquote (line 7)
- **Original:** "For <Link href="/stream">streaming and real-time</Link> setups, use the runtime-specific `new Telefunc()` from `telefunc/node`, `telefunc/bun`, `telefunc/deno`, or `telefunc/cloudflare` instead."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — a long four-item import list mid-sentence, with "instead" stranded after it.
- **Candidates:**
  1. "For <Link href="/stream">streaming and real-time</Link> setups, use the runtime-specific `new Telefunc()` instead, imported from `telefunc/node`, `telefunc/bun`, `telefunc/deno`, or `telefunc/cloudflare`." — C 9 / N 10 / Overall 10 — "instead" moves next to the verb; the list now closes the sentence and reads smoothly.
  2. "… use the runtime-specific `new Telefunc()` instead — from `telefunc/node`, `telefunc/bun`, `telefunc/deno`, or `telefunc/cloudflare`." — C 9 / N 9 / Overall 9 — em-dash sets off the list; slightly more abrupt.
  3. "… use the runtime-specific `new Telefunc()` from your runtime's entry point (`telefunc/node`, `telefunc/bun`, `telefunc/deno`, or `telefunc/cloudflare`) instead." — C 9 / N 9 / Overall 9 — clarifies but adds words and keeps "instead" trailing.
- **Decision:** Applied → "For <Link href="/stream">streaming and real-time</Link> setups, use the runtime-specific `new Telefunc()` instead, imported from `telefunc/node`, `telefunc/bun`, `telefunc/deno`, or `telefunc/cloudflare`." (new Overall 10)
- **Why:** Un-strands "instead" and lets the long list fall at the sentence end where it reads naturally; `<Link>` preserved.

---

## `docs/pages/Telefunc/+Page.mdx`

### [29] `docs/pages/Telefunc/+Page.mdx` — intro (line 6)
- **Original:** "Use `new Telefunc()` to embed Telefunc into your server, with full-fledged support for <Link href="/stream">Telefunc Stream</Link>."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the trailing ", with full-fledged support for …" is a loosely attached modifier that reads slightly tacked-on.
- **Candidates:**
  1. "Use `new Telefunc()` to embed Telefunc into your server with full-fledged support for <Link href="/stream">Telefunc Stream</Link>." — C 9 / N 10 / Overall 10 — dropping the comma fuses the clause: "embed … into your server with full-fledged support for X".
  2. "Use `new Telefunc()` to embed Telefunc into your server, including full-fledged support for <Link …>Telefunc Stream</Link>." — C 9 / N 9 / Overall 9 — "including" attaches better than "with" but keeps the supplementary comma.
  3. "Embed Telefunc into your server with `new Telefunc()`, which provides full-fledged support for <Link …>Telefunc Stream</Link>." — C 9 / N 9 / Overall 9 — clean relative clause but restructures the sentence.
- **Decision:** Applied → "Use `new Telefunc()` to embed Telefunc into your server with full-fledged support for <Link href="/stream">Telefunc Stream</Link>." (new Overall 10)
- **Why:** Removing the comma turns the tacked-on modifier into one tight clause; meaning and `<Link>` unchanged.

### [32] `docs/pages/Telefunc/+Page.mdx` — Methods table (line 44, `serve(input)`)
- **Original:** "Process a request and return the response. The input shape (`{ request }`, `{ req, res }`, `{ request, env, ctx }`, …) and return type vary by runtime — see <Link href="/server#return-value" />."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — a long sentence with a big inline parenthetical for a table cell (but well formed).
- **Candidates:**
  1. "Process a request and return the response. Both the input shape (`{ request }`, `{ req, res }`, `{ request, env, ctx }`, …) and the return type vary by runtime — see <Link href="/server#return-value" />." — C 9 / N 9 / Overall 8 — "Both … and the" flags the compound subject up front, but the flagged parenthetical length is unchanged.
  2. "Process a request and return the response. The input shape and return type vary by runtime (`{ request }`, `{ req, res }`, `{ request, env, ctx }`, …) — see <Link href="/server#return-value" />." — C 8 / N 8 / Overall 8 — moving the parenthetical to the end misattaches the examples to "runtime" rather than "input shape".
  3. "Process a request and return the response — the input shape (`{ request }`, `{ req, res }`, `{ request, env, ctx }`, …) and return type vary by runtime. See <Link href="/server#return-value" />." — C 9 / N 8 / Overall 8 — em-dash join doesn't reduce the parenthetical and adds a dash.
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** The documented weakness is the parenthetical's length, which carries the useful concrete input shapes; shrinking it loses information and the alternatives either misattach the examples or leave the issue untouched. No genuine gain.

### [38] `docs/pages/Telefunc/+Page.mdx` — "Configuration (Cloudflare)" (line 67)
- **Original:** "On Cloudflare the constructor also takes `bindingName`, `kvBindingName`, `scale`, `locationFallback`, `jurisdiction`, … — see <Link href="/stream/cloudflare#configuration" />."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — a long inline option list ending in "… —" is slightly clunky (ellipsis immediately followed by em-dash).
- **Candidates:**
  1. "On Cloudflare the constructor also takes `bindingName`, `kvBindingName`, `scale`, `locationFallback`, `jurisdiction`, and more — see <Link href="/stream/cloudflare#configuration" />." — C 10 / N 10 / Overall 10 — "and more" replaces the bare "…", removing the "… —" adjacency while keeping the non-exhaustive sense.
  2. "On Cloudflare the constructor also takes options like `bindingName`, `kvBindingName`, `scale`, `locationFallback`, and `jurisdiction` — see <Link …/>." — C 9 / N 10 / Overall 10 — "options like" signals non-exhaustive and drops the trailing "…", but restructures the list head.
  3. "On Cloudflare the constructor also takes further options — `bindingName`, `kvBindingName`, `scale`, `locationFallback`, `jurisdiction`, and more; see <Link …/>." — C 9 / N 9 / Overall 9 — clear but heavier restructuring.
- **Decision:** Applied → "On Cloudflare the constructor also takes `bindingName`, `kvBindingName`, `scale`, `locationFallback`, `jurisdiction`, and more — see <Link href="/stream/cloudflare#configuration" />." (new Overall 10)
- **Why:** "and more" eliminates the clunky ellipsis-dash sequence, preserves the non-exhaustive meaning, and reads naturally; `<Link>` preserved.

---

## Summary

- **Targets:** 18
- **Applied:** 16
- **Retained:** 2 — close [5] ("walked recursively") and Telefunc [32] (Methods-table parenthetical)
- **New score distribution (post-pass):**
  - Overall 10 — 13 targets: server [2], server [5], getContext [20], getContext [21], getContext [25], close [1], close [6], close [12], close [13], close [19], serve [21], Telefunc [29], Telefunc [38].
  - Overall 9 — 3 targets: close [3] ("several per-type APIs"), close [4] ("versus"), close [11] ("tell … free its resources").
  - Overall 8 (retained) — 2 targets: close [5], Telefunc [32].
- **Notes:** All edits are prose-only. Every `<Link>`, `<code>`, inline code span, bold emphasis, em-dash, table cell, and anchor (`#return-value`, `#configuration`) was preserved and verified by reading back each changed region. No headings touched. American English maintained. No targets reference `withContext/+Page.mdx` or `provideTelefuncContext/+Page.mdx`, so those files were not modified.
