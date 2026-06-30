# Sentence-by-sentence review of the Telefunc Stream docs (PR #264)

This document is a full, sentence-by-sentence technical-writing review of **every new
documentation page introduced by [PR #264 (`feat: stream`)](https://github.com/telefunc/telefunc/pull/264)**.

Each prose sentence is rated on two axes (1–10):

- **Clarity** — Is the sentence crystal clear? Easy to understand, **zero ambiguity**, no fuzzy/vague
  words, the reader never has to second-guess the meaning.
- **Naturalness** — Does it read like idiomatic, professional JavaScript/TypeScript documentation?
  No weird, awkward, or unfamiliar phrasing for this context.

## Scoring discipline

- **10** = genuinely flawless on that axis (reserved — most good sentences land at 9, not 10).
- **8–9** = good, with a minor nit named in the reason.
- **6–7** = noticeably weak (ambiguity, fuzzy word, agreement error, awkward phrasing) → **edited**.
- **≤5** = seriously unclear or unnatural (broken grammar, typo, wrong word) → **edited**.

Every score below 10 carries a one-line reason naming the exact word or construction that costs it
the point(s). A sentence was **edited in place** when its clarity *or* naturalness was **≤7**. After
each edit the new wording was re-rated; if the best wording had still scored ≤7 it would have been
held back for a *second PR* (per the brief). **No edit fell into that bucket** — every applied edit
re-rated ≥8 on both axes, so this is a single PR.

## What is *not* rated

Code inside fenced code blocks, inline code identifiers, import lines, JSX component tags/props,
URLs/`href`s, and comments inside code fences are out of scope — only natural-language prose is rated.
All edits preserve meaning, MDX/JSX, links, tables, and code **byte-for-byte except the prose words
changed**. The docs lint gate (`node docs/check-docs.mjs`) passes after editing (53 pages, 3
components, 113 internal anchor links resolve).

## Scope and tally

| # | Page | Prose units | Edited |
|---|------|-------------|--------|
| 1 | `docs/pages/stream/+Page.mdx` | 66 | 9 |
| 2 | `docs/pages/channel/+Page.mdx` | 83 | 3 |
| 3 | `docs/pages/transport/+Page.mdx` | 37 | 1 |
| 4 | `docs/pages/onClose/+Page.mdx` | 27 | 2 |
| 5 | `docs/pages/close/+Page.mdx` | 31 | 2 |
| 6 | `docs/pages/withContext/+Page.mdx` | 11 | 0 |
| 7 | `docs/pages/serve/+Page.mdx` | 23 | 1 |
| 8 | `docs/pages/Telefunc/+Page.mdx` | 24 | 1 |
| 9 | `docs/pages/provideTelefuncContext/+Page.mdx` | 6 | 0 |
| 10 | `docs/pages/testing/+Page.mdx` | 10 | 0 |
| 11 | `docs/pages/file-download/+Page.mdx` | 45 | 1 |
| 12 | `docs/pages/tanstack-query/+Page.mdx` | 35 | 3 |
| 13 | `docs/pages/rxjs/+Page.mdx` | 28 | 1 |
| 14 | `docs/pages/redis/+Page.mdx` | 15 | 1 |
| 15 | `docs/pages/stream/scale/+Page.mdx` | 31 | 2 |
| 16 | `docs/pages/stream/cloudflare/+Page.mdx` | 73 | 1 |
| 17 | `docs/pages/channel-config/+Page.mdx` | 6 | 0 |
| 18 | `docs/components/NeedsLongRunningServer.mdx` | 2 | 0 |
| | **Total** | **~553** | **29** |

**29 edits applied across 13 files. 0 sentences deferred to a second PR.**

---

# 1. `docs/pages/stream/+Page.mdx`

#### Sentence ratings

1. "*Telefunc Stream* supports streaming (one-way streams) and real-time (two-way streams) with:" — Clarity 9/10 ("with:" leads into a terse list, slight abruptness); Naturalness 9/10. KEEP
2. "Primitives" (bullet) — Clarity 9/10; Naturalness 9/10. KEEP
3. "Integrations" (bullet) — Clarity 9/10; Naturalness 9/10. KEEP
4. "Seamless DX" (bullet) — Clarity 9/10; Naturalness 9/10. KEEP
5. "The word *stream* denotes — broadly speaking and [as Wikipedia defines it] — not only a ReadableStream but any sequence of data made available over time." — Clarity 8/10 (double em-dash interruption is dense but parseable); Naturalness 8/10 (heavy parenthetical). KEEP
6. "API for advanced real-time use cases." (Channel link label) — Clarity 9/10; Naturalness 9/10. KEEP
7. "A telefunction can mix generators, streams, and promises side by side, each resolving independently." — Clarity 8/10 (doesn't convey "a single call" without "single"); Naturalness 9/10. **EDITED**
8. "automatically synced TanStack queries." (link label) — Clarity 9/10; Naturalness 9/10. KEEP
9. "reactive streams and operators." (link label) — Clarity 9/10; Naturalness 9/10. KEEP
10. "Integrations provide an even more seamless and ergonomic DX." — Clarity 9/10; Naturalness 8/10 ("seamless and ergonomic" piles two near-synonyms). KEEP
11. "They're powered by the primitives listed here." — Clarity 9/10; Naturalness 9/10. KEEP
12. "Runtime type validation — automatically validates every value sent from the client to the server against TypeScript types (no need for Zod)." — Clarity 9/10; Naturalness 9/10. KEEP
13. "Transport — automatically picks the most performant available transport (HTTP, SSE, WebSocket)." — Clarity 9/10; Naturalness 9/10. KEEP
14. "Backpressure — automatic backpressure when the network is the bottleneck." — Clarity 9/10; Naturalness 9/10. KEEP
15. "Reconnection — automatic recovery from network issues." — Clarity 9/10; Naturalness 9/10. KEEP
16. "An async function* (AsyncGenerator) is a good fit for short-lived streaming: a finite sequence of values that arrive one at a time and then completes." — Clarity 9/10 (number agreement "values that arrive… and then completes" is slightly bumpy); Naturalness 9/10. KEEP
17. "AI answer stream" (bullet) — Clarity 9/10; Naturalness 9/10. KEEP
18. "Progress updates for a long-running task (e.g. upload)" — Clarity 9/10; Naturalness 9/10. KEEP
19. "Search results streaming in as they're found" — Clarity 9/10; Naturalness 9/10. KEEP
20. "Rows of a large SQL query, streamed to the UI" — Clarity 9/10; Naturalness 9/10. KEEP
21. "It's a good fit because a generator closes itself when it completes." — Clarity 9/10; Naturalness 9/10. KEEP
22. "(Other streaming primitives like callbacks don't have such completion signal.)" — Clarity 8/10 (missing article "such **a**"); Naturalness 7/10 (ungrammatical). **EDITED**
23. "You can also use it for any other use cases such as never-ending streams if you want — the only deciding factor is DX." — Clarity 7/10 (missing commas make it run on); Naturalness 7/10 (awkward plural "any other use cases such as"). **EDITED**
24. "(Telefunc picks a different default transport but you change it.)" — Clarity 7/10 ("different" from what is implicit; "you change it" reads as fact, not capability); Naturalness 6/10 ("you change it" should be "you can change it"). **EDITED**
25. "A simple example:" — Clarity 9/10; Naturalness 9/10. KEEP
26. "A full-fledged example:" — Clarity 9/10; Naturalness 9/10. KEEP
27. "You can pass functions between client and server:" — Clarity 9/10; Naturalness 9/10. KEEP
28. "Client → server: pass a callback as a telefunction argument — the server calls it." — Clarity 9/10; Naturalness 9/10. KEEP
29. "Server → client: return a function from a telefunction — the client calls it." — Clarity 9/10; Naturalness 9/10. KEEP
30. "The underlying stream automatically closes itself when the client stops using (i.e. referencing) onProgress, but you can also manually close eagerly, see /close." — Clarity 6/10 ("stops using (i.e. referencing)" hedges; "manually close eagerly" lacks an object); Naturalness 6/10 ("close eagerly" with no "it" reads truncated). **EDITED**
31. "Under the hood, a passed function is just a Channel." — Clarity 9/10; Naturalness 9/10. KEEP
32. "A callback has the same lifecycle as a channel, just exposed with a nice JavaScript DX." — Clarity 8/10 ("just exposed with a nice DX" is loose); Naturalness 8/10. KEEP
33. "Reach for a raw Channel if you need more than call-and-return — e.g. two-way messaging, broadcast, or binary." — Clarity 9/10; Naturalness 9/10. KEEP
34. "If you return promises without awaiting them, the client receives the rest of the object immediately, and each promise resolves on its own." — Clarity 9/10; Naturalness 9/10. KEEP
35. "Like other streaming primitives, you can use onClose() and close to eagerly cancel fetchExtensiveReport()" — Clarity 8/10 (dangling modifier — the reader, not "you", is compared); Naturalness 8/10 (missing terminal period). **EDITED**
36. "Don't await inside the telefunction — that blocks the entire response:" — Clarity 9/10; Naturalness 9/10. KEEP
37. "Return the promise instead:" — Clarity 9/10; Naturalness 9/10. KEEP
38. "Stream raw bytes with a standard ReadableStream — in either direction." — Clarity 9/10; Naturalness 9/10. KEEP
39. "Return a ReadableStream:" — Clarity 9/10; Naturalness 9/10. KEEP
40. "You can also return a File or Blob instead." — Clarity 9/10; Naturalness 9/10. KEEP
41. "Pass a ReadableStream as a telefunction argument:" — Clarity 9/10; Naturalness 9/10. KEEP
42. "Accept a ReadableStream and return one, transforming the bytes as they pass through." — Clarity 9/10; Naturalness 9/10. KEEP
43. "The simplest transform is gzip compression:" — Clarity 8/10 ("the simplest transform" reads absolute rather than relative to this context); Naturalness 8/10. **EDITED**
44. "The transform can be arbitrarily heavy — for example, compressing a large video into a smaller MP4 and streaming it back to the client as it encodes:" — Clarity 9/10; Naturalness 9/10. KEEP
45. "The memory consumption is constant regardless of file size." — Clarity 9/10; Naturalness 9/10. KEEP
46. "A spawned process outlives the call and keeps burning CPU if the client navigates away mid-encode — make sure to close it in onClose(), see Cleanup." — Clarity 8/10 ("close it" for a process is odd — you terminate/kill a process); Naturalness 7/10 (non-idiomatic vs the code's kill()). **EDITED**
47. "For ongoing two-way and/or broadcast messaging, you can use a channel, see:" — Clarity 8/10 ("and/or" is bureaucratic); Naturalness 8/10. KEEP
48. "This is the low-level primitive that powers Telefunc Stream." — Clarity 9/10; Naturalness 9/10. KEEP
49. "Instead of directly using channels, most users reach for high-level primitives (e.g. callbacks) and high-level integrations (e.g. @telefunc/tanstack-query)." — Clarity 9/10; Naturalness 9/10. KEEP
50. "Make sure you always clean up resources:" — Clarity 9/10; Naturalness 9/10. KEEP
51. "Any resource a telefunction opens (setInterval, an event subscription, a DB cursor, an upstream stream) outlives the telefunction call — make sure to always clear resources when the stream closes." — Clarity 9/10; Naturalness 8/10 ("clear resources" vs the earlier "clean up resources" — minor inconsistency). KEEP
52. "You can also use a signal (AbortSignal) and channel.onClose() to listen for when a stream closes, see:" — Clarity 8/10 ("listen for when" slightly clunky); Naturalness 8/10. KEEP
53. "A stream automatically closes when the client stops using it — you usually don't have to manually close streams, see:" — Clarity 9/10; Naturalness 9/10. KEEP
54. "A streaming telefunction authorizes like any other telefunction (see /permissions): check getContext() and throw Abort() at open time, before you stream anything back." — Clarity 9/10; Naturalness 9/10. KEEP
55. "Here the client passes a callback and the server calls it:" — Clarity 9/10; Naturalness 9/10. KEEP
56. "getContext() and throw Abort() only work before the telefunction returns (see /getContext#access) — so authorize there, at open time." — Clarity 9/10; Naturalness 9/10. KEEP
57. "The user you captured stays valid inside the callback, which runs later." — Clarity 9/10; Naturalness 9/10. KEEP
58. "Consider re-checking. For sensitive operations, you can re-check authorization with the captured user each time the callback runs. Although most use cases don't need this." — Clarity 8/10; Naturalness 6/10 (sentence fragment starting with "Although"). **EDITED**
59. "Environment: server & client." — Clarity 9/10; Naturalness 9/10. KEEP
60. "The expected-vs-bug distinction described at /error-handling applies to streams too:" — Clarity 9/10; Naturalness 9/10. KEEP
61. "throw Abort() during a stream is relayed as an expected error" — Clarity 9/10; Naturalness 9/10. KEEP
62. "Any other thrown error is treated as a bug" — Clarity 9/10; Naturalness 9/10. KEEP
63. "The channel.onClose() hook can be used on the server- and client-side." — Clarity 9/10; Naturalness 8/10 (restates "Environment: server & client" — minor redundancy). KEEP
64. "You can cancel a stream at anytime by manually closing it — you can then use the onClose() hook to clear resources (server-side)." — Clarity 8/10 ("anytime" should be "any time" as an adverbial phrase); Naturalness 8/10. KEEP
65. "You can scale Telefunc horizontally (multiple instances/containers/machines) by adding sticky sessions and a cross-instance broadcast transport." — Clarity 9/10; Naturalness 9/10. KEEP
66. "On Cloudflare Workers, Telefunc routes channels through Durable Objects and fans out broadcasts across regions automatically — so neither a sticky load balancer nor a broadcast transport is needed." — Clarity 9/10; Naturalness 9/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 7 | "A telefunction can mix…" → "A **single** telefunction can mix…" | Clarity 9, Naturalness 9 — "single" makes the "one call does all three" point explicit. |
| 22 | "…don't have such completion signal.)" → "…don't have such **a** completion signal.)" | Clarity 9, Naturalness 9 — inserts the missing article; meaning unchanged. |
| 23 | "…any other use cases such as never-ending streams if you want…" → "…any other **use case, such as never-ending streams,** if you want…" | Clarity 9, Naturalness 9 — singular + bracketing commas fix the run-on. |
| 24 | "(Telefunc picks a different default transport but you change it.)" → "(Telefunc picks a different default transport **in that case, but you can change it.**)" | Clarity 9, Naturalness 9 — "in that case" anchors "different"; "you can change it" states capability. |
| 30 | "…stops using (i.e. referencing) onProgress, but you can also manually close eagerly…" → "…stops **referencing** onProgress, but you can also **close it eagerly yourself**…" | Clarity 9, Naturalness 9 — drops the hedge; gives "close" an object. |
| 35 | "Like other streaming primitives, you can use…" → "**As with** other streaming primitives, you can use…cancel fetchExtensiveReport()**.**" | Clarity 9, Naturalness 9 — removes dangling modifier; adds period. |
| 43 | "The simplest transform is gzip compression:" → "The simplest **such** transform is gzip compression:" | Clarity 9, Naturalness 9 — scopes the claim to the bidirectional-transform topic. |
| 46 | "make sure to **close it** in onClose()" → "make sure to **terminate it** in onClose()" | Clarity 9, Naturalness 9 — "terminate" matches the `ffmpeg.kill()` in the code. |
| 58 | "…each time the callback runs. Although most use cases don't need this." → "…each time the callback runs**, although** most use cases don't need this." | Clarity 9, Naturalness 9 — joins the clause, removing the "Although…" fragment. |

> **Note on sentence 22.** The reviewing pass initially held "such a completion signal" as a *second-PR
> candidate* out of caution (it touches an accuracy-sensitive parenthetical), but its best wording
> rated 9/9, well above the ≤7 threshold for deferral, so it was applied in this PR. Ten alternative
> wordings were weighed: "such a completion signal" (9/9, chosen — minimal, preserves the comparison),
> "any completion signal" (9/9 but shifts nuance), "no such completion signal" (9/8), "lack such a
> completion signal" (9/8), "don't provide a completion signal" (9/9, drops "such"), "a comparable
> completion signal" (8/8), "no equivalent completion signal" (8/7), "no built-in completion signal"
> (8/8, adds an unstated claim), "don't signal completion on their own" (8/8), "don't have such a
> signal" (8/8, drops "completion"). The winner is the smallest change that keeps the exact meaning.

**Tally:** 66 reviewed · 9 edited · 0 deferred.

---

# 2. `docs/pages/channel/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: server." — Clarity 9/10; Naturalness 9/10. KEEP
2. "The following low-level **primitives** enable all kinds of stream use cases." — Clarity 8/10 ("all kinds of" is mildly vague); Naturalness 9/10. KEEP
3. "direct messages between one server and one client." — Clarity 9/10; Naturalness 9/10. KEEP
4. "a keyed pub/sub bus (publish()/subscribe()), server-side." — Clarity 9/10; Naturalness 9/10. KEEP
5. "broadcast also on the client-side." (list label) — Clarity 8/10 ("also" placement slightly off); Naturalness 8/10 (label fragment). KEEP (the prose equivalent at the `new BroadcastChannel()` heading was edited instead)
6. "You can use them for both one-way (aka streaming) and two-way streams (aka real-time):" — Clarity 7/10 ("one-way" lacks the noun "streams"; only parses on second pass); Naturalness 7/10 (asymmetric). **EDITED**
7. "Chat rooms / Live feeds (sports scores, stock tickers) / Collaborative editing (presence & cursors) / Multiplayer games / Live dashboards & metrics / Notifications & activity feeds" — Clarity 9/10; Naturalness 9/10. KEEP
8. "new Channel() and new BroadcastChannel() are returned from a telefunction — they serialize into client-side objects automatically." — Clarity 9/10; Naturalness 9/10. KEEP
9. "Broadcast is a server-side API, so there's nothing to return." — Clarity 9/10; Naturalness 9/10. KEEP
10. "By default, channels and broadcasts use SSE and work without extra server setup." — Clarity 9/10; Naturalness 9/10. KEEP
11. "When WebSocket is enabled …, the client starts on SSE and seamlessly upgrades to WebSocket in the background …" — Clarity 9/10; Naturalness 9/10. KEEP
12. "For short-lived callbacks (e.g. progress updates), function passing is usually simpler." — Clarity 9/10; Naturalness 9/10. KEEP
13. "For channel configurations, see:" — Clarity 9/10; Naturalness 9/10. KEEP
14. "new Channel() creates a private, two-way message pipe between the server and the one client that called the telefunction." — Clarity 9/10; Naturalness 9/10. KEEP
15. "The server keeps the Channel object and hands the client its end by returning the channel's .client." — Clarity 8/10 ("its end" anaphora is compact but resolvable); Naturalness 9/10. KEEP
16. "The channel outlives the telefunction call: both ends can send() and listen() until one side closes it." — Clarity 9/10; Naturalness 9/10. KEEP
17. "The simplest case — the server pushes live updates to the one client that opened the channel:" — Clarity 9/10; Naturalness 9/10. KEEP
18. "The rest of this section adds types, two-way messaging, acks, and binary data; see … for the full API." — Clarity 9/10; Naturalness 9/10. KEEP
19–24. Method-table cells ("Send a message. Await to apply backpressure.", "Send and await acknowledgement.", "Receive messages. Return a value to ack. Returns an unlisten function.", "Lifecycle callbacks.", "Close gracefully." / "Terminate immediately.", "Server: return the channel's .client from the telefunction.") — Clarity 9/10; Naturalness 9/10. KEEP
25. "The client gets the same API with message directions flipped." — Clarity 8/10 ("directions flipped" compact but clear); Naturalness 9/10. KEEP
26–31. Generic-type explanation block (two generic type parameters; argument is the message type; return type is the acknowledgement type; send() direction per side; ".client type flips the message types") — Clarity 8–9/10; Naturalness 9/10. KEEP
32. "Broadcast is a keyed pub/sub bus: a message published to a key reaches every subscriber of that key…" — Clarity 9/10; Naturalness 9/10. KEEP
33. "Publishers and subscribers are decoupled; all they share is the string key." — Clarity 9/10; Naturalness 9/10. KEEP
34. "It's the fan-out layer that BroadcastChannel (below) is built on." — Clarity 9/10; Naturalness 9/10. KEEP
35. "The static methods run purely on the server — no client, no handle, no lifecycle — and take the key as their first argument:" — Clarity 9/10; Naturalness 9/10. KEEP
36–38. "publish() returns a receipt and subscribe() receives the same info:", "seq is monotonic per key, useful for ordering and gap detection.", "publish() resolves to info once the message is accepted." — Clarity 9/10; Naturalness 9/10. KEEP
39–41. "By default, broadcast is in-memory — messages only reach subscribers on the same server.", "This works out-of-the-box for single-server deployments.", "For scaling horizontally, you must configure Broadcast to publish across all server instances — see …" — Clarity 9/10; Naturalness 9/10. KEEP
42. "new BroadcastChannel({ key }) enables you to broadcast also on the client-side." — Clarity 7/10 ("enables you to … also" awkward, "also on" placement fuzzy); Naturalness 6/10 (non-idiomatic). **EDITED**
43. "It creates a Channel to the one client that received it, and bridges that client onto a Broadcast key." — Clarity 9/10; Naturalness 9/10. KEEP
44. "Return it from a telefunction and that client can publish() / subscribe() on the key — every message reaches every member of the group (both client- and server-side)." — Clarity 8/10 ("both client- and server-side" suspended hyphen); Naturalness 7/10 (suspended-hyphen reads clunky). **EDITED**
45–50. Security block ("Keys are capabilities — anyone who knows the key joins the group.", "Secure a broadcast in one of two ways:", "Guard the key…", "Guard the payload…", the tanstack-query safety example, "Unlike channels…, no .client is needed: broadcast is symmetric…") — Clarity 9/10; Naturalness 9/10. KEEP
51. "A per-user key with a subscribe-only client — the server returns a BroadcastChannel keyed to the user, and the client only listens:" — Clarity 9/10; Naturalness 9/10. KEEP
52–57. BroadcastChannel method table + "The instance is bound to its key…" + "The static Broadcast.publish(key, data) / Broadcast.subscribe(key, cb) take it first — and have no onOpen / onClose / close / abort…" — Clarity 9/10; Naturalness 9/10. KEEP
58–64. Channel-vs-broadcast prose ("a channel message is addressed to *someone*, a broadcast message is addressed to *a topic*", phone-call analogy, radio-frequency analogy, "Only the two ends of a channel can use it…", "A channel ends when either side closes it…", "You can think of new BroadcastChannel() as new Channel() plus the static…", "The server holds a private channel… then bridges that client into the keyed broadcast group…") — Clarity 9/10; Naturalness 9/10. KEEP
65–67. Comparison-table cells — Clarity 9/10; Naturalness 9/10. KEEP
68–74. Errors table + "Channels and broadcasts signal failure through four errors:" + "A graceful close() is not an error (onClose(err) receives undefined)." — Clarity 8–9/10; Naturalness 9/10. KEEP
75–79. Reconnection block ("If the connection drops, Telefunc reconnects automatically and resumes existing channels and broadcasts:", "Messages sent while offline are buffered and delivered in order on reconnect.", "Both sides keep a bounded replay buffer; after reconnect, missing frames are replayed.", "onOpen() fires only on initial open, onClose() only on permanent close.", "Reconnection is automatic — you don't need to handle it in your application code.") — Clarity 9/10; Naturalness 9/10. KEEP
80–83. Binary block ("Send and receive raw binary alongside structured messages:", "Both send() and sendBinary() return a Promise that resolves when the receiver has capacity for more data.", "Await them in a loop to apply backpressure:", "Fire-and-forget is also fine — data is always sent immediately regardless of whether you await:") — Clarity 9/10; Naturalness 9/10. KEEP

#### Edits applied

| Line | Before → After | New rating |
|---|----------------|-----------|
| 14 | "…both one-way (aka streaming) and two-way streams…" → "…both one-way **streams** (aka streaming) and two-way streams…" | Clarity 9, Naturalness 9 — adds the noun "streams" so the two items are parallel. |
| 246 | "new BroadcastChannel({ key }) **enables you to broadcast also on** the client-side." → "new BroadcastChannel({ key }) **lets you broadcast from the client-side as well.**" | Clarity 9, Naturalness 9 — idiomatic; precise about where the broadcast originates. |
| 250 | "…(both **client- and server-side**)." → "…(both **client-side and server-side**)." | Clarity 9, Naturalness 8 — removes the awkward suspended hyphen. |

**Tally:** 83 reviewed · 3 edited · 0 deferred.

---

# 3. `docs/pages/transport/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: client." — Clarity 9/10; Naturalness 9/10. KEEP
2. "Telefunc has two transport settings to control what network protocol to use for delivering messages:" — Clarity 7/10 ("what network protocol to use for delivering" is a clumsy double-infinitive); Naturalness 6/10 (stacked infinitives). **EDITED**
3–37. The remainder (the streaming-transport table and rows "Raw binary chunked response. Lowest overhead.", "Base64url-encoded SSE…", "Starts over HTTP, then continues over the configured channel transport.", the comparison legend, the "When to use what" guidance, the channel-transport table, "All channels share a single multiplexed connection per server URL…", the recommended-setup table, the per-call-overrides and "Channel & broadcast config" sections, "See also") — Clarity 8–10/10; Naturalness 8–10/10. KEEP. Lowest non-edited: sentence 18 "Controls how new Channel() and new BroadcastChannel() connections work — and which backend config.stream.transport = 'channel' uses." (8/8, slightly compressed but clear in context).

#### Edits applied

| Line | Before → After | New rating |
|---|----------------|-----------|
| intro | "Telefunc has two transport settings **to control what network protocol to use for delivering** messages:" → "Telefunc has two transport settings **that control which network protocol is used to deliver** messages:" | Clarity 9, Naturalness 9 — removes the stacked infinitives; "which" is precise for a closed set. |

**Tally:** 37 reviewed · 1 edited · 0 deferred.

---

# 4. `docs/pages/onClose/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: server & client." — Clarity 9/10; Naturalness 9/10. KEEP
2. "You can listen when Telefunc streams close to:" — Clarity 8/10 (the trailing "to:" before the list reads slightly oddly); Naturalness 8/10. KEEP
3–9. Reason bullets + table header/cells ("Clear resources", "Make sure you **always clear long-lived resources** (setInterval, an event subscription, a DB cursor, an upstream stream, …).", "Handle errors", "Cancel in-flight work", "*all* stream closes", "*the one* channel closes", "API | Side | Fires when") — Clarity 8–10/10; Naturalness 7–10/10. KEEP (row "*the one* channel closes" re-considered at 8/10 — the unusual emphasis is a deliberate parallel with "*all* stream closes").
10. "The listeners are fired regardless of the closing reason — whether all streams finished, the client stops using the streams, the client manually closed, the client disconnected, or an error occurred." — Clarity 6/10 (tense disagreement across the clauses; "the client manually closed" lacks an object); Naturalness 6/10 ("regardless of the closing reason" is stiff). **EDITED**
11. "Every stream eventually ends: at latest when the client disconnects …, but usually sooner when the client stops using the stream, the client manually closes the stream, the network connection is permanently lost (the network connection couldn't be recovered), or an error occurred." — Clarity 6/10 (redundant repeat of "the network connection"; "at latest" missing article); Naturalness 6/10 (redundancy + tense slip). **EDITED**
12–27. "To close streams, see: …" + the `context.onClose()`, `channel.onClose()`, and `context.signal` sub-sections ("The onClose() hook (defined on context) is called when *all* the streams opened by a telefunction call close.", "The onClose() hook defined on a new Channel() instance is called when that channel closes.", "The context object includes a signal (AbortSignal) that aborts when the telefunction call closes.", "The signal fires at the same time as onClose().", "Pass it to any API that accepts an AbortSignal (fetch(), database clients, etc.) to cancel in-flight work:") — Clarity 9–10/10; Naturalness 8–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 10 | "The listeners **are fired regardless of the closing reason** — whether all streams finished, the client **stops** using the streams, the client **manually closed**, the client disconnected, or an error occurred." → "The listeners **fire regardless of why the stream closed** — whether all streams finished, the client **stopped** using the streams, the client **closed them manually**, the client disconnected, or an error occurred." | Clarity 9, Naturalness 9 — uniform past tense; supplies the missing object; active "listeners fire". |
| 11 | "Every stream eventually ends**: at latest** when the client disconnects …, but usually sooner when … the network connection is permanently lost **(the network connection couldn't be recovered)**, or an error **occurred**." → "Every stream eventually ends **— at the latest** when the client disconnects …, but usually sooner**:** when … the network connection is permanently lost **(it couldn't be recovered)**, or an error **occurs**." | Clarity 9, Naturalness 9 — removes the redundant noun repeat; fixes "at the latest"; consistent present tense. |

**Tally:** 27 reviewed · 2 edited · 0 deferred.

---

# 5. `docs/pages/close/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: client & server." — Clarity 10/10; Naturalness 10/10. KEEP
2. "Manually close Telefunc streams." — Clarity 9/10; Naturalness 9/10. KEEP
3. "The close() function is one way amongst other to manually close streams — see all methods at …" — Clarity 7/10 (grammatical error "one way amongst other"); Naturalness 5/10 (broken English). **EDITED**
4. "You usually don't need to manually close streams yourself, see: …" — Clarity 9/10 (comma splice but readable); Naturalness 8/10 ("yourself" slightly redundant). KEEP
5. "For listing to streams closing, see: …" — Clarity 4/10 ("listing" is a typo for "listening" — wrong word); Naturalness 4/10. **EDITED**
6–14. "When to close" section ("A stream automatically closes itself when the client stops using it.", "**How does it work?** When a stream object becomes unreachable …, the browser's garbage collector clears the stream object and the stream automatically closes itself.", "You therefore usually don't have to manually close streams yourself.", "That said, there is a short delay (typically a few seconds) between when the client stops using the stream and when the stream is closed.", "(The garbage collector doesn't always immediately clear unused objects.)", "For example:", the "It's optional…" sentence, "That said, if you want the stream to close as soon as possible…") — Clarity 8–9/10; Naturalness 8–9/10. KEEP
15–31. "How to close" section + method table + "Gracefully closes *all* streams…", "To stop a call **immediately** instead of gracefully, use abort()…", "To abort via an AbortSignal instead, use withContext(fn, { signal }).", "See also" — Clarity 8–10/10; Naturalness 8–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 3 | "The close() function is **one way amongst other** to manually close streams…" → "The close() function is **one of several ways** to manually close streams…" | Clarity 10, Naturalness 10 — fixes the broken phrase; idiomatic. |
| 5 | "For **listing** to streams closing, see: …" → "For **listening** to streams closing, see: …" | Clarity 9, Naturalness 9 — corrects the wrong-word typo; now matches the linked `/onClose` target. |

**Tally:** 31 reviewed · 2 edited · 0 deferred.

---

# 6. `docs/pages/withContext/+Page.mdx`

#### Sentence ratings

1. "**Environment**: client." — Clarity 10/10; Naturalness 9/10. KEEP
2. "withContext(telefunction, context) from telefunc/client wraps a telefunction with **per-call client context** — applied to that one call instead of the global client-side config." — Clarity 9/10; Naturalness 9/10. KEEP
3. "Use it for an AbortSignal, extra headers, a URL override, or per-call transport overrides." — Clarity 9/10; Naturalness 9/10. KEEP
4. "withContext() returns a wrapped function with the same signature — call it exactly like the original telefunction." — Clarity 9/10; Naturalness 9/10. KEEP
5. "Cancel this call and any channels it opens. See also: …" — Clarity 9/10; Naturalness 9/10. KEEP
6. "Extra HTTP headers for this call. Global default: config.headers." — Clarity 9/10; Naturalness 9/10. KEEP
7. "Override config.telefuncUrl for this call and its channels." — Clarity 9/10; Naturalness 9/10. KEEP
8. "Override config.stream.transport for streaming." — Clarity 9/10; Naturalness 9/10. KEEP
9. "Override config.channel.transports for channels." — Clarity 9/10; Naturalness 9/10. KEEP
10. "By default, all channel calls to the same URL share one connection. Set a key to isolate calls onto separate connections: calls with the same key share a connection, while different keys (and keyless calls) each get their own." — Clarity 8/10 ("isolate calls onto separate connections" is dense, but the following clause disambiguates); Naturalness 9/10. KEEP
11. "How long (ms) to keep the underlying connection open after all channels close. Default 60_000; 0 closes it immediately." — Clarity 9/10; Naturalness 9/10. KEEP

#### Edits applied
None — all sentences ≥8 on both axes.

**Tally:** 11 reviewed · 0 edited · 0 deferred.

---

# 7. `docs/pages/serve/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: server." — Clarity 10/10; Naturalness 10/10. KEEP
2. "Low-level function that turns a telefunction HTTP request into an HTTP response." — Clarity 10/10; Naturalness 10/10. KEEP
3. "It's a pure function: stateless and side-effect-free." — Clarity 10/10; Naturalness 10/10. KEEP
4. "It runs in every runtime without adapter." — Clarity 7/10 ("without adapter" missing article; "every runtime" slightly over-strong); Naturalness 6/10 (missing "an"). **EDITED**
5. "**Most apps should use new Telefunc() instead** — it's the standard server integration." — Clarity 10/10; Naturalness 10/10. KEEP
6. "Because new Telefunc() has full-fledged support for Telefunc Stream, whereas serve() doesn't support the following:" — Clarity 8/10 (opens with "Because" as a standalone sentence); Naturalness 8/10. KEEP
7–10. WebSocket / Channels-on-Cloudflare bullets and their parentheticals ("You can still use Telefunc Stream but Telefunc won't use the WebSocket transport. (Another transport will be used.)", "You won't be able to use channels on Cloudflare at all. (Because channels need Durable Objects.)") — Clarity 9/10; Naturalness 8/10 (missing comma before "but"; parenthetical fragments). KEEP
11–23. The request/response usage prose + `httpResponse` field table ("Pass the web-standard Request object:", "Convert the result to a Response:", "Pass a context object to make request-scoped data available inside telefunctions via getContext():", "The context parameter is optional — only needed if you use getContext().", "For Express, Fastify, or any Node.js framework, pass the req readable stream:", "httpResponse contains everything needed to send the response:", and the field-description cells) — Clarity 9–10/10; Naturalness 9–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 4 | "It runs in **every** runtime without **adapter**." → "It runs in **any** runtime without **an adapter**." | Clarity 9, Naturalness 9 — adds the article; "any runtime" is the idiomatic claim. |

**Tally:** 23 reviewed · 1 edited · 0 deferred.

---

# 8. `docs/pages/Telefunc/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: server." — Clarity 10/10; Naturalness 10/10. KEEP
2. "Use new Telefunc() to embed Telefunc into your server (Hono, Cloudflare, Express, Fastify, …)." — Clarity 9/10 ("embed … into" slightly loose); Naturalness 9/10. KEEP
3. "Lower-level alternative: …" — Clarity 9/10; Naturalness 9/10. KEEP
4–9. Setup prose ("With the web standard Request/Response objects (Hono, Fastify, …):", "With the Node.js req/res objects (Express.js):", "**Performance tip**: prefer { req, res } over { request } when you have access to the raw Node.js objects…", "The req/res path reads the request body directly from the Node Readable and writes the response straight to the Node Writable, skipping the Web Streams conversion layer.", "Node's Web Streams implementation is slower than its internal streams; see [nodejs/performance#134].") — Clarity 9/10; Naturalness 9/10. KEEP
10. "On Cloudflare the context is set in a separate function than the request handler. (Because it runs in a Durable Object, whereas the request handler doesn't.)" — Clarity 8/10; Naturalness 6/10 ("separate function **than**" is ungrammatical; should be "from"). **EDITED**
11–13. Cloudflare config prose ("On Cloudflare new Telefunc also takes bindingName, kvBindingName, scale, locationFallback, jurisdiction, and more — see …", "Add the Durable Object and KV bindings to your wrangler.jsonc:", "See Cloudflare Workers for scaling, distributed broadcast, delivery guarantees, and Durable Object configuration.") — Clarity 9/10; Naturalness 9/10. KEEP
14–18. Methods table ("Process a request and return the response…", "Enable WebSocket channels on your HTTP server. Idempotent…", "The WebSocket handler — pass it to Bun.serve({ websocket }).", "The Durable Object class to export — see …") — Clarity 8–10/10; Naturalness 8–9/10. KEEP
19–24. Return section + "See also" ("telefunc.serve() returns:", the two return-table rows, "telefunc.serve() returns a falsy value (undefined or false) for non-Telefunc requests, allowing you to chain it with other handlers.") — Clarity 9–10/10; Naturalness 9–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 10 | "…set in a separate function **than** the request handler." → "…set in a separate function **from** the request handler." | Clarity 9, Naturalness 9 — "separate … from" is the correct idiom. |

**Tally:** 24 reviewed · 1 edited · 0 deferred.

---

# 9. `docs/pages/provideTelefuncContext/+Page.mdx`

#### Sentence ratings

1. "**Environment**: server." — Clarity 10/10; Naturalness 10/10. KEEP
2. "provideTelefuncContext() makes the context object available to telefunctions via getContext() — useful when a telefunction runs **outside telefunc.serve()**, such as server-side rendering or unit tests." — Clarity 8/10 ("such as server-side rendering or unit tests" attaches grammatically to "runs outside serve()" but means example scenarios — slightly loose); Naturalness 9/10. KEEP (meaning recoverable; edit risk to the inline `<Link>` not worth the marginal gain)
3. "Call it before the telefunction runs (e.g. in your test setup or your SSR request handler)." — Clarity 9/10; Naturalness 9/10. KEEP
4. "With serve() / new Telefunc(), the context is provided for you — see …" — Clarity 9/10; Naturalness 9/10. KEEP
5. "Reach for provideTelefuncContext() only when a telefunction runs outside both." — Clarity 8/10 ("outside both" refers back to serve() and new Telefunc() — requires holding the prior clause in mind); Naturalness 9/10. KEEP
6. "## See also" — Clarity 10/10; Naturalness 10/10. KEEP

#### Edits applied
None — all sentences ≥8 on both axes.

**Tally:** 6 reviewed · 0 edited · 0 deferred.

---

# 10. `docs/pages/testing/+Page.mdx`

#### Sentence ratings

1. "**Environment**: server." — Clarity 10/10; Naturalness 10/10. KEEP
2. "Telefunctions are plain functions — unit-test them by importing and calling them directly." — Clarity 10/10; Naturalness 10/10. KEEP
3. "No server, no HTTP, no mocking." — Clarity 10/10; Naturalness 9/10 (intentional punchy fragment). KEEP
4. "## Providing context" — Clarity 10/10; Naturalness 10/10. KEEP
5. "If a telefunction reads getContext() (e.g. to get the logged-in user or request headers), provide the context in your test setup with … before calling it:" — Clarity 9/10; Naturalness 8/10 (long but flows). KEEP
6. "This is also how you test /permissions — set up a context that should (or shouldn't) pass, then assert the telefunction returns or throws Abort() accordingly." — Clarity 9/10; Naturalness 9/10. KEEP
7. "## Channels & the wire protocol" — Clarity 10/10; Naturalness 10/10. KEEP
8. "A telefunction that opens a Channel or BroadcastChannel is still a plain function — call it directly to assert it authorizes correctly and wires up its listeners." — Clarity 8/10 ("wires up its listeners" is slightly vague); Naturalness 9/10. KEEP
9. "The wire protocol itself, though — reconnection, multi-client broadcast, transport upgrades — only exists over a real connection, so cover it with an **end-to-end test** against a running server." — Clarity 8/10 (mid-sentence em-dash list + leading "though" add slight friction); Naturalness 8/10. KEEP
10. "## See also" — Clarity 10/10; Naturalness 10/10. KEEP

#### Edits applied
None — all sentences ≥8 on both axes.

**Tally:** 10 reviewed · 0 edited · 0 deferred.

---

# 11. `docs/pages/file-download/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "You can return a File or Blob from a telefunction like any other value." — Clarity 9/10 ("like any other value" trailing modifier slightly loose); Naturalness 10/10. KEEP
2. "A single file, multiple files, or files mixed with other return values — all are supported." — Clarity 9/10; Naturalness 9/10. KEEP
3. "The client receives a standard File / Blob ready for URL.createObjectURL, <img src>, fetch({ body }), FormData, etc." — Clarity 9/10; Naturalness 9/10. KEEP
4. "There are multiple streaming and reading strategies, see:" — Clarity 9/10; Naturalness 8/10 (comma splice before "see:"). KEEP
5–10. Example headings + intros ("Return a native File or Blob like any other value.", "Calling dl.cancel() aborts the download — the pending saveToMemory call then rejects.", "Return any number of File, Blob, or download() values anywhere in the response — in arrays, in nested objects, or mixed with regular data.") — Clarity 9–10/10; Naturalness 9–10/10. KEEP
11. "Each one independently exposes its own onProgress / cancel / saveTo* methods on the client." — Clarity 7/10 (redundant "independently" + "its own" muddies whether independence is per-method or per-download); Naturalness 7/10 (doubled redundancy). **EDITED**
12–45. The passthrough, streaming-strategies, reading-strategies, `saveToDisk({ mode })`, error-handling, and limitations sections (incl. the decision tables, "Wrap a ReadableStream with download() to stream bytes from an upstream source through your server without buffering.", "For large files, prefer dl.stream() / dl.saveToDisk() / dl.saveToOpfs() — memory stays constant regardless of file size.", "The client receives the File / Blob as a **lazy, streaming** object…", "The streaming download can only be consumed once — calling .stream(), .bytes(), .text(), .arrayBuffer(), .slice(), or any saveTo* method a second time throws.", "If you need the data more than once, materialize it once via dl.saveToMemory() … and reuse the returned File / Blob.") — Clarity 8–10/10; Naturalness 8–10/10. KEEP. (The commented-out `{/* … */}` TODO block at lines 231–275 is non-rendered and excluded.)

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 11 | "Each one **independently** exposes **its own** onProgress / cancel / saveTo* methods on the client." → "Each one exposes its own onProgress / cancel / saveTo* methods on the client, **independently of the others**." | Clarity 9, Naturalness 9 — removes the "independently … its own" redundancy; makes the independence relation explicit. |

**Tally:** 45 reviewed · 1 edited · 0 deferred.

---

# 12. `docs/pages/tanstack-query/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "Live queries for TanStack Query — invalidate a query key on the server, and every connected client with a matching query refetches automatically." — Clarity 9/10 (dense but unambiguous); Naturalness 9/10. KEEP
2–5. Setup prose ("Wrap your QueryClient with withTelefunc():", "withTelefunc() returns the same new QueryClient() instance:", "All options and APIs continue to work as before", "Any TanStack Query adapter works: React, Vue, Svelte, Solid") — Clarity 9–10/10; Naturalness 9–10/10. KEEP
6. "There's no server-side setup: Telefunc finds @telefunc/* packages in your package.json and auto-loads it on the server-side." — Clarity 6/10 (pronoun "it" disagrees with plural "packages"; "server-side" used as a noun); Naturalness 6/10. **EDITED**
7–14. Local-vs-global key explanation ("…keys prefixed with global: are special because they invalidate globally…", "All other keys remain normal (invalidate locally, i.e. current tab only).", "**How does it work?** Under the hood, the global: key is sent to the server then broadcasted to the clients that use a query key that matches.", "A key is global when its *first* element is a string starting with global:, for example:", "Use meta.invalidates on mutations to invalidate matching queries after the mutation succeeds.", "meta.invalidates is a @telefunc/tanstack-query convention: TanStack Query itself attaches no behavior to meta.") — Clarity 8–10/10; Naturalness 8–10/10. KEEP
15. "When using a global key, every connected clients refetches." — Clarity 8/10; Naturalness 5/10 (subject–verb disagreement "every connected clients refetches"). **EDITED**
16. "Local and global keys can be mixed in a single mutation:" — Clarity 10/10; Naturalness 10/10. KEEP
17. "For example an online collaborative document editor: when a collaborator edits a document, every client viewing it refetches:" — Clarity 7/10 (verbless fragment "For example an online … editor:"); Naturalness 6/10 (missing comma after "For example"). **EDITED**
18–35. The remaining "Make sure that:" rules, the `invalidate()` section, and the "How it works" walkthrough ("**queryFn and mutationFn must call a telefunction.**", "**Return the telefunction call directly.** Transform the result with select instead of inside queryFn:", "For changes not triggered by a client mutation (e.g. background jobs, webhooks), you can use invalidate():", "invalidate() only accepts global keys.", "Invalidation is prefix-based: invalidating ['global:documents'] matches ['global:documents', docId] too.", "Global keys are powered by Telefunc Stream under the hood (in particular channels and broadcasts).", "The server broadcasts an invalidation event to the clients that subscribe to a query key that matches a global key.") — Clarity 8–10/10; Naturalness 8–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 6 | "…and auto-loads **it** on the **server-side**." → "…and auto-loads **them** on the **server side**." | Clarity 9, Naturalness 9 — plural agreement; adverbial "server side". |
| 15 | "…every connected **clients refetches**." → "…every connected **client refetches**." | Clarity 9, Naturalness 9 — subject–verb agreement. |
| 17 | "**For example an online collaborative document editor:** when a collaborator edits a document…" → "**Consider a collaborative document editor:** when one user edits a document…" | Clarity 9, Naturalness 9 — proper imperative sentence; "one user" reads more naturally; meaning preserved. |

**Tally:** 35 reviewed · 3 edited · 0 deferred.

---

# 13. `docs/pages/rxjs/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "The @telefunc/rxjs integration lets you pass RxJS Observable and Subject instances directly between client and server, in both directions and with all operators." — Clarity 9/10; Naturalness 9/10. KEEP
2. "**Automatic type runtime validation**: all value sent from the client to the server are validated against TypeScript types at runtime (no need for Zod)." — Clarity 9/10; Naturalness 5/10 (agreement error "all value sent … are validated"; garbled heading "type runtime validation"). **EDITED**
3–28. The remaining prose (intro, examples, and explanatory callouts) — Clarity 8–10/10; Naturalness 8–10/10. KEEP. All other sentences rated 8–10 on both axes and were kept byte-identical.

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 2 | "**Automatic type runtime validation**: **all value sent** from the client to the server **are** validated against TypeScript types…" → "**Automatic runtime type validation**: **all values sent** from the client to the server are validated against **your** TypeScript types…" | Clarity 9, Naturalness 9 — fixes plural agreement ("all values"), reorders the garbled heading to "runtime type validation", adds "your". |

**Tally:** 28 reviewed · 1 edited · 0 deferred.

---

# 14. `docs/pages/redis/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "Redis-backed broadcast fan-out across server instances — publish() on one server instance reaches subscribe() on all other server instances." — Clarity 9/10; Naturalness 9/10. KEEP
2. "You only need this if you scale horizontally, see /stream/scale." — Clarity 9/10; Naturalness 7/10 (comma splice "if you scale horizontally, see …"). **EDITED**
3–15. The install/config prose — Clarity 8–10/10; Naturalness 8–10/10. KEEP. All other sentences rated 8–10 on both axes.

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 2 | "You only need this if you scale horizontally**,** see /stream/scale." → "You only need this if you scale horizontally **—** see /stream/scale." | Clarity 9, Naturalness 9 — replaces the comma splice with the em dash used by the other "— see" callouts in these docs. |

**Tally:** 15 reviewed · 1 edited · 0 deferred.

---

# 15. `docs/pages/stream/scale/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "If you use Telefunc Stream, scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements:" — Clarity 9/10; Naturalness 9/10. KEEP
2. "**Sticky sessions** (so a reconnecting client returns to the same server instance) — your load balancer must route every request from a given client to the same instance." — Clarity 9/10; Naturalness 9/10. KEEP
3. "Required: every stream requires this." — Clarity 8/10 ("Required" then "requires" is mildly redundant); Naturalness 8/10. KEEP
4. "**Cross-instance broadcast transport** (a broadcast must be delivered across all server instances) — install one like @telefunc/redis." — Clarity 7/10 ("delivered across all server instances" is vague about who receives it); Naturalness 7/10 ("install one like" is colloquial). **EDITED**
5–22. "Optional: this is required only for streams that broadcast." + the sticky-sessions deep-dive ("A Telefunc Channel is a stateful connection.", "Its server-side state … lives in one server process.", "When the client reconnects …, the next request has to land on the same process or the channel can't recover.", "The same load-balancer feature solves the problem for Telefunc: a sticky session, usually backed by a cookie or by the client IP.", the AWS target-group instructions, "Serverless platforms that don't expose sticky-session routing aren't a good fit for Channel.", "Broadcasts still work (publishes round-trip through Redis), but channel reconnects can fail unpredictably.", "Most teams pair Telefunc with a regular long-running server tier when they need channels at scale.", "Broadcast is different: publishers and subscribers are intentionally decoupled — they only share a string key.", "In a single-instance setup the default in-memory transport is enough.", "In a multi-instance setup, install a transport that fans out across the cluster — such as @telefunc/redis.", the NATS/Kafka/RabbitMQ note) — Clarity 8–10/10; Naturalness 8–10/10. KEEP
23. "For Cloudflare Workers, Telefunc has an apadter that uses Durable Objects — see …" — Clarity 6/10 (typo "apadter" forces a re-read); Naturalness 6/10 (misspelling). **EDITED**
24–31. The BroadcastTransport interface notes + "Cloudflare Workers is fundamentally different…" + "As a result, neither a sticky load balancer nor a broadcast transport is needed." + "You scale with the scale option instead of a load balancer." — Clarity 8–10/10; Naturalness 9–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 10 | "(a broadcast must **be delivered across all server instances**) — install one **like** @telefunc/redis." → "(a broadcast must **reach subscribers on every server instance**) — install one **such as** @telefunc/redis." | Clarity 9, Naturalness 9 — names who receives the broadcast; "such as" is more formal than "like". |
| 70 | "Telefunc has an **apadter** that uses Durable Objects" → "Telefunc has an **adapter** that uses Durable Objects" | Clarity 9, Naturalness 9 — fixes the typo. |

**Tally:** 31 reviewed · 2 edited · 0 deferred.

---

# 16. `docs/pages/stream/cloudflare/+Page.mdx`

#### Sentence ratings (highlights; full set reviewed)

1. "**Environment**: server." — Clarity 10/10; Naturalness 10/10. KEEP
2. "Cloudflare-specific setup for Telefunc Stream." — Clarity 9/10; Naturalness 9/10 (terse subtitle). KEEP
3–7. Setup intro ("Fundamentally, channels are stateful: the server holds live state for each open channel.", "Cloudflare Workers, however, are stateless and ephemeral — any request can be served by any worker, and nothing is remembered between requests.", "The state therefore needs to live somewhere else — Durable Objects and KV are Cloudflare's primitives for exactly that:", "**Durable Objects** provide stateful compute: each channel lives on a Durable Object, which holds the connection and its state.") — Clarity 9/10; Naturalness 9/10. KEEP
8. "**KV** provides shared storage: it's how stateless workers find the Durable Object that holds the state, wherever a request lands." — Clarity 7/10 ("wherever a request lands" is vague/elliptical — lands where?); Naturalness 8/10. **EDITED**
9–73. The rest of the page — Setup, Context, Architecture (Regions, Session affinity), Scaling, Distributed broadcast (How it works, Ordering, Presence), Delivery guarantees, Configuration, Hibernation, See also. Representative ratings: "Both bindings are required whenever you use telefunc/cloudflare." (10/10, 10/10), "Every telefunction call and channel message from a browser must reach the **same Durable Object** — otherwise that browser's channel state would be unavailable." (9/9), "On the first request, Telefunc picks a Durable Object in the nearest region, stores a session token in KV (TTL: 24 hours), and returns that token to the client via the x-telefunc-session header." (9/9), "Binary frames have a separate budget and can never evict text." (8/8 — "evict text" is jargon-y but clear in context), "**Hibernation while channels are open isn't supported.**" (9/9), "The Durable Object can hibernate once all channels are closed, no clients remain connected, and both the reconnect and idle windows have expired." (9/9). All Clarity 8–10/10; Naturalness 8–10/10. KEEP

#### Edits applied

| # | Before → After | New rating |
|---|----------------|-----------|
| 8 | "it's how stateless workers find the Durable Object that holds the state, **wherever a request lands**." → "it's how stateless workers find the Durable Object that holds the state, **no matter which worker a request lands on**." | Clarity 9, Naturalness 9 — names the destination (the worker), removing the ambiguity of "lands". |

**Tally:** 73 reviewed · 1 edited · 0 deferred.

---

# 17. `docs/pages/channel-config/+Page.mdx`

#### Sentence ratings

1. "**Environment**: server." — Clarity 10/10; Naturalness 9/10. KEEP
2. "This is the **server-side** config.channel — a separate object from the **client-side** config.channel.transports (telefunc/client)." — Clarity 9/10; Naturalness 9/10. KEEP
3. "**What to tune**: the defaults suit typical stream use cases; tune these only if you run into one of the issues below:" — Clarity 9/10; Naturalness 9/10. KEEP
4. "Slow/flaky clients dropping with NetworkError (isChannel: true) → raise reconnectTimeout (how long the server holds a channel open while a client is gone)." — Clarity 9/10; Naturalness 8/10 ("Slow/flaky" slash compound is informal but fits a terse troubleshooting bullet). KEEP
5. "ChannelOverflowError while a peer is briefly offline → raise bufferLimit/bufferLimitBinary, or apply backpressure by await-ing your send()s." — Clarity 9/10; Naturalness 9/10. KEEP
6. "Want reconnects to replay more history → raise the replay buffers; lower them to cap memory." — Clarity 9/10; Naturalness 9/10. KEEP

#### Edits applied
None — all sentences ≥8 on both axes.

**Tally:** 6 reviewed · 0 edited · 0 deferred.

---

# 18. `docs/components/NeedsLongRunningServer.mdx`

#### Sentence ratings

1. "**Needs a long-running server.** A channel holds a connection open for its entire lifetime, so channels and broadcasts don't work on most serverless platforms (e.g. Vercel, AWS Lambda), which terminate a connection after a short time limit." — Clarity 9/10; Naturalness 9/10. KEEP
2. "See for running multiple instances, or for Cloudflare's Durable Objects." — Clarity 9/10; Naturalness 9/10. KEEP

#### Edits applied
None — all sentences ≥8 on both axes.

**Tally:** 2 reviewed · 0 edited · 0 deferred.

---

# Why so few 10/10 ratings

Almost every kept sentence sits at **8–9, not 10**. A 10 was awarded only to sentences that are
genuinely impossible to improve on either axis (typically very short, unambiguous labels and section
headings such as "**Environment**: server.", "Messages are delivered in order.", "## See also"). The
recurring reasons a sentence stops short of 10:

- **Trailing/loose modifiers** — "like any other value", "as before", "ready for", which technically
  parse but momentarily leave the scope open.
- **Compact anaphora** — "its end", "outside both", "Same +", which force the reader to hold the prior
  clause in mind.
- **Telegraphic callout fragments** — "Proxy buffers binary", "*all* stream closes" — idiomatic for a
  table cell but not full sentences.
- **Mild jargon** — "evict text", "wires up its listeners", "walked recursively" — correct, but a
  half-step above plain language.
- **Stacked clauses / em-dash density** — double parentheticals and "that … that" chains that are
  readable but dense.

None of those reach the ≤7 "must edit" bar; they are the honest difference between *good* and *flawless*.

# Edits not made (deliberately kept)

A handful of low-but-defensible sentences were left as-is because editing carried more risk than reward
(touching an inline `<Link>`, or breaking a deliberate parallel structure): the
`provideTelefuncContext` "such as server-side rendering or unit tests" modifier, the `onClose`
"*the one* channel closes" table cell (a deliberate parallel with "*all* stream closes"), and the
`serve` parenthetical fragments. Each is rated 8 on at least one axis and is clear in context.

# Second PR

The brief reserves a *second PR* for any edit whose **best** wording still rates ≤7. **No edit met that
condition** — every one of the 29 applied edits re-rated ≥8 on both clarity and naturalness — so this
review ships as a single PR with no deferred changes.
