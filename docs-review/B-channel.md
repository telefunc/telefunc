# Sentence review — `docs/pages/channel/+Page.mdx`

Reviewer B. New file (whole-file prose review). PR #264 (`feat: stream`).
Methodology: `/home/user/telefunc/docs-review/METHODOLOGY.md`.

Notes on scope: rated every introduced prose unit — paragraphs, list items, blockquote/callout
sentences, `<Link>` descriptions, and prose table cells. Skipped: `import` lines, frontmatter,
bare component tags, anchor slugs, URLs, and pure-syntax/pure-code table cells (e.g. `send()` /
`listen()`, `publish()` / `subscribe()`). Code is not rated, but code comments are scanned for
typos/clarity.

---

### [1] `docs/pages/channel/+Page.mdx` — intro, environment label (line 6)
- **Original:** "**Environment**: server."
- **Clarity:** 10/10
- **Naturalness:** 9/10 — A label fragment, not a full sentence; standard in these docs, but minimal.
- **Overall:** 9/10
- **Action:** Kept

### [2] `docs/pages/channel/+Page.mdx` — intro lead-in (line 8)
- **Original:** "Telefunc has two real-time **primitives** — and one **composition** of them:"
- **Clarity:** 9/10 — "composition of them" is slightly abstract until the bullets explain it, but the colon resolves it.
- **Naturalness:** 9/10 — Crisp; the em-dash split is idiomatic Telefunc voice.
- **Overall:** 9/10
- **Action:** Kept

### [3] `docs/pages/channel/+Page.mdx` — primitives bullet 1 (line 10)
- **Original:** "**`new Channel()`** — a private two-way pipe between the server and *one* client."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [4] `docs/pages/channel/+Page.mdx` — primitives bullet 2 (line 11)
- **Original:** "**`Broadcast`** — a keyed pub/sub **bus**: `publish()` / `subscribe()` by string `key`, server-side."
- **Clarity:** 9/10 — Dense (three concepts: keyed, pub/sub, bus), but each is reinforced later; reader can parse it.
- **Naturalness:** 9/10 — Telegraphic but fits a definition-list bullet.
- **Overall:** 9/10
- **Action:** Kept

### [5] `docs/pages/channel/+Page.mdx` — primitives bullet 3 (line 12)
- **Original:** "**`new BroadcastChannel()`** — a `Channel` bridged onto a `Broadcast` key; the two primitives combined."
- **Clarity:** 9/10 — "bridged onto a key" is jargon, but the trailing clause clarifies the intent.
- **Naturalness:** 9/10 — Reads well; consistent with the other two bullets.
- **Overall:** 9/10
- **Action:** Kept

### [6] `docs/pages/channel/+Page.mdx` — return semantics (line 14, sentence 1)
- **Original:** "`new Channel()` and `new BroadcastChannel()` are returned from a telefunction — they serialize into client-side objects automatically."
- **Clarity:** 9/10 — Clear; "serialize into client-side objects" is precise for the audience.
- **Naturalness:** 9/10 — Slightly long subject list, but normal API prose.
- **Overall:** 9/10
- **Action:** Kept

### [7] `docs/pages/channel/+Page.mdx` — return semantics (line 14, sentence 2)
- **Original:** "`Broadcast` is a server-side API, so there's nothing to return."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [8] `docs/pages/channel/+Page.mdx` — transport callout (line 16, sentence 1)
- **Original:** "By default, channels and broadcasts use **SSE** and work without extra server setup."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [9] `docs/pages/channel/+Page.mdx` — transport callout (line 16, sentence 2)
- **Original:** "When WebSocket is enabled (see <Link href="/server" />), the client starts on SSE and seamlessly upgrades to WebSocket in the background (see <Link href="/transport" />)."
- **Clarity:** 9/10 — Clear; the two parentheticals are a little heavy but each is useful.
- **Naturalness:** 9/10 — "seamlessly" is mild marketing fluff, otherwise solid technical prose.
- **Overall:** 9/10
- **Action:** Kept

### [10] `docs/pages/channel/+Page.mdx` — short-lived callbacks callout (line 20)
- **Original:** "For short-lived callbacks (e.g. progress updates), <Link href="/stream#function-passing">function passing</Link> is usually simpler."
- **Clarity:** 9/10 — "short-lived callbacks" is a touch abstract, but the example clarifies it.
- **Naturalness:** 10/10 — Natural, idiomatic recommendation phrasing.
- **Overall:** 9/10
- **Action:** Kept

### [11] `docs/pages/channel/+Page.mdx` — `new Channel()` intro (line 25, sentence 1)
- **Original:** "`new Channel()` creates a private, two-way message pipe between the server and the one client that called the telefunction."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [12] `docs/pages/channel/+Page.mdx` — `new Channel()` intro (line 25, sentence 2)
- **Original:** "The server keeps the `Channel` object and hands the client its end by returning the channel's `.client`."
- **Clarity:** 9/10 — "hands the client its end" relies on the pipe metaphor; clear in context.
- **Naturalness:** 10/10 — Reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [13] `docs/pages/channel/+Page.mdx` — `new Channel()` intro (line 25, sentence 3)
- **Original:** "The channel outlives the telefunction call: both ends can `send()` and `listen()` until one side closes it."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [14] `docs/pages/channel/+Page.mdx` — simplest-case lead-in (line 27)
- **Original:** "The simplest case — the server pushes live updates to the one client that opened the channel:"
- **Clarity:** 9/10 — Clear lead-in to the code block.
- **Naturalness:** 9/10 — Fragment-as-caption; standard for code intros, slightly terse.
- **Overall:** 9/10
- **Action:** Kept

### [15] `docs/pages/channel/+Page.mdx` — section roadmap (line 53)
- **Original:** "The rest of this section adds types, two-way messaging, acks, and binary data; see <Link href="#channel-methods" /> for the full API."
- **Clarity:** 9/10 — Clear; "acks" is an abbreviation but the next section spells out "Acknowledgements".
- **Naturalness:** 9/10 — Natural roadmap sentence.
- **Overall:** 9/10
- **Action:** Kept

### [16] `docs/pages/channel/+Page.mdx` — Channel methods table, `send(data)` (line 59)
- **Original:** "Send a message. Await to apply backpressure."
- **Clarity:** 9/10 — "Await to apply backpressure" is terse but accurate; the Backpressure section expands it.
- **Naturalness:** 9/10 — Imperative table-cell style, fits.
- **Overall:** 9/10
- **Action:** Kept

### [17] `docs/pages/channel/+Page.mdx` — Channel methods table, `send(data, { ack: true })` (line 60)
- **Original:** "Send and await acknowledgement."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [18] `docs/pages/channel/+Page.mdx` — Channel methods table, `sendBinary(data)` (line 61)
- **Original:** "Send binary data. Await to apply backpressure."
- **Clarity:** 9/10 — Same terse "Await to apply backpressure" as [16]; clear enough.
- **Naturalness:** 9/10 — Consistent table style.
- **Overall:** 9/10
- **Action:** Kept

### [19] `docs/pages/channel/+Page.mdx` — Channel methods table, `sendBinary(data, { ack: true })` (line 62)
- **Original:** "Send binary and await acknowledgement."
- **Clarity:** 9/10 — "Send binary" elides "data"; clear from the method name.
- **Naturalness:** 9/10 — Consistent with [17].
- **Overall:** 9/10
- **Action:** Kept

### [20] `docs/pages/channel/+Page.mdx` — Channel methods table, `listen(cb)` (line 63)
- **Original:** "Receive messages. Return a value to ack. Returns an unlisten function."
- **Clarity:** 9/10 — Three crisp statements; "Return a value to ack" mixes imperative + verb-as-noun but is idiomatic here.
- **Naturalness:** 9/10 — Slight verb-tense shift (imperative "Return" then declarative "Returns"); acceptable in a cell.
- **Overall:** 9/10
- **Action:** Kept

### [21] `docs/pages/channel/+Page.mdx` — Channel methods table, `listenBinary(cb)` (line 64)
- **Original:** "Receive binary. Return a value to ack. Returns an unlisten function."
- **Clarity:** 9/10 — "Receive binary" elides "data"; clear from the method name.
- **Naturalness:** 9/10 — Same mixed tense as [20]; consistent.
- **Overall:** 9/10
- **Action:** Kept

### [22] `docs/pages/channel/+Page.mdx` — Channel methods table, lifecycle (line 65)
- **Original:** "Lifecycle callbacks."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [23] `docs/pages/channel/+Page.mdx` — Channel methods table, `close()` (line 66)
- **Original:** "Close gracefully."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [24] `docs/pages/channel/+Page.mdx` — Channel methods table, `abort(value?)` (line 67)
- **Original:** "Terminate immediately."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [25] `docs/pages/channel/+Page.mdx` — after methods table (line 69, sentence 1)
- **Original:** "Server: return the channel's `.client` from the telefunction."
- **Clarity:** 10/10
- **Naturalness:** 9/10 — "Server:" label-prefix fragment; common in these docs, slightly terse.
- **Overall:** 9/10
- **Action:** Kept

### [26] `docs/pages/channel/+Page.mdx` — after methods table (line 69, sentence 2)
- **Original:** "The client gets the same API with message directions flipped."
- **Clarity:** 9/10 — "message directions flipped" is concise jargon; the TypeScript section makes it concrete.
- **Naturalness:** 9/10 — Reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [27] `docs/pages/channel/+Page.mdx` — TypeScript intro (line 74, sentence 1)
- **Original:** "`new Channel<ClientToServer, ServerToClient>()` takes two generic type parameters."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [28] `docs/pages/channel/+Page.mdx` — TypeScript intro (line 74, sentence 2)
- **Original:** "Each is a function signature:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [29] `docs/pages/channel/+Page.mdx` — TypeScript bullet (line 75)
- **Original:** "The **argument** is the message type."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [30] `docs/pages/channel/+Page.mdx` — TypeScript bullet (line 76)
- **Original:** "The **return type** is the acknowledgement type (`void` if no ack)."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [31] `docs/pages/channel/+Page.mdx` — after TS example (line 129, sentence 1)
- **Original:** "On the server, `send()` sends `ServerMessage`."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [32] `docs/pages/channel/+Page.mdx` — after TS example (line 129, sentence 2)
- **Original:** "On the client, `send()` sends `ClientMessage`."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [33] `docs/pages/channel/+Page.mdx` — after TS example (line 129, sentence 3)
- **Original:** "The `chat.client` type flips the message types."
- **Clarity:** 9/10 — "flips the message types" reuses the earlier metaphor; clear here.
- **Naturalness:** 9/10 — Natural, concise.
- **Overall:** 9/10
- **Action:** Kept

### [34] `docs/pages/channel/+Page.mdx` — validation note (line 131, sentence 1)
- **Original:** "Both generic parameters also drive runtime validation: Telefunc auto-generates shields that check every message and ack arriving at the server against its declared type."
- **Clarity:** 8/10 — "drive runtime validation" is slightly abstract; "its declared type" — antecedent ("each message and ack") is recoverable but the singular "its" after a plural pairing is mildly loose.
- **Naturalness:** 9/10 — Reads well; "auto-generates shields" is established Telefunc terminology.
- **Overall:** 8/10
- **Action:** Kept

### [35] `docs/pages/channel/+Page.mdx` — validation note (line 131, sentence 2)
- **Original:** "See <Link href="/shield" />."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [36] `docs/pages/channel/+Page.mdx` — Backpressure intro (line 186)
- **Original:** "Both `send()` and `sendBinary()` return a `Promise` that resolves when the receiver has capacity for more data."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [37] `docs/pages/channel/+Page.mdx` — Backpressure loop lead-in (line 186, continued)
- **Original:** "Await them in a loop to apply [backpressure](https://en.wikipedia.org/wiki/Backpressure_routing):"
- **Clarity:** 9/10 — Clear; the linked term carries the definition.
- **Naturalness:** 9/10 — Natural instruction.
- **Overall:** 9/10
- **Action:** Kept

### [38] `docs/pages/channel/+Page.mdx` — fire-and-forget lead-in (line 194)
- **Original:** "Fire-and-forget is also fine — data is always sent immediately regardless of whether you await:"
- **Clarity:** 9/10 — "is also fine" is mildly colloquial but unambiguous.
- **Naturalness:** 9/10 — Reads naturally; "Fire-and-forget" is standard async terminology.
- **Overall:** 9/10
- **Action:** Kept

### [39] `docs/pages/channel/+Page.mdx` — `Broadcast` intro (line 203, sentence 1)
- **Original:** "`Broadcast` is a keyed pub/sub **bus**: a message published to a `key` reaches every subscriber of that `key` — server-side subscribers (via `Broadcast.subscribe()`) and clients bridged into the key (via a `BroadcastChannel`)."
- **Clarity:** 8/10 — Long but well-structured; the colon + em-dash carry a lot. One nit: "the key" (no backticks) vs `key` elsewhere is mildly inconsistent.
- **Naturalness:** 9/10 — Dense but reads as competent reference prose.
- **Overall:** 8/10
- **Action:** Kept

### [40] `docs/pages/channel/+Page.mdx` — `Broadcast` intro (line 203, sentence 2)
- **Original:** "Publishers and subscribers are decoupled; all they share is the string `key`."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [41] `docs/pages/channel/+Page.mdx` — `Broadcast` intro (line 203, sentence 3)
- **Original:** "It's the fan-out layer that `BroadcastChannel` (below) is built on."
- **Clarity:** 9/10 — "fan-out layer" is precise jargon for the audience.
- **Naturalness:** 9/10 — Natural; ending on "built on" is fine.
- **Overall:** 9/10
- **Action:** Kept

### [42] `docs/pages/channel/+Page.mdx` — static methods note (line 205)
- **Original:** "The static methods run purely on the server — no client, no handle, no lifecycle — and take the `key` as their first argument:"
- **Clarity:** 9/10 — Clear; the triple "no ..." is emphatic and unambiguous.
- **Naturalness:** 9/10 — Idiomatic Telefunc rhythm.
- **Overall:** 9/10
- **Action:** Kept

### [43] `docs/pages/channel/+Page.mdx` — receipt note (line 219)
- **Original:** "`publish()` returns a receipt and `subscribe()` receives the same `info`:"
- **Clarity:** 9/10 — Clear; "receipt" and "info" are tied together by "the same".
- **Naturalness:** 9/10 — Natural lead-in to the code block.
- **Overall:** 9/10
- **Action:** Kept

### [44] `docs/pages/channel/+Page.mdx` — seq note (line 227, sentence 1)
- **Original:** "`seq` is monotonic per `key`, useful for ordering and gap detection."
- **Clarity:** 9/10 — Clear; "gap detection" is concise but understandable for the audience.
- **Naturalness:** 9/10 — Reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [45] `docs/pages/channel/+Page.mdx` — seq note (line 227, sentence 2)
- **Original:** "`publish()` resolves to `info` once the message is accepted."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [46] `docs/pages/channel/+Page.mdx` — multi-server callout (line 229, sentence 1)
- **Original:** "In-memory broadcast works out-of-the-box for single-server deployments."
- **Clarity:** 10/10
- **Naturalness:** 9/10 — "out-of-the-box" is informal but widely used in docs.
- **Overall:** 9/10
- **Action:** Kept

### [47] `docs/pages/channel/+Page.mdx` — multi-server callout (line 229, sentence 2)
- **Original:** "For multi-server, set `config.broadcast.transport` — see <Link text="Multi-server" href="#multi-server" />."
- **Clarity:** 10/10
- **Naturalness:** 9/10 — "For multi-server," elides "deployments"; terse but clear.
- **Overall:** 9/10
- **Action:** Kept

### [48] `docs/pages/channel/+Page.mdx` — multi-server callout (line 229, sentence 3)
- **Original:** "On Cloudflare Workers, Telefunc uses Durable Objects automatically — see <Link href="/stream/cloudflare" />."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [49] `docs/pages/channel/+Page.mdx` — `new BroadcastChannel()` intro (line 234, sentence 1)
- **Original:** "`new BroadcastChannel({ key })` is the composition: a `Channel` to the one client that received it, bridged onto a `Broadcast` key."
- **Clarity:** 9/10 — "is the composition" leans on the intro's "composition" framing; clear in flow.
- **Naturalness:** 9/10 — Natural; the appositive after the colon is clean.
- **Overall:** 9/10
- **Action:** Kept

### [50] `docs/pages/channel/+Page.mdx` — `new BroadcastChannel()` intro (line 234, sentence 2)
- **Original:** "Return it from a telefunction and that client can `publish()` / `subscribe()` on the key — every message reaches every member of the group, including the publisher itself."
- **Clarity:** 9/10 — Clear; "the publisher itself" usefully calls out self-delivery.
- **Naturalness:** 9/10 — Reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [51] `docs/pages/channel/+Page.mdx` — `new BroadcastChannel()` intro (line 234, sentence 3)
- **Original:** "No `.client` needed: broadcast is symmetric (one message type, no directional flip), so the same instance works on both ends — unlike channels."
- **Clarity:** 9/10 — Dense but each clause lands; "directional flip" ties back to earlier wording.
- **Naturalness:** 9/10 — Idiomatic, em-dash contrast is on-voice.
- **Overall:** 9/10
- **Action:** Kept

### [52] `docs/pages/channel/+Page.mdx` — Warning, capabilities (line 238, sentence 1)
- **Original:** "**Keys are capabilities** — anyone who knows the `key` joins the group."
- **Clarity:** 9/10 — "Keys are capabilities" is a security term; the em-dash gloss explains it.
- **Naturalness:** 9/10 — Strong, on-voice warning.
- **Overall:** 9/10
- **Action:** Kept

### [53] `docs/pages/channel/+Page.mdx` — Warning, capabilities (line 238, sentence 2)
- **Original:** "Secure a broadcast in one of two ways:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [54] `docs/pages/channel/+Page.mdx` — Warning bullet, guard the key (line 239)
- **Original:** "**Guard the key**: derive it from **authorized server-side context** (e.g. the user from <Link text="getContext()" href="/getContext" />), never from raw client input — see <Link text="Authorization" href="/stream#authorization" />."
- **Clarity:** 9/10 — Clear, concrete guidance; the "never from raw client input" contrast is strong.
- **Naturalness:** 9/10 — Reads naturally despite the embedded links.
- **Overall:** 9/10
- **Action:** Kept

### [55] `docs/pages/channel/+Page.mdx` — Warning bullet, guard the payload (line 240, sentence 1)
- **Original:** "**Guard the payload**: whatever you `publish()` reaches every subscriber, so broadcast only non-sensitive data."
- **Clarity:** 9/10 — Clear; "broadcast only non-sensitive data" is unambiguous advice.
- **Naturalness:** 9/10 — Natural imperative.
- **Overall:** 9/10
- **Action:** Kept

### [56] `docs/pages/channel/+Page.mdx` — Warning bullet, guard the payload (line 240, sentence 2)
- **Original:** "That's how <Link text={<code>@telefunc/tanstack-query</code>} href="/tanstack-query" /> stays safe — it broadcasts only a *"refetch"* signal (the query key), and each client loads the actual data through its own authorized telefunction."
- **Clarity:** 9/10 — Concrete example; clear how the pattern preserves safety.
- **Naturalness:** 9/10 — Reads naturally; em-dash + parenthetical are on-voice.
- **Overall:** 9/10
- **Action:** Kept

### [57] `docs/pages/channel/+Page.mdx` — Notifications intro (line 288)
- **Original:** "A per-user key with a subscribe-only client — the server returns a `BroadcastChannel` keyed to the user, and the client only listens:"
- **Clarity:** 9/10 — Clear; "subscribe-only client" is a tidy descriptor explained by the rest.
- **Naturalness:** 9/10 — Caption-style lead-in; reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [58] `docs/pages/channel/+Page.mdx` — code comment (line 298)
- **Original:** "// Sent a toast to *all* clients match the user id (i.e. on all the tabs of all the user's devices)"
- **Clarity:** 4/10 — Ungrammatical; "Sent" (past tense) and "clients match" make it read as broken.
- **Naturalness:** 3/10 — "clients match the user id" is not grammatical English.
- **Overall:** 3/10 — Code comment, but a clear typo/grammar error per methodology — fixed.
- **Action:** Edited
- **Edit:** "// Send a toast to *all* clients matching the user id (i.e. on all the tabs of all the user's devices)"
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — Now grammatical imperative; "matching" fixes the relative clause. Minor: parenthetical is long, but it adds useful scope.

### [59] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, `publish(data)` (line 317)
- **Original:** "Publish to every subscriber of the key. Returns a receipt."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [60] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, `subscribe(cb)` (line 318)
- **Original:** "Receive `(data, info)`. Returns an unsubscribe function."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [61] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, `publishBinary(data)` (line 319)
- **Original:** "Publish raw binary to every subscriber. Returns a receipt."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [62] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, `subscribeBinary(cb)` (line 320)
- **Original:** "Receive `(data, info)` for binary messages. Returns an unsubscribe function."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [63] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, lifecycle (line 321)
- **Original:** "Lifecycle callbacks."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [64] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, `close()` (line 322)
- **Original:** "Close gracefully."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [65] `docs/pages/channel/+Page.mdx` — BroadcastChannel methods table, `abort(value?)` (line 323)
- **Original:** "Terminate immediately."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [66] `docs/pages/channel/+Page.mdx` — instance-vs-static callout (line 325, sentence 1)
- **Original:** "The instance is bound to its `key`, so `publish(data)` / `subscribe(cb)` take no key."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [67] `docs/pages/channel/+Page.mdx` — instance-vs-static callout (line 325, sentence 2)
- **Original:** "The static <Link text={<code>Broadcast.publish(key, data)</code>} href="#broadcast" /> / `Broadcast.subscribe(key, cb)` take it first — and have no `onOpen` / `onClose` / `close` / `abort`, since there's no instance to manage."
- **Clarity:** 9/10 — Clear; "take it first" refers to the key, unambiguous after sentence 1.
- **Naturalness:** 9/10 — Dense method enumeration but reads correctly.
- **Overall:** 9/10
- **Action:** Kept

### [68] `docs/pages/channel/+Page.mdx` — Fundamentally (line 332)
- **Original:** "Fundamentally, the difference is what a message is addressed to: a channel message is addressed to *someone*, a broadcast message is addressed to *a topic* (the `key`)."
- **Clarity:** 9/10 — Clear contrast; the "addressed to" framing is the right mental model.
- **Naturalness:** 8/10 — The leading "Fundamentally," restates the "### Fundamentally" heading directly above it, a mild redundancy. Kept because removing it (a) edits prose only marginally and (b) the word still reads naturally as a topic sentence; net Overall stays ≥ 8.
- **Overall:** 8/10
- **Action:** Kept

### [69] `docs/pages/channel/+Page.mdx` — Fundamentally, phone call (line 334)
- **Original:** "A channel is a conversation, like a phone call: two fixed ends (the server and the one client that received the channel), private to them, ending when either side hangs up."
- **Clarity:** 9/10 — Vivid analogy; all three trailing clauses map cleanly to channel properties.
- **Naturalness:** 9/10 — Reads naturally; parallel participial clauses are elegant.
- **Overall:** 9/10
- **Action:** Kept

### [70] `docs/pages/channel/+Page.mdx` — Fundamentally, radio (line 336)
- **Original:** "A broadcast is an announcement, like a radio frequency: whoever tunes in to the `key` receives everything published to it, members come and go, and the `key` belongs to no one."
- **Clarity:** 9/10 — Strong analogy parallel to [69]; clear.
- **Naturalness:** 9/10 — Natural, balanced with the phone-call sentence.
- **Overall:** 9/10
- **Action:** Kept

### [71] `docs/pages/channel/+Page.mdx` — Fundamentally, usage contrast (line 338, sentence 1)
- **Original:** "Only the two ends of a channel can use it, while anything that knows a broadcast's `key` can join it."
- **Clarity:** 9/10 — Clear contrast; "while" carries the comparison cleanly.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [72] `docs/pages/channel/+Page.mdx` — Fundamentally, lifetime contrast (line 338, sentence 2)
- **Original:** "A channel ends when either side closes it, while a broadcast outlives any single member."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [73] `docs/pages/channel/+Page.mdx` — Technically (line 342, sentence 1)
- **Original:** "You can think of `new BroadcastChannel()` as `new Channel()` plus the static `Broadcast.publish()` / `Broadcast.subscribe()`."
- **Clarity:** 9/10 — Clear mental-model framing.
- **Naturalness:** 9/10 — Natural "You can think of X as Y" idiom.
- **Overall:** 9/10
- **Action:** Kept

### [74] `docs/pages/channel/+Page.mdx` — Technically (line 342, sentence 2)
- **Original:** "The server holds a private channel to the one client that received it, then bridges that client into the keyed broadcast group — the same group those static methods publish to and subscribe from."
- **Clarity:** 9/10 — Clear; the trailing "the same group ..." ties the composition together well.
- **Naturalness:** 9/10 — Reads naturally; long but well-paced.
- **Overall:** 9/10
- **Action:** Kept

### [75] `docs/pages/channel/+Page.mdx` — Comparison table, Topology / Channel (line 348)
- **Original:** "One server ↔ one client"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [76] `docs/pages/channel/+Page.mdx` — Comparison table, Topology / BroadcastChannel (line 348)
- **Original:** "All members sharing a `key` (server and client)"
- **Clarity:** 9/10 — Clear; the parenthetical clarifies who counts as a member.
- **Naturalness:** 9/10 — Noun-phrase cell, fits the table.
- **Overall:** 9/10
- **Action:** Kept

### [77] `docs/pages/channel/+Page.mdx` — Comparison table, Delivery / Channel (line 350)
- **Original:** "The other end"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [78] `docs/pages/channel/+Page.mdx` — Comparison table, Delivery / BroadcastChannel (line 350)
- **Original:** "Every subscriber of the `key`, including the publisher"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [79] `docs/pages/channel/+Page.mdx` — Comparison table, Message type / Channel (line 351)
- **Original:** "Asymmetric: Two different types ...; The server uses the `new Channel()` instance and returns `channel.client` to the client"
- **Clarity:** 9/10 — Clear; the list items concretely explain "Asymmetric".
- **Naturalness:** 9/10 — Reads naturally for a list-in-cell.
- **Overall:** 9/10
- **Action:** Kept

### [80] `docs/pages/channel/+Page.mdx` — Comparison table, Message type / BroadcastChannel (line 351)
- **Original:** "Symmetric: Same type for every member ...; The server uses and returns the same `BroadcastChannel` instance"
- **Clarity:** 9/10 — Clear; parallel to [79].
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [81] `docs/pages/channel/+Page.mdx` — Comparison table, Message return / Channel (line 352)
- **Original:** "The other end's reply (if `{ ack: true }`)"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [82] `docs/pages/channel/+Page.mdx` — Comparison table, Message return / BroadcastChannel (line 352)
- **Original:** "A receipt: `{ key, seq, timestamp }`"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [83] `docs/pages/channel/+Page.mdx` — Comparison table, `close()` / Channel (line 353)
- **Original:** "Closes the channel for both ends"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [84] `docs/pages/channel/+Page.mdx` — Comparison table, `close()` / BroadcastChannel (line 353)
- **Original:** "Detaches this member; the group lives on"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [85] `docs/pages/channel/+Page.mdx` — Multi-server intro (line 359, sentence 1)
- **Original:** "By default, broadcast is in-memory — messages only reach subscribers on the same server."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [86] `docs/pages/channel/+Page.mdx` — Multi-server intro (line 359, sentence 2)
- **Original:** "To broadcast across multiple servers, configure a transport on `config.broadcast.transport`."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [87] `docs/pages/channel/+Page.mdx` — Redis pointer (line 361)
- **Original:** "For Redis-backed multi-server routing and broadcast fan-out, see <Link href="/redis" text={<code>@telefunc/redis</code>} />."
- **Clarity:** 9/10 — Clear; "routing and broadcast fan-out" is precise.
- **Naturalness:** 9/10 — Natural pointer sentence.
- **Overall:** 9/10
- **Action:** Kept

### [88] `docs/pages/channel/+Page.mdx` — Custom transport (line 365, sentence 1)
- **Original:** "For other backends (NATS, Kafka, …), implement `BroadcastTransport` and assign it to `config.broadcast.transport`."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [89] `docs/pages/channel/+Page.mdx` — Custom transport (line 365, sentence 2)
- **Original:** "Telefunc wraps it with subscriber multiplexing and same-node delivery, so each `key` only opens one upstream subscription no matter how many local subscribers attach."
- **Clarity:** 8/10 — "subscriber multiplexing and same-node delivery" is dense jargon; the trailing clause does explain the payoff, which rescues it.
- **Naturalness:** 9/10 — Reads as competent systems-doc prose.
- **Overall:** 8/10
- **Action:** Kept

### [90] `docs/pages/channel/+Page.mdx` — transport contract (line 376, sentence 1)
- **Original:** "`send` / `sendBinary` must return the assigned `{ seq, timestamp }` so subscribers across nodes see a single global order per key."
- **Clarity:** 9/10 — Clear; "single global order per key" precisely states the guarantee.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [91] `docs/pages/channel/+Page.mdx` — transport contract (line 376, sentence 2)
- **Original:** "`listen` / `listenBinary` return an unsubscribe function and are called at most once per key."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [92] `docs/pages/channel/+Page.mdx` — Cloudflare callout (line 378)
- **Original:** "On Cloudflare Workers, Telefunc handles distributed broadcast automatically via Durable Objects — no transport needed."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [93] `docs/pages/channel/+Page.mdx` — Lifecycle code comment (line 384)
- **Original:** "// Connection established — safe to send"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept (comment scan — clear)

### [94] `docs/pages/channel/+Page.mdx` — Lifecycle code comment (line 390)
- **Original:** "// Graceful close — `close()` completed, channel done"
- **Clarity:** 9/10 — "channel done" is terse but clear in context.
- **Naturalness:** 9/10 — Comment style, fine.
- **Overall:** 9/10
- **Action:** Kept (comment scan)

### [95] `docs/pages/channel/+Page.mdx` — Lifecycle code comments (lines 392, 396)
- **Original:** "// Either side called abort(value)" / "// Connection lost beyond reconnectTimeout"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept (comment scan — clear)

### [96] `docs/pages/channel/+Page.mdx` — closed-channel callout (line 406)
- **Original:** "After `close()` or `abort()`, calling `send()` throws `ChannelClosedError` synchronously."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [97] `docs/pages/channel/+Page.mdx` — Reconnection intro (line 411)
- **Original:** "If the connection drops, Telefunc reconnects automatically and resumes existing channels and broadcasts:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [98] `docs/pages/channel/+Page.mdx` — Reconnection bullet (line 413)
- **Original:** "Messages sent while offline are buffered and delivered in order on reconnect."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [99] `docs/pages/channel/+Page.mdx` — Reconnection bullet (line 414)
- **Original:** "Both sides keep a bounded replay buffer; after reconnect, missing frames are replayed."
- **Clarity:** 9/10 — Clear; "bounded replay buffer" and "frames" are precise for the audience.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [100] `docs/pages/channel/+Page.mdx` — Reconnection bullet (line 415)
- **Original:** "`onOpen()` fires only on initial open, `onClose()` only on permanent close."
- **Clarity:** 9/10 — Clear; the elided second "fires" reads fine due to parallelism.
- **Naturalness:** 9/10 — Natural compression.
- **Overall:** 9/10
- **Action:** Kept

### [101] `docs/pages/channel/+Page.mdx` — Reconnection callout (line 417)
- **Original:** "Reconnection is automatic — you don't need to handle it in your application code."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [102] `docs/pages/channel/+Page.mdx` — Error handling intro (line 422)
- **Original:** "**Channels and broadcasts** signal failure through four errors:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [103] `docs/pages/channel/+Page.mdx` — Error table, `Abort` / When (line 426)
- **Original:** "Either side called `abort(value)`, or a callback threw `Abort`"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [104] `docs/pages/channel/+Page.mdx` — Error table, `Abort` / Recovery (line 426)
- **Original:** "Inspect `err.abortValue`"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [105] `docs/pages/channel/+Page.mdx` — Error table, `NetworkError` / When (line 427)
- **Original:** "Connection lost beyond `reconnectTimeout`"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [106] `docs/pages/channel/+Page.mdx` — Error table, `NetworkError` / Recovery (line 427)
- **Original:** "Auto-closed — recreate if needed"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [107] `docs/pages/channel/+Page.mdx` — Error table, `ChannelClosedError` / When (line 428)
- **Original:** "`send()` on a closed channel (thrown synchronously); also rejects pending `ack` sends orphaned by close or close timeout"
- **Clarity:** 8/10 — "orphaned by close or close timeout" is dense; meaning recoverable but it asks the reader to parse two failure modes packed into one clause.
- **Naturalness:** 9/10 — Reads as terse reference prose, acceptable in a table.
- **Overall:** 8/10
- **Action:** Kept

### [108] `docs/pages/channel/+Page.mdx` — Error table, `ChannelClosedError` / Recovery (line 428)
- **Original:** "Recreate the channel"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [109] `docs/pages/channel/+Page.mdx` — Error table, `ChannelOverflowError` / When (line 429)
- **Original:** "A buffered send was dropped — the buffer exceeded `config.channel.bufferLimit`"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [110] `docs/pages/channel/+Page.mdx` — Error table, `ChannelOverflowError` / Recovery (line 429)
- **Original:** "Only the dropped `send()` rejects; `await` your sends to apply backpressure"
- **Clarity:** 9/10 — Clear; "Only the dropped `send()` rejects" usefully scopes the failure.
- **Naturalness:** 9/10 — Natural reference style.
- **Overall:** 9/10
- **Action:** Kept

### [111] `docs/pages/channel/+Page.mdx` — graceful-close callout (line 431)
- **Original:** "A graceful `close()` is not an error (`onClose(err)` receives `undefined`)."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [112] `docs/pages/channel/+Page.mdx` — Configuration callout (line 436, sentence 1)
- **Original:** "This is the **server-side** `config.channel` — reconnect/idle timeouts and buffer limits."
- **Clarity:** 9/10 — Clear; the em-dash gloss names what the config covers.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [113] `docs/pages/channel/+Page.mdx` — Configuration callout (line 436, sentence 2)
- **Original:** "It's a separate object from the **client-side** <Link text={<code>config.channel.transports</code>} href="/transport#channel-transport" /> (`telefunc/client`)."
- **Clarity:** 9/10 — Clear distinction; the `telefunc/client` parenthetical pins down where it lives.
- **Naturalness:** 9/10 — Reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [114] `docs/pages/channel/+Page.mdx` — Configuration callout (line 436, sentence 3)
- **Original:** "The assignment below sets every server-side field at once and doesn't touch the client transports."
- **Clarity:** 9/10 — Clear; explains the intent of the example.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [115] `docs/pages/channel/+Page.mdx` — config comment, reconnectTimeout (line 444)
- **Original:** "// Hold channels open after disconnect (ms)"
- **Clarity:** 9/10 — Clear inline comment.
- **Naturalness:** 9/10 — Comment style, fine.
- **Overall:** 9/10
- **Action:** Kept (comment scan)

### [116] `docs/pages/channel/+Page.mdx` — config comment, idleTimeout (line 445)
- **Original:** "// Keep connection alive after last channel closes (ms)"
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Comment style.
- **Overall:** 9/10
- **Action:** Kept (comment scan)

### [117] `docs/pages/channel/+Page.mdx` — config comments, buffers (lines 446–455)
- **Original:** "// Client ping interval (ms)" / "// Wait for client to connect to new channel (ms)" / "// Server replay buffer for text frames (bytes)" / "// Binary buffers — separate budget so binary can never evict text" / etc.
- **Clarity:** 9/10 — All clear and consistent; "separate budget so binary can never evict text" is a nice explanatory comment.
- **Naturalness:** 9/10 — Comment style throughout, consistent.
- **Overall:** 9/10
- **Action:** Kept (comment scan — no typos found)

### [118] `docs/pages/channel/+Page.mdx` — What to tune callout (line 459)
- **Original:** "**What to tune**: the defaults suit typical chat/dashboard traffic; tune these only if you run into one of the issues below:"
- **Clarity:** 9/10 — Clear; "the issues below" correctly forward-references the bullets.
- **Naturalness:** 9/10 — Natural advice phrasing.
- **Overall:** 9/10
- **Action:** Kept

### [119] `docs/pages/channel/+Page.mdx` — What to tune bullet 1 (line 460)
- **Original:** "Slow/flaky clients dropping with `NetworkError` (`isChannel: true`) → raise `reconnectTimeout` (how long the server holds a channel open while a client is gone)."
- **Clarity:** 9/10 — Clear problem→fix mapping; the parenthetical defines the knob.
- **Naturalness:** 9/10 — "Slow/flaky" is informal but apt; arrow notation is common in tuning docs.
- **Overall:** 9/10
- **Action:** Kept

### [120] `docs/pages/channel/+Page.mdx` — What to tune bullet 2 (line 461)
- **Original:** "`ChannelOverflowError` while a peer is briefly offline → raise `bufferLimit` / `bufferLimitBinary`, or apply backpressure by `await`-ing your `send()`s."
- **Clarity:** 9/10 — Clear; two remedies clearly offered.
- **Naturalness:** 9/10 — Natural; "`await`-ing your `send()`s" is idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [121] `docs/pages/channel/+Page.mdx` — What to tune bullet 3 (line 462)
- **Original:** "Want reconnects to replay more history → raise the replay buffers; lower them to cap memory."
- **Clarity:** 9/10 — Clear bidirectional guidance.
- **Naturalness:** 9/10 — Slightly clipped ("Want ... → ...") but consistent with the other bullets.
- **Overall:** 9/10
- **Action:** Kept

---

## Summary

- **Sentences/prose units reviewed:** 121 (includes paragraph sentences, list items, callout
  sentences, `<Link>` descriptions, prose table cells, and scanned code comments).
- **Kept:** 120
- **Edited:** 1 (entry [58] — the code comment on line 298: "Sent a toast to *all* clients match the
  user id ..." → "Send a toast to *all* clients matching the user id ...").
- **Second-PR candidates:** 0

### Notes
- This is a strong, well-edited page. The prose is consistently concise, technical, and on-voice
  (frequent em-dashes, analogy pairs, problem→fix tuning bullets). Most units land at 9/10: clear
  and natural, but dense enough (stacked jargon, telegraphic table cells) that they are not flawless.
- A handful of borderline units were considered but kept because no rewrite reached a *clearly*
  higher Overall without risking the established voice or technical precision:
  - [68] line 332: the leading "Fundamentally," echoes the "### Fundamentally" heading directly
    above. Removing it is tempting, but the word also functions as a legitimate topic-sentence
    discourse marker, and the sentence already rates 8/10, above the edit threshold — left as-is.
  - [39]/[34]/[89]/[107] are dense (long appositives or stacked terms) but each resolves its own
    jargon within the sentence; rated 8/10, above the ≤ 7 edit threshold.
- All MDX/JSX (`<Link/>`, `<Warning>`, `<StreamingBeta/>`, `<NeedsLongRunningServer/>`, `<code>`,
  `<ul>/<li>`, inline code, emphasis, anchors, URLs) and the comparison/methods/error tables were
  left structurally untouched. The single edit changed only code-comment prose.
