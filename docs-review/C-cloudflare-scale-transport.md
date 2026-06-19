# Sentence review — Cloudflare / Scale / Transport

Reviewer C. Files reviewed (all NEW — whole-file prose review):
- `docs/pages/stream/cloudflare/+Page.mdx`
- `docs/pages/stream/scale/+Page.mdx`
- `docs/pages/transport/+Page.mdx`

Confirmed new files: none of the three appear in `/tmp/diffs/`, so every prose sentence is in scope.
Code blocks not rated; comments scanned for typos (none found).

---

## `docs/pages/stream/cloudflare/+Page.mdx`

### [1] `docs/pages/stream/cloudflare/+Page.mdx` — intro line
- **Original:** "Cloudflare-specific setup for <Link href="/stream">real-time</Link> (i.e. <Link href="/channel">`Channel` and `BroadcastChannel`</Link>)."
- **Clarity:** 8/10 — It's a sentence fragment (no verb), but acceptable as a page subtitle; "i.e." reads slightly formal where "namely" or a colon would be smoother.
- **Naturalness:** 8/10 — Fragment-as-subtitle is a normal docs pattern; the nested links make it dense.
- **Overall:** 8/10
- **Action:** Kept

### [2] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, sentence 1
- **Original:** "Fundamentally, channels are stateful: the server holds live state for each open channel."
- **Clarity:** 9/10 — Precise; the colon cleanly introduces the explanation.
- **Naturalness:** 9/10 — Reads well; "Fundamentally" is a slightly heavy opener but on-voice.
- **Overall:** 9/10
- **Action:** Kept

### [3] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, sentence 2
- **Original:** "Cloudflare Workers, however, are stateless and ephemeral — any request can be served by any worker, and nothing is remembered between requests."
- **Clarity:** 9/10 — Unambiguous; the em-dash expansion concretely defines both adjectives.
- **Naturalness:** 9/10 — Natural; mid-sentence "however" is correct but mildly formal.
- **Overall:** 9/10
- **Action:** Kept

### [4] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, sentence 3
- **Original:** "The state therefore needs to live somewhere else, and Durable Objects and KV are Cloudflare's primitives for exactly that purpose:"
- **Clarity:** 8/10 — Clear; "exactly that purpose" is mild filler.
- **Naturalness:** 8/10 — Reads naturally; the "Durable Objects and KV are ... primitives" doubling of "and" makes it a touch long.
- **Overall:** 8/10
- **Action:** Kept

### [5] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, DO bullet
- **Original:** "**Durable Objects** provide stateful compute: each channel lives on a Durable Object, which holds the connection and its state."
- **Clarity:** 9/10 — Precise and concrete.
- **Naturalness:** 9/10 — Clean; only the term "stateful compute" is jargon, but it's the correct Cloudflare term.
- **Overall:** 9/10
- **Action:** Kept

### [6] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, KV bullet
- **Original:** "**KV** provides shared storage: it's how stateless workers — wherever a request happens to land — find the Durable Object that holds the state."
- **Clarity:** 8/10 — Clear; the inserted em-dash clause "wherever a request happens to land" adds a beat the reader must parse.
- **Naturalness:** 9/10 — Natural, parallel with the DO bullet.
- **Overall:** 8/10
- **Action:** Kept

### [7] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, bindings
- **Original:** "Both bindings are required whenever you use `telefunc/cloudflare`."
- **Clarity:** 9/10 — Direct and unambiguous.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [8] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, opt-out
- **Original:** "If your app doesn't use `Channel` or `BroadcastChannel`, there's no state to keep — skip both bindings and use `serve()` instead (see <Link text="Low-level API" href="/server#low-level-api" />)."
- **Clarity:** 9/10 — Clear conditional with a concrete instruction.
- **Naturalness:** 9/10 — On-voice; the em-dash imperative is idiomatic Telefunc style.
- **Overall:** 9/10
- **Action:** Kept

### [9] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, cross-ref
- **Original:** "See <Link href="#architecture" /> for implementation details."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [10] `docs/pages/stream/cloudflare/+Page.mdx` — Context
- **Original:** "Pass per-request data to telefunctions via <Link text={<code>getContext()</code>} href="/getContext" />:"
- **Clarity:** 9/10 — Clear; "per-request data" is precise.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [11] `docs/pages/stream/cloudflare/+Page.mdx` — Architecture, sentence 1
- **Original:** "Telefunc uses [Durable Objects](https://developers.cloudflare.com/durable-objects/) for channel state and broadcast fan-out."
- **Clarity:** 9/10 — Clear; "fan-out" is a term defined later but used here first.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [12] `docs/pages/stream/cloudflare/+Page.mdx` — Architecture, sentence 2
- **Original:** "Channel state is in-memory JavaScript (closures, callbacks, local variables), so it must live on the same Durable Object as the WebSocket connection."
- **Clarity:** 9/10 — Concrete and unambiguous.
- **Naturalness:** 9/10 — Natural; "in-memory JavaScript" as a noun phrase is slightly compressed but reads fine.
- **Overall:** 9/10
- **Action:** Kept

### [13] `docs/pages/stream/cloudflare/+Page.mdx` — Architecture, KV intro
- **Original:** "[KV](https://developers.cloudflare.com/kv/) stores two things:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [14] `docs/pages/stream/cloudflare/+Page.mdx` — Architecture, KV bullet 1
- **Original:** "**Session tokens** — pin a browser to the same Durable Object across requests."
- **Clarity:** 9/10 — Clear; "pin ... to" is precise vocabulary.
- **Naturalness:** 9/10 — Telegraphic bullet style is appropriate here.
- **Overall:** 9/10
- **Action:** Kept

### [15] `docs/pages/stream/cloudflare/+Page.mdx` — Architecture, KV bullet 2
- **Original:** "**Broadcast presence** — tracks which Durable Objects have active subscribers for each key."
- **Clarity:** 9/10 — Clear and specific.
- **Naturalness:** 9/10 — Parallel with bullet 1; fine.
- **Overall:** 9/10
- **Action:** Kept

### [16] `docs/pages/stream/cloudflare/+Page.mdx` — Regions intro
- **Original:** "Telefunc maps each request to one of six geographic regions using Cloudflare's colocation data:"
- **Clarity:** 9/10 — Clear; "colocation data" is a Cloudflare-specific term the reader may not know, but it's accurate.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [17] `docs/pages/stream/cloudflare/+Page.mdx` — Regions, closing line
- **Original:** "Each region runs its own Durable Objects so that channel state stays close to users."
- **Clarity:** 9/10 — Clear cause/effect.
- **Naturalness:** 9/10 — Natural; "stays close to users" reads well.
- **Overall:** 9/10
- **Action:** Kept

### [18] `docs/pages/stream/cloudflare/+Page.mdx` — Session affinity, sentence 1
- **Original:** "Every telefunction call and channel message from a browser must reach the **same Durable Object** — otherwise that browser's channel state would be unavailable."
- **Clarity:** 9/10 — Clear; the consequence is concrete.
- **Naturalness:** 9/10 — On-voice; the conditional-after-em-dash is idiomatic here.
- **Overall:** 9/10
- **Action:** Kept

### [19] `docs/pages/stream/cloudflare/+Page.mdx` — Session affinity, sentence 2
- **Original:** "On the first request, Telefunc picks a Durable Object in the nearest region, stores a session token in KV (TTL: 24 hours), and returns that token to the client via the `x-telefunc-session` header."
- **Clarity:** 9/10 — Clear three-step description.
- **Naturalness:** 9/10 — Reads naturally despite being a long enumerated sentence.
- **Overall:** 9/10
- **Action:** Kept

### [20] `docs/pages/stream/cloudflare/+Page.mdx` — Session affinity, sentence 3
- **Original:** "Subsequent requests send this token back so Telefunc routes to the same Durable Object."
- **Clarity:** 9/10 — Clear; "routes to" is slightly terse but unambiguous.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [21] `docs/pages/stream/cloudflare/+Page.mdx` — Warning, sentence 1
- **Original:** "**CDN / reverse proxy** — make sure `x-telefunc-session` is not stripped from responses or requests."
- **Clarity:** 9/10 — Clear, actionable instruction.
- **Naturalness:** 9/10 — Imperative is appropriate for a warning.
- **Overall:** 9/10
- **Action:** Kept

### [22] `docs/pages/stream/cloudflare/+Page.mdx` — Warning, sentence 2
- **Original:** "Without it, each request would be routed to a random Durable Object and channel state would be lost."
- **Clarity:** 9/10 — Clear consequence.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [23] `docs/pages/stream/cloudflare/+Page.mdx` — Scaling, sentence 1
- **Original:** "By default, Telefunc creates one Durable Object per region."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [24] `docs/pages/stream/cloudflare/+Page.mdx` — Scaling, sentence 2
- **Original:** "Increase capacity with `scale`:"
- **Clarity:** 9/10 — Clear; very terse but the code block that follows resolves it.
- **Naturalness:** 9/10 — Natural lead-in.
- **Overall:** 9/10
- **Action:** Kept

### [25] `docs/pages/stream/cloudflare/+Page.mdx` — Scaling, after code
- **Original:** "This creates 4 Durable Objects per region, and Telefunc distributes sessions across them."
- **Clarity:** 9/10 — Clear; matches the code example.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [26] `docs/pages/stream/cloudflare/+Page.mdx` — Per-region scale, sentence 1
- **Original:** "Only specified regions get Durable Objects."
- **Clarity:** 9/10 — Clear; "specified" ties back to the config example.
- **Naturalness:** 9/10 — Natural, concise.
- **Overall:** 9/10
- **Action:** Kept

### [27] `docs/pages/stream/cloudflare/+Page.mdx` — Per-region scale, sentence 2
- **Original:** "Requests from unspecified regions fall back to `locationFallback`."
- **Clarity:** 9/10 — Clear; references a config key shown later in Configuration.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [28] `docs/pages/stream/cloudflare/+Page.mdx` — Distributed broadcast, intro
- **Original:** "Broadcasts (<Link href="/channel#new-broadcastchannel" />) fan out across all regions automatically."
- **Clarity:** 9/10 — Clear; "fan out" used as a verb is fine here.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [29] `docs/pages/stream/cloudflare/+Page.mdx` — How it works, Advanced callout
- **Original:** "You can skip this section — it's Telefunc's internal fan-out, not anything you configure."
- **Clarity:** 9/10 — Clear and reassuring.
- **Naturalness:** 9/10 — On-voice; "not anything you configure" is colloquial but reads well in a callout.
- **Overall:** 9/10
- **Action:** Kept

### [30] `docs/pages/stream/cloudflare/+Page.mdx` — How it works, step 1
- **Original:** "The publisher forwards to an **authority** Durable Object for the key."
- **Clarity:** 8/10 — Clear; "forwards to" without an object noun ("forwards the message") is slightly clipped but understandable from context.
- **Naturalness:** 9/10 — Natural in a numbered flow.
- **Overall:** 8/10
- **Action:** Kept

### [31] `docs/pages/stream/cloudflare/+Page.mdx` — How it works, step 2
- **Original:** "The authority assigns a monotonic sequence number, reads active presence from KV, and forwards to each region with subscribers."
- **Clarity:** 9/10 — Clear three-part action; "monotonic sequence number" is precise.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [32] `docs/pages/stream/cloudflare/+Page.mdx` — How it works, step 3
- **Original:** "Each region's coordinator fans out to the Durable Objects in that region."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [33] `docs/pages/stream/cloudflare/+Page.mdx` — How it works, step 4
- **Original:** "Each Durable Object delivers to its local subscribers."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [34] `docs/pages/stream/cloudflare/+Page.mdx` — Ordering
- **Original:** "Publishes for a given key go through a single authority, so subscribers receive messages in order with a monotonic `seq`."
- **Clarity:** 9/10 — Clear cause/effect; `seq` is tied to the earlier "sequence number."
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [35] `docs/pages/stream/cloudflare/+Page.mdx` — Presence, intro
- **Original:** "Telefunc uses KV to track which Durable Objects have active subscribers:"
- **Clarity:** 9/10 — Clear; slightly repeats the earlier "Broadcast presence" bullet but in context it's fine.
- **Naturalness:** 9/10 — Natural lead-in to the table.
- **Overall:** 9/10
- **Action:** Kept

### [36] `docs/pages/stream/cloudflare/+Page.mdx` — Presence, sentence 1
- **Original:** "A KV record is created on subscribe and deleted on unsubscribe."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [37] `docs/pages/stream/cloudflare/+Page.mdx` — Presence, sentence 2
- **Original:** "If a Durable Object is evicted (e.g. during a deployment), the record expires after 90 seconds and the region is excluded from fan-out."
- **Clarity:** 9/10 — Clear; the parenthetical example helps.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [38] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, Ordering cell (channel)
- **Original:** "Messages are delivered in order."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [39] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, Buffering cell
- **Original:** "The server buffers messages while the client is disconnected — up to `config.channel.bufferLimit` for text (default: 512 KB), and up to `config.channel.bufferLimitBinary` for binary (default: 2 MB). Binary frames have a separate budget and can never evict text."
- **Clarity:** 8/10 — Dense but precise; the two limits and their defaults are clearly attributed. "can never evict text" is unambiguous once "separate budget" is read.
- **Naturalness:** 8/10 — Reads naturally for a reference table; the first sentence is long with stacked parentheticals.
- **Overall:** 8/10
- **Action:** Kept

### [40] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, Replay cell
- **Original:** "Both sides keep a replay buffer. On reconnect, missing messages are replayed and duplicates are ignored."
- **Clarity:** 9/10 — Clear two-sentence description.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [41] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, Acknowledgements cell
- **Original:** "`send(data, { ack: true })` resolves when the other side processes the message."
- **Clarity:** 9/10 — Clear; "the other side" is unambiguous in a two-party channel.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [42] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, Loss cell
- **Original:** "If the disconnection lasts longer than `config.channel.reconnectTimeout`, the channel closes with `NetworkError`."
- **Clarity:** 9/10 — Clear conditional.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [43] `docs/pages/stream/cloudflare/+Page.mdx` — Broadcast guarantees, Ordering cell
- **Original:** "Publishes for a given key are serialized and delivered in order."
- **Clarity:** 9/10 — Clear; "serialized" here means sequenced, which is consistent with the Ordering section.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [44] `docs/pages/stream/cloudflare/+Page.mdx` — Broadcast guarantees, Delivery cell
- **Original:** "`publish()` resolves after all Durable Objects with subscribers have received the message. Client delivery happens over the channel wire."
- **Clarity:** 8/10 — First sentence is precise. "over the channel wire" is mild jargon, but understandable.
- **Naturalness:** 8/10 — Natural; "channel wire" is a slightly informal metaphor for technical prose.
- **Overall:** 8/10
- **Action:** Kept

### [45] `docs/pages/stream/cloudflare/+Page.mdx` — Broadcast guarantees, Presence lag cell
- **Original:** "A new subscriber may miss publishes until its KV presence record is written, which typically takes a few milliseconds."
- **Clarity:** 6/10 — The trailing clause "which typically takes a few milliseconds" can be misread as modifying "publishes" rather than the writing of the record; minor dangling-modifier ambiguity.
- **Naturalness:** 8/10 — Reads fine, but the relative clause is loosely attached.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "A new subscriber may miss publishes during the few milliseconds it takes to write its KV presence record."
- **Edit rating:** Clarity 9/10, Naturalness 8/10, Overall 8/10 — removes the ambiguous antecedent and states the window directly; "during the few milliseconds it takes to" is slightly long but unambiguous.

### [46] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, closing blockquote
- **Original:** "Once a broadcast message reaches a Durable Object, it has the same buffering and replay guarantees as regular channel messages."
- **Clarity:** 9/10 — Clear; ties back cleanly to the channel guarantees table.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [47] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation warning, heading sentence
- **Original:** "**[Hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/#why-use-hibernation) while channels are open is not supported.**"
- **Clarity:** 9/10 — Clear, emphatic statement.
- **Naturalness:** 9/10 — Natural for a warning header.
- **Overall:** 9/10
- **Action:** Kept

### [48] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation warning, sentence 1
- **Original:** "Channel state is in-memory JavaScript."
- **Clarity:** 9/10 — Clear; restates a fact from Architecture, appropriate for a self-contained warning.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [49] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation warning, sentence 2
- **Original:** "If the Durable Object hibernates, that state is gone."
- **Clarity:** 9/10 — Clear; "is gone" is colloquial but effective in a warning.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [50] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation warning, sentence 3
- **Original:** "Telefunc keeps the Durable Object alive while channels are open."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [51] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation, after warning
- **Original:** "Once all channels close, no clients are connected, and the reconnect and idle windows have expired, the Durable Object can hibernate."
- **Clarity:** 7/10 — The three coordinated conditions are correct but read as a flat list where the second ("no clients are connected") overlaps with the first ("all channels close"); the doubled "and the reconnect and idle windows" is momentarily hard to parse.
- **Naturalness:** 8/10 — Slightly heavy front-loaded conditional, but grammatical.
- **Overall:** 7/10
- **Action:** Second-PR candidate (best edit did not reach a confident ≥8 without risking a change in technical meaning — see SECOND-PR CANDIDATES)

### [52] `docs/pages/stream/cloudflare/+Page.mdx` — See also (Link descriptions are auto-generated, not prose)
- **Note:** The four `<Link>` entries under "See also" render auto-generated titles, not authored prose — skipped per methodology (bare component tags / no authored prose).

---

## `docs/pages/stream/scale/+Page.mdx`

### [53] `docs/pages/stream/scale/+Page.mdx` — intro sentence
- **Original:** "If you use <Link href="/stream">Telefunc's streaming capabilities</Link>, then scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements:"
- **Clarity:** 9/10 — Clear; the parenthetical concretely defines "horizontally."
- **Naturalness:** 8/10 — Natural; the "If ..., then ..." construction is slightly formal and the sentence is long, but it reads fine.
- **Overall:** 8/10
- **Action:** Kept

### [54] `docs/pages/stream/scale/+Page.mdx` — intro, bullet 1
- **Original:** "**Sticky sessions** (so a reconnecting client returns to the instance holding its channel) — the load balancer in front of your instances must route every request from the same client to the same instance."
- **Clarity:** 9/10 — Clear; the parenthetical explains the "why" and the main clause the "what."
- **Naturalness:** 8/10 — Natural, though the bold-term-then-parenthetical-then-em-dash structure is a bit busy.
- **Overall:** 8/10
- **Action:** Kept

### [55] `docs/pages/stream/scale/+Page.mdx` — intro, bullet 2
- **Original:** "**Cross-instance broadcast transport** (broadcast publishes must fan out across instances) — install a **broadcast transport** like <Link href="/redis">`@telefunc/redis`</Link>."
- **Clarity:** 8/10 — Clear; "broadcast transport" appears in both the bold heading and the instruction, which is slightly repetitive but reinforces the term.
- **Naturalness:** 8/10 — Parallel with bullet 1; fine.
- **Overall:** 8/10
- **Action:** Kept

### [56] `docs/pages/stream/scale/+Page.mdx` — intro closing, sentence 1
- **Original:** "That's the whole picture."
- **Clarity:** 9/10 — Clear; idiomatic.
- **Naturalness:** 9/10 — On-voice (concise, direct); colloquial but intentional.
- **Overall:** 9/10
- **Action:** Kept

### [57] `docs/pages/stream/scale/+Page.mdx` — intro closing, sentence 2
- **Original:** "The rest of this page explains why and shows the common configurations."
- **Clarity:** 9/10 — Clear; "the common configurations" is slightly generic but acceptable.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [58] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 1
- **Original:** "A Telefunc `Channel` is a stateful connection."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [59] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 2
- **Original:** "Its server-side state — the `Channel` instance, its `send()`/`listen()` closures, any interval the telefunction set up — lives in one Node process."
- **Clarity:** 9/10 — Clear; the em-dash list concretely enumerates the state.
- **Naturalness:** 9/10 — Natural; "any interval the telefunction set up" reads idiomatically.
- **Overall:** 9/10
- **Action:** Kept

### [60] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 3
- **Original:** "When the client reconnects (network issues, a page reload, an SSE→WS upgrade), the next request has to land on the same process or the channel can't recover."
- **Clarity:** 9/10 — Clear; the parenthetical gives concrete reconnect triggers.
- **Naturalness:** 9/10 — Natural; "has to land on" is idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [61] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 4
- **Original:** "This is the same constraint Socket.IO documents under [Using multiple nodes](https://socket.io/docs/v4/using-multiple-nodes/)."
- **Clarity:** 9/10 — Clear; "documents under" reads naturally as "documents in the section titled."
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [62] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 5
- **Original:** "The same load-balancer feature solves it for Telefunc: a sticky session, usually backed by a cookie or by the client IP."
- **Clarity:** 8/10 — Clear; "it" refers to the constraint from the prior sentence, which the reader must hold in mind.
- **Naturalness:** 9/10 — Natural; the colon expansion is clean.
- **Overall:** 8/10
- **Action:** Kept

### [63] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 6
- **Original:** "Without sticky sessions, a reconnect that lands on a different instance sees no matching channel state, the recovery handshake fails, and your client's `Channel` ends."
- **Clarity:** 9/10 — Clear chain of consequences.
- **Naturalness:** 9/10 — Natural; the three-part outcome flows well.
- **Overall:** 9/10
- **Action:** Kept

### [64] `docs/pages/stream/scale/+Page.mdx` — Broadcast across instances, sentence 1
- **Original:** "A <Link text="broadcast" href="/channel#broadcast" /> is different: publishers and subscribers are intentionally decoupled — they only share a string key."
- **Clarity:** 9/10 — Clear; "different" implies "different from a channel," which the prior section established.
- **Naturalness:** 9/10 — Natural; colon + em-dash structure is readable.
- **Overall:** 9/10
- **Action:** Kept

### [65] `docs/pages/stream/scale/+Page.mdx` — Broadcast across instances, sentence 2
- **Original:** "Each instance keeps its own subscriber list locally, and the broadcast transport is what makes a publish on instance A reach a subscriber on instance B."
- **Clarity:** 9/10 — Clear; the A/B framing makes the cross-instance case concrete.
- **Naturalness:** 9/10 — Natural; "is what makes ... reach" is slightly wordy but idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [66] `docs/pages/stream/scale/+Page.mdx` — Broadcast across instances, sentence 3
- **Original:** "In a single-instance setup the default in-memory transport is enough."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural; a comma after "setup" would aid the read but is optional.
- **Overall:** 9/10
- **Action:** Kept

### [67] `docs/pages/stream/scale/+Page.mdx` — Broadcast across instances, sentence 4
- **Original:** "In a multi-instance setup, configure a transport that fans out across the cluster:"
- **Clarity:** 9/10 — Clear; lead-in to the code block.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [68] `docs/pages/stream/scale/+Page.mdx` — Broadcast across instances, after code
- **Original:** "See <Link href="/redis" /> for details."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [69] `docs/pages/stream/scale/+Page.mdx` — Broadcast across instances, custom transport
- **Original:** "Redis is the only adapter shipped today, but the `BroadcastTransport` interface is small (about four methods), so you can write a custom transport on top of NATS, Kafka, RabbitMQ, or any other message broker."
- **Clarity:** 9/10 — Clear; "shipped today" dates the statement nicely without overpromising.
- **Naturalness:** 9/10 — Natural; the list of brokers reads well.
- **Overall:** 9/10
- **Action:** Kept

### [70] `docs/pages/stream/scale/+Page.mdx` — AWS ALB
- **Original:** "In the target group's attributes, enable **Stickiness** with type **Load balancer generated cookie**."
- **Clarity:** 9/10 — Clear, actionable; the bold strings match AWS console labels.
- **Naturalness:** 9/10 — Natural imperative.
- **Overall:** 9/10
- **Action:** Kept

### [71] `docs/pages/stream/scale/+Page.mdx` — Cloud Run / serverless, sentence 1
- **Original:** "Serverless platforms that don't expose sticky-session routing aren't a good fit for `Channel`."
- **Clarity:** 9/10 — Clear; "expose ... routing" is precise.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [72] `docs/pages/stream/scale/+Page.mdx` — Cloud Run / serverless, sentence 2
- **Original:** "Broadcasts still work (publishes round-trip through Redis), but channel reconnects can fail unpredictably."
- **Clarity:** 8/10 — Clear; "fail unpredictably" is slightly fuzzy (no detail on the failure mode), but it accurately conveys non-determinism.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 8/10
- **Action:** Kept

### [73] `docs/pages/stream/scale/+Page.mdx` — Cloud Run / serverless, sentence 3
- **Original:** "Most teams pair Telefunc with a regular long-running server tier when they need channels at scale."
- **Clarity:** 9/10 — Clear; "regular long-running server tier" is concrete.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [74] `docs/pages/stream/scale/+Page.mdx` — Cloudflare, sentence 1
- **Original:** "Cloudflare Workers are the exception among serverless platforms: `telefunc/cloudflare` routes channels through Durable Objects instead of sticky sessions, and fans out broadcasts across regions automatically, so neither a sticky load balancer nor a broadcast transport is needed."
- **Clarity:** 8/10 — Clear and complete, but long: it stacks two mechanisms plus a conclusion in one sentence, so the reader must hold a lot before the "so" payoff.
- **Naturalness:** 8/10 — Natural; the length is the only knock.
- **Overall:** 8/10
- **Action:** Kept

### [75] `docs/pages/stream/scale/+Page.mdx` — Cloudflare, sentence 2
- **Original:** "You scale with the `scale` option instead of a load balancer."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [76] `docs/pages/stream/scale/+Page.mdx` — Sanity check, sentence 1
- **Original:** "After deploying behind a sticky load balancer, open the browser network tab, refresh once, and confirm every request to `/_telefunc` carries the same sticky cookie."
- **Clarity:** 9/10 — Clear, step-by-step instruction.
- **Naturalness:** 9/10 — Natural imperative sequence.
- **Overall:** 9/10
- **Action:** Kept

### [77] `docs/pages/stream/scale/+Page.mdx` — Sanity check, sentence 2
- **Original:** "If two consecutive requests carry different sticky values, the load balancer isn't configured for sticky sessions."
- **Clarity:** 9/10 — Clear diagnostic conditional.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

---

## `docs/pages/transport/+Page.mdx`

### [78] `docs/pages/transport/+Page.mdx` — intro
- **Original:** "Telefunc has two transport settings:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [79] `docs/pages/transport/+Page.mdx` — table, `config.stream.transport` cell
- **Original:** "How <Link href="/stream">streamed values</Link> (`AsyncGenerator`, `ReadableStream`, `Promise`) are delivered"
- **Clarity:** 9/10 — Clear; the parenthetical types make "streamed values" concrete.
- **Naturalness:** 9/10 — Natural for a table cell.
- **Overall:** 9/10
- **Action:** Kept

### [80] `docs/pages/transport/+Page.mdx` — table, `config.channel.transports` cell
- **Original:** "How <Link text={<code>new Channel()</code>} href="/channel" /> and <Link text={<code>new BroadcastChannel()</code>} href="/channel#new-broadcastchannel" /> connections work"
- **Clarity:** 9/10 — Clear; "connections work" is slightly generic but the context resolves it.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [81] `docs/pages/transport/+Page.mdx` — after table
- **Original:** "Plain telefunction calls are always `text/plain` JSON — these settings only affect streaming and channels."
- **Clarity:** 9/10 — Clear; "Plain telefunction calls" distinguishes from streaming/channel calls.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [82] `docs/pages/transport/+Page.mdx` — client/server callout, sentence 1
- **Original:** "**Client vs server `config.channel`.** The settings on this page are **client-side** (`telefunc/client`)."
- **Clarity:** 9/10 — Clear; the bold lead-in frames the distinction.
- **Naturalness:** 9/10 — Natural; the period-terminated bold "title" is a docpress callout convention.
- **Overall:** 9/10
- **Action:** Kept

### [83] `docs/pages/transport/+Page.mdx` — client/server callout, sentence 2
- **Original:** "The server-side `config.channel` is a *different* object (reconnect/idle timeouts and buffer limits), imported from `telefunc` — see <Link href="/channel#configuration" />."
- **Clarity:** 9/10 — Clear; the parenthetical lists what the server object holds.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [84] `docs/pages/transport/+Page.mdx` — client/server callout, sentence 3
- **Original:** "They share a name but live on separate imports and don't overlap, so setting one never clears the other."
- **Clarity:** 9/10 — Clear; "live on separate imports" is concrete.
- **Naturalness:** 9/10 — Natural; "live on separate imports" is mildly informal but idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [85] `docs/pages/transport/+Page.mdx` — Stream transport, intro
- **Original:** "Controls how streamed values are delivered over HTTP."
- **Clarity:** 9/10 — Clear; verb-initial fragment is a normal section-intro style.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [86] `docs/pages/transport/+Page.mdx` — Stream transport table, `'binary-inline'` description
- **Original:** "Raw binary chunked response. Lowest overhead."
- **Clarity:** 9/10 — Clear; terse but the two fragments are unambiguous.
- **Naturalness:** 9/10 — Natural for a table cell.
- **Overall:** 9/10
- **Action:** Kept

### [87] `docs/pages/transport/+Page.mdx` — Stream transport table, `'sse-inline'` description
- **Original:** "Base64url-encoded SSE. Works through proxies that buffer binary."
- **Clarity:** 8/10 — Clear; "buffer binary" is terse (means "buffer binary responses") but understandable.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 8/10
- **Action:** Kept

### [88] `docs/pages/transport/+Page.mdx` — Stream transport table, `'channel'` description
- **Original:** "Starts over HTTP, then continues over the configured channel transport."
- **Clarity:** 9/10 — Clear; "configured channel transport" points to the other setting.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [89] `docs/pages/transport/+Page.mdx` — Comparison key (stream)
- **Original:** "Key: ✅ good · 🟡 partial / caveats · ❌ none"
- **Clarity:** 9/10 — Clear legend.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [90] `docs/pages/transport/+Page.mdx` — When to use what (stream), bullet 1
- **Original:** "**Start with `'binary-inline'`** — it's the fastest and works in most setups."
- **Clarity:** 9/10 — Clear, actionable.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [91] `docs/pages/transport/+Page.mdx` — When to use what (stream), bullet 2
- **Original:** "**Switch to `'sse-inline'`** if a proxy or CDN buffers binary HTTP responses but passes SSE events through without buffering."
- **Clarity:** 9/10 — Clear; the contrast (buffers binary vs. passes SSE) is precise.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [92] `docs/pages/transport/+Page.mdx` — When to use what (stream), bullet 3
- **Original:** "**Use `'channel'`** when you want streamed values to reconnect automatically after a dropped connection (just as Telefunc channels do)."
- **Clarity:** 9/10 — Clear; the parenthetical analogy reinforces it.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [93] `docs/pages/transport/+Page.mdx` — Channel transport, intro
- **Original:** "Controls how <Link text={<code>new Channel()</code>} href="/channel" /> and <Link text={<code>new BroadcastChannel()</code>} href="/channel#new-broadcastchannel" /> connections work, and which backend `config.stream.transport = 'channel'` uses."
- **Clarity:** 8/10 — Clear; it carries two ideas ("how connections work" + "which backend the channel stream uses") in one fragment, making it slightly dense.
- **Naturalness:** 9/10 — Natural section intro.
- **Overall:** 8/10
- **Action:** Kept

### [94] `docs/pages/transport/+Page.mdx` — Channel transport table, `'sse'` description
- **Original:** "HTTP requests + SSE stream. Works without extra server setup."
- **Clarity:** 9/10 — Clear; the `+` shorthand reads fine in a table.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [95] `docs/pages/transport/+Page.mdx` — Channel transport table, `'ws'` description
- **Original:** "Multiplexed WebSocket. Requires <Link text="WebSocket server setup" href="/server" />."
- **Clarity:** 9/10 — Clear; "Multiplexed" is precise and explained later.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [96] `docs/pages/transport/+Page.mdx` — Channel transport, sentence after table 1
- **Original:** "The client default is `['sse', 'ws']` — start on SSE, then upgrade to WebSocket once the server offers it."
- **Clarity:** 9/10 — Clear; the array-then-explanation pattern is effective.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [97] `docs/pages/transport/+Page.mdx` — Channel transport, sentence after table 2
- **Original:** "The server offers SSE out-of-the-box and adds WebSocket when you install an adapter (see <Link href="/server" />)."
- **Clarity:** 9/10 — Clear; "out-of-the-box" matches house style.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [98] `docs/pages/transport/+Page.mdx` — Channel transport, multiplexing note
- **Original:** "All channels share a single multiplexed connection per server URL, so opening many channels doesn't open many connections."
- **Clarity:** 9/10 — Clear; "per server URL" scopes the sharing precisely.
- **Naturalness:** 9/10 — Natural; the "many ... many" repetition is deliberate and effective.
- **Overall:** 9/10
- **Action:** Kept

### [99] `docs/pages/transport/+Page.mdx` — Comparison key (channel)
- **Original:** "Key: ✅ good · 🟡 partial / caveats · ❌ none"
- **Clarity:** 9/10 — Clear legend (duplicate of the stream legend, which is fine).
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [100] `docs/pages/transport/+Page.mdx` — When to use what (channel), bullet 1
- **Original:** "**Start with `'sse'`** — it works everywhere out-of-the-box."
- **Clarity:** 9/10 — Clear.
- **Naturalness:** 9/10 — Natural; "works everywhere" is a mild generalization but acceptable.
- **Overall:** 9/10
- **Action:** Kept

### [101] `docs/pages/transport/+Page.mdx` — When to use what (channel), bullet 2
- **Original:** "**Use `'ws'`** for chatty real-time traffic where you want a full-duplex connection."
- **Clarity:** 6/10 — "chatty" is an informal, fuzzy qualifier for a recommendation; "where you want a full-duplex connection" is slightly awkward phrasing for "that benefits from."
- **Naturalness:** 7/10 — "chatty real-time traffic" is colloquial for technical reference docs.
- **Overall:** 6/10
- **Action:** Edited
- **Edit:** "**Use `'ws'`** for high-frequency, real-time traffic that benefits from a full-duplex connection."
- **Edit rating:** Clarity 8/10, Naturalness 8/10, Overall 8/10 — "high-frequency" replaces the colloquial "chatty" with a precise qualifier, and "that benefits from" reads more naturally than "where you want."

### [102] `docs/pages/transport/+Page.mdx` — Channel transport, closing blockquote, sentence 1
- **Original:** "When WebSocket is enabled (see <Link href="/server" />), the server supports both SSE and WS."
- **Clarity:** 9/10 — Clear conditional.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [103] `docs/pages/transport/+Page.mdx` — Channel transport, closing blockquote, sentence 2
- **Original:** "The client starts on SSE and seamlessly upgrades to WebSocket in the background (open channels keep working without interruption)."
- **Clarity:** 9/10 — Clear; the parenthetical reassures about continuity.
- **Naturalness:** 8/10 — Natural, though "seamlessly" + "without interruption" is mildly redundant (both convey no disruption).
- **Overall:** 8/10
- **Action:** Kept

### [104] `docs/pages/transport/+Page.mdx` — Recommended setup table, "Full-duplex channels" cell
- **Note:** The Goal/Config cells ("Best default", "Proxy buffers binary", "Streams with reconnection", "Channels without WS", "Full-duplex channels") are terse labels paired with code; they are pure-syntax/label cells rather than prose sentences — skipped per methodology.

### [105] `docs/pages/transport/+Page.mdx` — Per-call overrides, intro
- **Original:** "Override transport for a single call (instead of globally) with <Link text={<code>withContext()</code>} href="/withContext" /> from `telefunc/client`:"
- **Clarity:** 9/10 — Clear; "(instead of globally)" contrasts with the `config` approach.
- **Naturalness:** 9/10 — Natural lead-in to the code block.
- **Overall:** 9/10
- **Action:** Kept

### [106] `docs/pages/transport/+Page.mdx` — Per-call overrides, blockquote
- **Original:** "`withContext()` also takes a per-call `signal`, `headers`, and `telefuncUrl` — see <Link href="/withContext" /> for the full list."
- **Clarity:** 9/10 — Clear; lists the additional per-call options.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [107] `docs/pages/transport/+Page.mdx` — Channel & broadcast config, intro
- **Original:** "Beyond transport, channels and broadcasts expose more server-side `config` options:"
- **Clarity:** 9/10 — Clear; "Beyond transport" situates the section.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [108] `docs/pages/transport/+Page.mdx` — Channel & broadcast config, bullet 1
- **Original:** "**`config.channel`** — reconnect/idle timeouts and per-peer buffer limits. See <Link href="/channel#configuration" />."
- **Clarity:** 9/10 — Clear; concise list of what the option covers.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### [109] `docs/pages/transport/+Page.mdx` — Channel & broadcast config, bullet 2
- **Original:** "**`config.broadcast.transport`** — cross-instance broadcast for multi-server deployments. See <Link href="/channel#multi-server" />, or <Link href="/redis" /> for the Redis adapter."
- **Clarity:** 9/10 — Clear; gives both the general and Redis-specific reference.
- **Naturalness:** 9/10 — Natural.
- **Overall:** 9/10
- **Action:** Kept

### Code-comment scan (all three files)
- All code-block comments are accurate and free of typos:
  - cloudflare: `// worker.ts`, `// Environment: server`, `// wrangler.jsonc`, `// DO binding name (default)` etc., `// Shorter windows = hibernate sooner, less disconnect tolerance` — clear and correct.
  - scale: `// server.ts`, `// Environment: server` — fine.
  - transport: `// Environment: client`, `// default`, `// override config.stream.transport`, `// override config.channel.transports` — clear and correct.
- No comment fixes needed.

---

## Summary

- **Sentences reviewed:** 103 (entries [1]–[109]; [52], [104] are skip-notes for auto-generated/label cells, not rated sentences).
- **Kept:** 100
- **Edited (applied in place):** 2
  - cloudflare [45] — Presence lag cell (removed dangling modifier).
  - transport [101] — `'ws'` recommendation ("chatty" → "high-frequency").
- **Second-PR candidates:** 1 (cloudflare [51]).

Most sentences landed in the 8–9 band: the prose is technically precise and on-voice, but several sentences are long/multi-clause (notably cloudflare [39], [44]; scale [53], [74]; transport [93]) or rely on terms-of-art ("channel wire", "stateful compute", "colocation data") that are accurate but slightly jargon-heavy. None of those crossed the ≤7 edit threshold.

---

## SECOND-PR CANDIDATES

### [51] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation, after warning
- **Original:** "Once all channels close, no clients are connected, and the reconnect and idle windows have expired, the Durable Object can hibernate."
- **Clarity:** 7/10 — The three coordinated conditions read as a flat list; the second condition ("no clients are connected") overlaps semantically with the first ("all channels close"), and "the reconnect and idle windows" momentarily parses oddly because of the back-to-back "and ... and".
- **Naturalness:** 8/10 — Front-loaded conditional is grammatical but heavy.
- **Overall:** 7/10
- **Best edit:** "The Durable Object can hibernate once all channels close, no clients remain connected, and both the reconnect and idle windows have expired."
- **Best edit rating:** Clarity 7/10, Naturalness 8/10, Overall 7/10 — moving the main clause to the front improves flow, and "both the reconnect and idle windows" disambiguates the conjunction, but the underlying overlap between "all channels close" and "no clients remain connected" is a content-level redundancy I can't resolve without risking a change in technical meaning (the two conditions may be intentionally distinct — e.g. a connected client with no open channel). Left for a second pass where the author can confirm whether the two conditions are truly independent and whether one can be dropped.
- **Alternatives considered:**
  1. "The Durable Object can hibernate once all channels close, no clients remain connected, and both the reconnect and idle windows have expired." — Overall 7/10 — best flow, but keeps the apparent redundancy.
  2. "Once all channels close and the reconnect and idle windows have expired, the Durable Object can hibernate." — Overall 6/10 — drops "no clients are connected", which may change the technical meaning (not allowed).
  3. "The Durable Object can hibernate after all channels are closed, no clients are connected, and the reconnect and idle windows have expired." — Overall 7/10 — "after" vs "once" is neutral; redundancy remains.
  4. "Once every channel is closed, no clients are connected, and the reconnect and idle windows have both expired, the Durable Object can hibernate." — Overall 7/10 — "both" at the end is slightly awkward.
  5. "The Durable Object can hibernate only after all channels close, all clients disconnect, and the reconnect and idle windows expire." — Overall 7/10 — "only after" subtly strengthens the claim (possible meaning change); avoided.
  6. "Hibernation becomes possible once all channels close, no clients are connected, and the reconnect and idle windows have expired." — Overall 7/10 — "becomes possible" is wordier than "can hibernate".
  7. "The Durable Object can hibernate once all channels close, no client connections remain, and the reconnect and idle windows have expired." — Overall 7/10 — "no client connections remain" is marginally clearer than "no clients are connected" but the redundancy persists.
  8. "Once channels are all closed, clients are all disconnected, and the reconnect and idle windows have expired, the Durable Object can hibernate." — Overall 6/10 — "all closed / all disconnected" reads stilted.
  9. "The Durable Object can hibernate when no channels are open, no clients are connected, and the reconnect and idle windows have expired." — Overall 7/10 — "when" softens the precondition framing slightly; redundancy remains.
  10. "After all channels close and all clients disconnect — and once the reconnect and idle windows expire — the Durable Object can hibernate." — Overall 7/10 — the em-dash insertion helps separate the window condition but makes the sentence busier.
  - **Chosen:** #1 — but it still rates Overall 7/10, so it is NOT applied; recorded here for the second PR.
