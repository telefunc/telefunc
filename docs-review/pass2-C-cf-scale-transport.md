# Pass-2 report — C: cloudflare / scale / transport

Reviewer C. Files:
- `docs/pages/stream/cloudflare/+Page.mdx`
- `docs/pages/stream/scale/+Page.mdx`
- `docs/pages/transport/+Page.mdx`

---

## `docs/pages/stream/cloudflare/+Page.mdx`

### [1] `docs/pages/stream/cloudflare/+Page.mdx` — intro line
- **Original:** "Cloudflare-specific setup for <Link href="/stream">real-time</Link> (i.e. <Link href="/channel">`Channel` and `BroadcastChannel`</Link>)."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "i.e." reads slightly formal where "namely" or a colon would be smoother; nested links make the fragment dense.
- **Candidates:**
  1. "Cloudflare-specific setup for <Link href="/stream">real-time</Link> — <Link href="/channel">`Channel` and `BroadcastChannel`</Link>." — C 9 / N 9 / Overall 9 — em-dash replaces the formal "(i.e. …)"; reads as a clean appositive, lighter than nested parentheses + abbreviation.
  2. "Cloudflare-specific setup for <Link href="/stream">real-time</Link>: <Link href="/channel">`Channel` and `BroadcastChannel`</Link>." — C 9 / N 8 / Overall 8 — colon works, but two colon-style breaks (this and the page's later patterns) make em-dash feel more in-voice.
  3. "Cloudflare-specific setup for <Link href="/stream">real-time</Link> (namely <Link href="/channel">`Channel` and `BroadcastChannel`</Link>)." — C 8 / N 8 / Overall 8 — "namely" is less formal than "i.e." but keeps the parenthetical density.
- **Decision:** Applied → "Cloudflare-specific setup for <Link href="/stream">real-time</Link> — <Link href="/channel">`Channel` and `BroadcastChannel`</Link>." (new Overall 9)
- **Why:** Em-dash appositive removes the formal "i.e." and the parenthesis, matching Telefunc's em-dash voice; both links preserved.

### [4] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, sentence 3
- **Original:** "The state therefore needs to live somewhere else, and Durable Objects and KV are Cloudflare's primitives for exactly that purpose:"
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "exactly that purpose" is mild filler; the "Durable Objects and KV are … and" doubling of "and" lengthens it.
- **Candidates:**
  1. "The state therefore needs to live somewhere else — Durable Objects and KV are Cloudflare's primitives for exactly that:" — C 9 / N 9 / Overall 9 — em-dash drops the first "and" (removing the doubling); "for exactly that" trims the "purpose" filler.
  2. "The state therefore needs to live somewhere else, and Durable Objects and KV are Cloudflare's primitives for precisely this:" — C 8 / N 8 / Overall 8 — fixes filler but keeps the double "and".
  3. "The state therefore needs to live elsewhere — Durable Objects and KV are Cloudflare's primitives built for exactly that:" — C 9 / N 8 / Overall 8 — "built for" re-adds words.
- **Decision:** Applied → "The state therefore needs to live somewhere else — Durable Objects and KV are Cloudflare's primitives for exactly that:" (new Overall 9)
- **Why:** One em-dash removes the doubled "and" and the trailing filler at once; meaning and the colon lead-in to the bullets are preserved.

### [6] `docs/pages/stream/cloudflare/+Page.mdx` — Setup blockquote, KV bullet
- **Original:** "**KV** provides shared storage: it's how stateless workers — wherever a request happens to land — find the Durable Object that holds the state."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — the inserted em-dash clause "wherever a request happens to land" adds a mid-sentence beat the reader must parse.
- **Candidates:**
  1. "**KV** provides shared storage: it's how stateless workers find the Durable Object that holds the state, wherever a request lands." — C 9 / N 9 / Overall 9 — moves the clause to the end (no interruption); "wherever a request lands" is tighter than "happens to land".
  2. "**KV** provides shared storage: it's how stateless workers locate the Durable Object that holds the state, no matter where a request lands." — C 9 / N 8 / Overall 8 — "no matter where" is longer.
  3. "**KV** provides shared storage: it's how a stateless worker — wherever the request lands — finds the Durable Object that holds the state." — C 8 / N 8 / Overall 8 — keeps the interruption.
- **Decision:** Applied → "**KV** provides shared storage: it's how stateless workers find the Durable Object that holds the state, wherever a request lands." (new Overall 9)
- **Why:** Relocating the clause to a trailing modifier removes the parsing beat while preserving the parallel "it's how …" structure with the Durable Objects bullet.

### [30] `docs/pages/stream/cloudflare/+Page.mdx` — How it works, step 1
- **Original:** "The publisher forwards to an **authority** Durable Object for the key."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "forwards to" without an object noun ("forwards the message") is slightly clipped.
- **Candidates:**
  1. "The publisher forwards the message to an **authority** Durable Object for the key." — C 9 / N 9 / Overall 9 — supplies the missing object noun; matches the other steps, which all act on the message.
  2. "The publisher forwards the publish to an **authority** Durable Object for the key." — C 8 / N 8 / Overall 8 — "the publish" as a noun is awkward.
  3. "The publisher sends the message to an **authority** Durable Object for the key." — C 9 / N 8 / Overall 8 — "sends" loses the relay/"forwards" nuance of the fan-out chain.
- **Decision:** Applied → "The publisher forwards the message to an **authority** Durable Object for the key." (new Overall 9)
- **Why:** Adding "the message" completes the verb and aligns step 1 with steps 2–4; emphasis preserved.

### [39] `docs/pages/stream/cloudflare/+Page.mdx` — Delivery, Buffering cell
- **Original:** "The server buffers messages while the client is disconnected — up to `config.channel.bufferLimit` for text (default: 512 KB), and up to `config.channel.bufferLimitBinary` for binary (default: 2 MB). Binary frames have a separate budget and can never evict text."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — first sentence is long with stacked parentheticals.
- **Candidates:**
  1. "The server buffers messages while the client is disconnected: up to `config.channel.bufferLimit` for text (default: 512 KB) and up to `config.channel.bufferLimitBinary` for binary (default: 2 MB). Binary frames have a separate budget and can never evict text." — C 9 / N 9 / Overall 9 — colon introduces the two limits as a list; dropping the comma before "and" pairs the parallel clauses cleanly.
  2. "While the client is disconnected, the server buffers messages — up to `config.channel.bufferLimit` for text (default: 512 KB) and up to `config.channel.bufferLimitBinary` for binary (default: 2 MB). Binary frames have a separate budget and can never evict text." — C 9 / N 8 / Overall 8 — front-loads the condition but doesn't reduce the parentheticals.
  3. "The server buffers messages while the client is disconnected, up to two limits: `config.channel.bufferLimit` for text (default: 512 KB) and `config.channel.bufferLimitBinary` for binary (default: 2 MB). Binary frames have a separate budget and can never evict text." — C 9 / N 8 / Overall 8 — "up to two limits" adds a clunky meta-count.
- **Decision:** Applied → "The server buffers messages while the client is disconnected: up to `config.channel.bufferLimit` for text (default: 512 KB) and up to `config.channel.bufferLimitBinary` for binary (default: 2 MB). Binary frames have a separate budget and can never evict text." (new Overall 9)
- **Why:** The colon plus removed comma turn the two limits into a clean parallel pair; all config names, defaults, and the second sentence are unchanged.

### [44] `docs/pages/stream/cloudflare/+Page.mdx` — Broadcast guarantees, Delivery cell
- **Original:** "`publish()` resolves after all Durable Objects with subscribers have received the message. Client delivery happens over the channel wire."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "over the channel wire" is mild jargon and a slightly informal metaphor.
- **Candidates:**
  1. "`publish()` resolves after all Durable Objects with subscribers have received the message. Delivery to clients then happens over the channel connection." — C 9 / N 9 / Overall 9 — "channel connection" drops the "wire" metaphor; "then" clarifies that client delivery follows the server fan-out.
  2. "`publish()` resolves after all Durable Objects with subscribers have received the message. Each Durable Object then delivers it to its clients over the channel." — C 9 / N 8 / Overall 8 — adds who-delivers detail that overlaps with the fan-out section; longer.
  3. "`publish()` resolves after all Durable Objects with subscribers have received the message. Clients then receive it over the channel connection." — C 9 / N 9 / Overall 9 — also good; flips to the clients' perspective.
- **Decision:** Applied → "`publish()` resolves after all Durable Objects with subscribers have received the message. Delivery to clients then happens over the channel connection." (new Overall 9)
- **Why:** Keeps the original "delivery" framing while replacing the informal "wire" with "connection" and signaling ordering with "then".

### [51] `docs/pages/stream/cloudflare/+Page.mdx` — Hibernation, after warning
- **Original:** "Once all channels close, no clients are connected, and the reconnect and idle windows have expired, the Durable Object can hibernate."
- **Previous:** Clarity 7 / Naturalness 8 / Overall 7 — three coordinated conditions read as a flat list; condition 2 overlaps with condition 1; the back-to-back "and … and" parses oddly.
- **Note:** The literal Original is **no longer present** — the current file already carries a revised descendant: "The Durable Object can hibernate once all channels close, no clients remain connected, and both the reconnect and idle windows have expired." That revision already (a) moved the main clause to the front, fixing the heavy front-loaded conditional, and (b) inserted "both" before "the reconnect and idle windows", fixing the "and … and" parse. The only residual issue is the channels/clients semantic overlap, which appears to be a deliberate technical distinction (channels closing vs. the client connection lingering), so it is left intact. Candidates below operate on the current text.
- **Candidates:**
  1. "The Durable Object can hibernate once all channels are closed, no clients remain connected, and both the reconnect and idle windows have expired." — C 9 / N 9 / Overall 9 — "are closed" makes all three conditions parallel *states* (channels closed / clients connected / windows expired), reading more consistently than the event verb "close".
  2. "The Durable Object can hibernate once all channels have closed, no clients remain connected, and both the reconnect and idle windows have expired." — C 9 / N 9 / Overall 9 — "have closed" matches the "have expired" tense; equally good.
  3. Retain current text. — C 9 / N 8 / Overall 8/9 — already strong after the prior revision.
- **Decision:** Applied → "The Durable Object can hibernate once all channels are closed, no clients remain connected, and both the reconnect and idle windows have expired." (new Overall 9)
- **Why:** "are closed" aligns the three conditions as parallel states, a genuine small naturalness gain; the literal target Original was already gone, so this polishes its in-file descendant.

---

## `docs/pages/stream/scale/+Page.mdx`

### [53] `docs/pages/stream/scale/+Page.mdx` — intro sentence
- **Original:** "If you use <Link href="/stream">Telefunc's streaming capabilities</Link>, then scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements:"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the "If …, then …" construction is slightly formal and the sentence is long.
- **Candidates:**
  1. "If you use <Link href="/stream">Telefunc's streaming capabilities</Link>, scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements:" — C 9 / N 9 / Overall 9 — drops the formal "then"; the comma-led conditional reads more naturally.
  2. "Scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements when you use <Link href="/stream">Telefunc's streaming capabilities</Link>:" — C 8 / N 8 / Overall 8 — trailing link leaves the colon dangling after it, slightly awkward.
  3. "Once you use <Link href="/stream">Telefunc's streaming capabilities</Link>, scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements:" — C 8 / N 8 / Overall 8 — "Once" implies a temporal trigger, subtly off-meaning.
- **Decision:** Applied → "If you use <Link href="/stream">Telefunc's streaming capabilities</Link>, scaling Telefunc horizontally (multiple Node instances, multiple containers, multiple machines) adds two requirements:" (new Overall 9)
- **Why:** Removing "then" is the lowest-risk fix for the formality; meaning and the link are unchanged.

### [54] `docs/pages/stream/scale/+Page.mdx` — intro, bullet 1
- **Original:** "**Sticky sessions** (so a reconnecting client returns to the instance holding its channel) — the load balancer in front of your instances must route every request from the same client to the same instance."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the bold-term-then-parenthetical-then-em-dash structure is a bit busy.
- **Candidates:**
  1. "**Sticky sessions** (so a reconnecting client returns to the instance holding its channel) — your load balancer must route every request from a given client to the same instance." — C 9 / N 9 / Overall 9 — "your load balancer" drops "the load balancer in front of your instances" (one fewer "instances"); "a given client" reads cleaner against "the same instance". Keeps the parallel scaffold shared with bullet 2.
  2. "**Sticky sessions** — the load balancer in front of your instances must route every request from the same client to the same instance, so a reconnecting client returns to the instance holding its channel." — C 9 / N 9 / Overall 9 — moves the "why" to a trailing clause; cleaner, but breaks parallelism with bullet 2.
  3. "**Sticky sessions** (so a reconnecting client returns to the instance holding its channel): the load balancer in front of your instances must route every request from the same client to the same instance." — C 9 / N 8 / Overall 8 — colon for em-dash; still busy.
- **Decision:** Applied → "**Sticky sessions** (so a reconnecting client returns to the instance holding its channel) — your load balancer must route every request from a given client to the same instance." (new Overall 9)
- **Why:** Trimming the repeated "instance(s)" reduces the busyness flagged while preserving the intentional parallel structure with bullet 2; meaning intact.

### [55] `docs/pages/stream/scale/+Page.mdx` — intro, bullet 2
- **Original:** "**Cross-instance broadcast transport** (broadcast publishes must fan out across instances) — install a **broadcast transport** like <Link href="/redis">`@telefunc/redis`</Link>."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "broadcast transport" appears in both the bold heading and the instruction, slightly repetitive.
- **Candidates:**
  1. "**Cross-instance broadcast transport** (broadcast publishes must fan out across instances) — install one like <Link href="/redis">`@telefunc/redis`</Link>." — C 9 / N 9 / Overall 9 — "install one" refers back to the bold term, removing the repeated "broadcast transport".
  2. "**Cross-instance broadcast transport** (broadcast publishes must fan out across instances) — install an adapter like <Link href="/redis">`@telefunc/redis`</Link>." — C 8 / N 8 / Overall 8 — "adapter" introduces a term not used in this intro.
  3. "**Cross-instance broadcast transport** (broadcast publishes must fan out across instances) — provide one, like <Link href="/redis">`@telefunc/redis`</Link>." — C 8 / N 8 / Overall 8 — "provide" weaker than "install".
- **Decision:** Applied → "**Cross-instance broadcast transport** (broadcast publishes must fan out across instances) — install one like <Link href="/redis">`@telefunc/redis`</Link>." (new Overall 9)
- **Why:** "install one" anaphorically reuses the bold heading term, eliminating the repetition; the term stays bolded in the heading, the `<Link>` is preserved.

### [62] `docs/pages/stream/scale/+Page.mdx` — Sticky sessions, sentence 5
- **Original:** "The same load-balancer feature solves it for Telefunc: a sticky session, usually backed by a cookie or by the client IP."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "it" refers to the constraint from the prior sentence, which the reader must hold in mind.
- **Candidates:**
  1. "The same load-balancer feature solves the problem for Telefunc: a sticky session, usually backed by a cookie or by the client IP." — C 9 / N 9 / Overall 9 — "the problem" names the antecedent concretely, removing the bare-pronoun resolution.
  2. "The same load-balancer feature solves this for Telefunc: a sticky session, usually backed by a cookie or by the client IP." — C 9 / N 9 / Overall 9 — "this" is marginally more anchoring than "it" but still a pronoun.
  3. "The same load-balancer feature solves this for Telefunc too: a sticky session, usually backed by a cookie or by the client IP." — C 9 / N 8 / Overall 8 — "too" is redundant with "same".
- **Decision:** Applied → "The same load-balancer feature solves the problem for Telefunc: a sticky session, usually backed by a cookie or by the client IP." (new Overall 9)
- **Why:** "the problem" gives the verb a concrete object, directly fixing the dangling-pronoun knock while staying concise.

### [72] `docs/pages/stream/scale/+Page.mdx` — Cloud Run / serverless, sentence 2
- **Original:** "Broadcasts still work (publishes round-trip through Redis), but channel reconnects can fail unpredictably."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "fail unpredictably" is slightly fuzzy (no detail on the failure mode), though it accurately conveys non-determinism.
- **Candidates:**
  1. "Broadcasts still work (publishes round-trip through Redis), but channel reconnects can fail intermittently." — C 8 / N 9 / Overall 8 — "intermittently" implies time-based randomness; "unpredictably" better captures instance-routing non-determinism. Not an improvement.
  2. "Broadcasts still work (publishes round-trip through Redis), but channel reconnects may not land on the right instance and can fail." — C 9 / N 8 / Overall 8 — adds a mechanism, but states the failure mode more specifically than the source and is longer.
  3. "Broadcasts still work (publishes round-trip through Redis), but channel reconnects can fail without warning." — C 8 / N 8 / Overall 8 — "without warning" shifts the meaning toward "silently".
  4. Retain. — C 8 / N 9 / Overall 8 — "unpredictably" is the most accurate single word for "depends on which instance the request lands on".
- **Decision:** Retained (no candidate beat Overall 8 without changing meaning or adding facts).
- **Why:** Sharpening "unpredictably" would either add a failure-mode fact (forbidden) or shift the meaning; the word already correctly conveys the routing non-determinism.

### [74] `docs/pages/stream/scale/+Page.mdx` — Cloudflare, sentence 1
- **Original:** "Cloudflare Workers are the exception among serverless platforms: `telefunc/cloudflare` routes channels through Durable Objects instead of sticky sessions, and fans out broadcasts across regions automatically, so neither a sticky load balancer nor a broadcast transport is needed."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — long: stacks two mechanisms plus a conclusion, so the reader holds a lot before the "so" payoff.
- **Candidates:**
  1. "Cloudflare Workers are the exception among serverless platforms: `telefunc/cloudflare` routes channels through Durable Objects instead of sticky sessions, and fans out broadcasts across regions automatically. As a result, neither a sticky load balancer nor a broadcast transport is needed." — C 9 / N 9 / Overall 9 — splits the conclusion into its own sentence; the payoff lands after a shorter buildup. "As a result" replaces "so".
  2. "Cloudflare Workers are the exception among serverless platforms. `telefunc/cloudflare` routes channels through Durable Objects instead of sticky sessions and fans out broadcasts across regions automatically, so neither a sticky load balancer nor a broadcast transport is needed." — C 9 / N 8 / Overall 8 — split is at the wrong seam; the second sentence keeps the payoff-at-end issue.
  3. "Cloudflare Workers are the exception among serverless platforms: `telefunc/cloudflare` routes channels through Durable Objects instead of sticky sessions and fans out broadcasts across regions automatically. So neither a sticky load balancer nor a broadcast transport is needed." — C 9 / N 8 / Overall 8 — sentence-initial "So" is a touch informal here.
- **Decision:** Applied → "Cloudflare Workers are the exception among serverless platforms: `telefunc/cloudflare` routes channels through Durable Objects instead of sticky sessions, and fans out broadcasts across regions automatically. As a result, neither a sticky load balancer nor a broadcast transport is needed." (new Overall 9)
- **Why:** Giving the conclusion its own sentence with "As a result" relieves the load-before-payoff problem; inline code and meaning preserved, following sentence untouched.

---

## `docs/pages/transport/+Page.mdx`

### [87] `docs/pages/transport/+Page.mdx` — Stream transport table, `'sse-inline'` description
- **Original:** "Base64url-encoded SSE. Works through proxies that buffer binary."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "buffer binary" is terse (means "buffer binary responses") but understandable.
- **Candidates:**
  1. "Base64url-encoded SSE. Works through proxies that buffer binary responses." — C 9 / N 9 / Overall 9 — supplies the elided noun; matches the "When to use" wording ("buffers binary HTTP responses").
  2. "Base64url-encoded SSE. Works through proxies that buffer binary data." — C 9 / N 9 / Overall 9 — "binary data" also complete, but "responses" is what's actually buffered and is the page's established term.
  3. "Base64url-encoded SSE. Gets through proxies that buffer binary responses." — C 8 / N 8 / Overall 8 — "gets through" is more colloquial.
- **Decision:** Applied → "Base64url-encoded SSE. Works through proxies that buffer binary responses." (new Overall 9)
- **Why:** Adding "responses" completes the elliptical phrase and matches the page's own phrasing; table structure preserved.

### [93] `docs/pages/transport/+Page.mdx` — Channel transport, intro
- **Original:** "Controls how <Link text={<code>new Channel()</code>} href="/channel" /> and <Link text={<code>new BroadcastChannel()</code>} href="/channel#new-broadcastchannel" /> connections work, and which backend `config.stream.transport = 'channel'` uses."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — carries two ideas (how connections work + which backend the channel stream uses) in one fragment, making it slightly dense.
- **Candidates:**
  1. "Controls how <Link text={<code>new Channel()</code>} href="/channel" /> and <Link text={<code>new BroadcastChannel()</code>} href="/channel#new-broadcastchannel" /> connections work — and which backend `config.stream.transport = 'channel'` uses." — C 9 / N 9 / Overall 9 — em-dash gives the second idea a cleaner break than the comma, signaling a related-but-distinct point; in Telefunc voice.
  2. "Controls how <Link .../> and <Link .../> connections work. Also sets which backend `config.stream.transport = 'channel'` uses." — C 9 / N 8 / Overall 8 — two fragments; "Also sets" is choppy as a standalone.
  3. "Controls how <Link .../> and <Link .../> connections work, plus which backend `config.stream.transport = 'channel'` uses." — C 9 / N 8 / Overall 8 — "plus" is slightly casual.
- **Decision:** Applied → "Controls how <Link text={<code>new Channel()</code>} href="/channel" /> and <Link text={<code>new BroadcastChannel()</code>} href="/channel#new-broadcastchannel" /> connections work — and which backend `config.stream.transport = 'channel'` uses." (new Overall 9)
- **Why:** The em-dash separates the two ideas to reduce density without restructuring; both `<Link>` components and the inline code are preserved exactly.

### [103] `docs/pages/transport/+Page.mdx` — Channel transport, closing blockquote, sentence 2
- **Original:** "The client starts on SSE and seamlessly upgrades to WebSocket in the background (open channels keep working without interruption)."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "seamlessly" + "without interruption" is mildly redundant (both convey no disruption).
- **Candidates:**
  1. "The client starts on SSE and upgrades to WebSocket in the background (open channels keep working without interruption)." — C 9 / N 9 / Overall 9 — drops the abstract adverb "seamlessly"; the concrete parenthetical already conveys seamlessness.
  2. "The client starts on SSE and seamlessly upgrades to WebSocket in the background (open channels keep working)." — C 9 / N 8 / Overall 8 — keeps "seamlessly" but trims the more concrete "without interruption".
  3. "The client starts on SSE and transparently upgrades to WebSocket in the background (open channels keep working without interruption)." — C 8 / N 8 / Overall 8 — "transparently" still overlaps with "without interruption".
- **Decision:** Applied → "The client starts on SSE and upgrades to WebSocket in the background (open channels keep working without interruption)." (new Overall 9)
- **Why:** Removing "seamlessly" eliminates the redundancy while keeping the more informative parenthetical; meaning unchanged.

---

## Summary

- **Targets:** 15
- **Applied:** 14
- **Retained:** 1 ([72])
- **Not found (literal):** 1 ([51]) — literal Original already superseded by an in-file revision; the descendant sentence was located and polished, so it is counted under Applied.
- **New score distribution:** 14 targets raised to Overall 9; 1 retained at Overall 8 ([72]). No target was left below its previous score.

Notes:
- [51]: the literal target Original text was no longer in the file (already revised). The change was applied to its current in-file descendant; documented above.
- All edits preserve `<Link>` / `<code>` JSX, inline code, table structure, bold emphasis (in [55] the second bolded "broadcast transport" was removed as part of de-duplication, but the term remains bolded in the bullet heading), defaults, and meaning. Changed regions were re-read to confirm MDX/JSX integrity.
