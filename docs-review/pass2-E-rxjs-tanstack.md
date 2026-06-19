# Pass-2 report E — rxjs & tanstack-query

Second-pass polish of sentences previously rated 7–8. Targets span two files.

## `docs/pages/rxjs/+Page.mdx`

### [2] `docs/pages/rxjs/+Page.mdx` — intro (line 8)
- **Original:** "The `@telefunc/rxjs` integration lets you pass RxJS `Observable` and `Subject` directly between client and server — both directions, all operators."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the trailing fragment "both directions, all operators" reads as a punchy, slightly promotional appositive rather than precise reference prose.
- **Candidates:**
  1. "...directly between client and server, in both directions and with all operators." — C 10 / N 9 / Overall 9.5 — integrates the fragment as a clause; removes the clipped/promotional feel.
  2. "...directly between client and server — both directions, every operator." — C 9 / N 8 / Overall 8.5 — still a clipped fragment.
  3. "...directly between client and server, in either direction and with full operator support." — C 9 / N 9 / Overall 9 — "full operator support" echoes line 6.
  4. "...directly between client and server — in both directions, with all operators." — C 10 / N 9 / Overall 9.5 — keeps em-dash but makes it a proper phrase.
- **Decision:** Applied → "The `@telefunc/rxjs` integration lets you pass RxJS `Observable` and `Subject` directly between client and server, in both directions and with all operators." (new Overall 9.5)
- **Why:** Turns the clipped appositive into an integrated, precise clause without changing the facts.

### [6] `docs/pages/rxjs/+Page.mdx` — Live stock ticker (line 27)
- **Original:** "Server pushes prices every second."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the article-less "Server" reads headline-style, slightly clipped for body prose.
- **Candidates:**
  1. "The server pushes prices every second." — C 10 / N 10 / Overall 10 — adds the article; clean body prose.
  2. "The server pushes a price every second." — C 9 / N 9 / Overall 9 — narrows "prices" to "a price," subtly altering meaning.
  3. "Here, the server pushes prices every second." — C 9 / N 9 / Overall 9 — adds filler.
- **Decision:** Applied → "The server pushes prices every second." (new Overall 10)
- **Why:** Restoring the article fixes the only nit and matches the full-sentence style of the surrounding prose.

### [7] `docs/pages/rxjs/+Page.mdx` — Live stock ticker (line 27)
- **Original:** "The client filters and limits them locally, with standard RxJS operators."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the comma before "with standard RxJS operators" makes the trailing phrase feel bolted on.
- **Candidates:**
  1. "The client filters and limits them locally with standard RxJS operators." — C 10 / N 10 / Overall 10 — drop the comma; the adverbial integrates cleanly.
  2. "Using standard RxJS operators, the client filters and limits them locally." — C 9 / N 9 / Overall 9 — fronting the phrase changes the emphasis.
  3. "The client uses standard RxJS operators to filter and limit them locally." — C 10 / N 9 / Overall 9.5 — restructures more than necessary.
- **Decision:** Applied → "The client filters and limits them locally with standard RxJS operators." (new Overall 10)
- **Why:** Dropping the comma integrates the trailing phrase; no other change needed.

### [8] `docs/pages/rxjs/+Page.mdx` — Collaborative editor (line 58)
- **Original:** "A shared `Subject` returned to multiple clients multicasts across them: when one client emits with `next()`, every other client's subscribers receive the value through the server."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — a heavy subject is stacked before the verb (parsed twice), and "multicasts across them" is an unusual verb-phrasing.
- **Candidates:**
  1. "When a shared `Subject` is returned to multiple clients, it multicasts across them: when one client emits with `next()`, ..." — C 8 / N 8 / Overall 8 — two "when" clauses now clash.
  2. "A shared `Subject` returned to multiple clients is multicast across them: ..." — C 8 / N 8 / Overall 8 — passive, but still front-loaded.
  3. "Return a shared `Subject` to multiple clients and it multicasts among them: when one client emits with `next()`, ..." — C 9 / N 9 / Overall 9 — imperative opener lightens the front-loading; "among them" reads more idiomatically.
  4. "A shared `Subject` returned to multiple clients is multicast among them: ..." — C 9 / N 8 / Overall 8.5 — passive plus "among."
- **Decision:** Applied → "Return a shared `Subject` to multiple clients and it multicasts among them: when one client emits with `next()`, every other client's subscribers receive the value through the server." (new Overall 9)
- **Why:** The imperative opener removes the front-loaded noun phrase, and "among them" replaces the awkward "across them," both without changing the fact.

### [11] `docs/pages/rxjs/+Page.mdx` — Collaborative editor callout (line 83)
- **Original:** "These multicast examples (the editor above, and *Live cursors* below) work as-is on a single instance."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the mid-sentence parenthetical with an internal comma interrupts the flow.
- **Candidates:**
  1. "These multicast examples — the editor above and *Live cursors* below — work as-is on a single instance." — C 9 / N 9 / Overall 9.5 — em-dashes (Telefunc voice); drops the internal comma.
  2. "Both multicast examples (the editor above and *Live cursors* below) work as-is on a single instance." — C 9 / N 9 / Overall 9 — keeps parens; "Both" since there are two.
  3. "These multicast examples work as-is on a single instance: the editor above and *Live cursors* below." — C 9 / N 8 / Overall 8.5 — moving the list to the end weakens the link to "as-is."
- **Decision:** Applied → "These multicast examples — the editor above and *Live cursors* below — work as-is on a single instance." (new Overall 9.5)
- **Why:** Em-dashes set off the appositive cleanly and removing the comma-in-a-two-item-list smooths the flow; *Live cursors* emphasis preserved.

### [12] `docs/pages/rxjs/+Page.mdx` — Collaborative editor callout (line 83)
- **Original:** "Across multiple instances, each server process has its own `Subject` — so route shared state through a broadcast transport instead, see <Link href="/stream/scale" />."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — chaining an imperative onto a trailing "see X" via a comma is slightly overloaded.
- **Candidates:**
  1. "Across multiple instances, each server process has its own `Subject`, so route shared state through a broadcast transport instead — see <Link href="/stream/scale" />." — C 9 / N 9 / Overall 9.5 — the em-dash now sets off the "see X" pointer; a comma joins the consequence.
  2. "...has its own `Subject` — so route shared state through a broadcast transport instead (see <Link href="/stream/scale" />)." — C 9 / N 9 / Overall 9 — parenthesizes the see-reference.
  3. "...has its own `Subject`; to share state, route it through a broadcast transport instead — see <Link href="/stream/scale" />." — C 9 / N 8 / Overall 8.5 — restructures more than needed.
- **Decision:** Applied → "Across multiple instances, each server process has its own `Subject`, so route shared state through a broadcast transport instead — see <Link href="/stream/scale" />." (new Overall 9.5)
- **Why:** Moving the em-dash to set off "see <Link/>" gives the cross-reference a clean trailing pointer; `<Link/>` preserved exactly.

### [14] `docs/pages/rxjs/+Page.mdx` — Click heatmap (line 89)
- **Original:** "The server subscribes and processes the stream — useful for telemetry, analytics, or any client-driven event stream."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "useful for ..." dangles: it modifies the overall pattern, not the immediately preceding "the stream."
- **Candidates:**
  1. "...the stream — a good fit for telemetry, analytics, or any client-driven event stream." — C 9 / N 9 / Overall 9 — "a good fit for" attaches as a noun phrase.
  2. "The server subscribes and processes the stream. This pattern suits telemetry, analytics, or any client-driven event stream." — C 9 / N 9 / Overall 9.5 — splits into two sentences with explicit subject.
  3. "...the stream — useful for telemetry, analytics, and any other client-driven event stream." — C 8 / N 8 / Overall 8.5 — minimal; dangling clause remains.
  4. "...the stream, which is useful for telemetry, analytics, or any client-driven event stream." — C 8 / N 8 / Overall 8.5 — "which" referring to the whole action stays loose.
  5. "The server subscribes and processes the stream — a pattern useful for telemetry, analytics, or any client-driven event stream." — C 10 / N 9 / Overall 9.5 — "a pattern" gives the trailing clause an explicit noun to modify; keeps the em-dash rhythm.
- **Decision:** Applied → "The server subscribes and processes the stream — a pattern useful for telemetry, analytics, or any client-driven event stream." (new Overall 9.5)
- **Why:** Inserting the explicit noun "a pattern" anchors the previously dangling "useful for ..." clause while preserving the original structure and voice.

## `docs/pages/tanstack-query/+Page.mdx`

### [29] `docs/pages/tanstack-query/+Page.mdx` — Setup (line 35)
- **Original:** "All options and APIs keep working unchanged."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "keep working unchanged" is slightly informal/loose; "continue to work as before" would be tighter.
- **Candidates:**
  1. "All options and APIs continue to work as before." — C 10 / N 10 / Overall 10 — tighter and more precise.
  2. "All options and APIs keep working as before." — C 9 / N 9 / Overall 9 — still slightly loose.
  3. "All options and APIs work exactly as before." — C 9 / N 9 / Overall 9.5 — adds mild emphasis.
- **Decision:** Applied → "All options and APIs continue to work as before." (new Overall 10)
- **Why:** Adopts the documented tighter phrasing without changing meaning.

### [30] `docs/pages/tanstack-query/+Page.mdx` — Setup callout (line 37)
- **Original:** "The integration lives in `QueryClient` (the peer dependency is `@tanstack/query-core` v5+), so any TanStack Query adapter works: React, Vue, Svelte, Solid."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "lives in `QueryClient`" + a peer-dependency parenthetical + a colon list packs three ideas into one sentence, reading crowded.
- **Candidates:**
  1. "The integration lives in `QueryClient` — its peer dependency is `@tanstack/query-core` v5+ — so any TanStack Query adapter works: React, Vue, Svelte, Solid." — C 9 / N 8 / Overall 8.5 — em-dashes, but the same density remains.
  2. "Because the integration lives in `QueryClient` (peer dependency `@tanstack/query-core` v5+), any TanStack Query adapter works: ..." — C 9 / N 9 / Overall 9 — tightens cause/effect.
  3. "The integration lives in `QueryClient`, so any TanStack Query adapter works: React, Vue, Svelte, Solid. (The peer dependency is `@tanstack/query-core` v5+.)" — C 10 / N 9 / Overall 9.5 — moves the peer-dep aside to its own short sentence, relieving the crowding.
  4. "The integration lives in `QueryClient` (peer dependency `@tanstack/query-core` v5+), so any TanStack Query adapter works: ..." — C 9 / N 9 / Overall 9 — trims the parenthetical only.
- **Decision:** Applied → "The integration lives in `QueryClient`, so any TanStack Query adapter works: React, Vue, Svelte, Solid. (The peer dependency is `@tanstack/query-core` v5+.)" (new Overall 9.5)
- **Why:** Splitting the peer-dependency note into its own short sentence directly relieves the documented density while preserving every fact and the blockquote.

### [37] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 52, blockquote)
- **Original:** "A key is global when its *first* element is a string starting with `global:` — e.g. `['global:todos']` is global, while `['todos', 'global:x']` is local (its first element isn't)."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "(its first element isn't)" elides the predicate; the reader must reconstruct "isn't a string starting with `global:`."
- **Candidates:**
  1. "...is local (its first element isn't a `global:` string)." — C 10 / N 9 / Overall 9.5 — restores the predicate concisely.
  2. "...is local (its first element doesn't start with `global:`)." — C 10 / N 10 / Overall 10 — restores the predicate; mirrors the rule's wording naturally.
  3. "...is local (its first element isn't a string starting with `global:`)." — C 10 / N 8 / Overall 9 — full but verbatim-repeats the rule, reading heavy.
  4. "...is local — its first element doesn't start with `global:`." — C 9 / N 9 / Overall 9.5 — em-dash instead of paren.
- **Decision:** Applied → "...while `['todos', 'global:x']` is local (its first element doesn't start with `global:`)." (new Overall 10)
- **Why:** Restores the elided predicate clearly and concisely; *first* emphasis and inline code preserved.

### [41] `docs/pages/tanstack-query/+Page.mdx` — rule 1 (line 56)
- **Original:** "The same goes for mutations: global keys in `meta.invalidates` are published server-side after the telefunction succeeds, so `mutationFn` must be a telefunction call too."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — chains topic → mechanism → conclusion, so the reader holds three clauses; dense for one sentence.
- **Candidates:**
  1. "...published server-side once the telefunction succeeds, so `mutationFn` must be a telefunction call too." — C 8 / N 8 / Overall 8.5 — "once" for "after," negligible.
  2. "The same applies to mutations: their global keys in `meta.invalidates` are published server-side after the telefunction succeeds, so ..." — C 8 / N 8 / Overall 8.5 — minor.
  3. "Mutations work the same way: global keys in `meta.invalidates` are published server-side after the telefunction succeeds, so ..." — C 8 / N 8 / Overall 8.5 — echoes "work the same way" used at lines 86 and 140.
  4. "...are published server-side after the telefunction succeeds. So `mutationFn` must be a telefunction call too." — C 9 / N 8 / Overall 8.5 — sentence-initial "So" is a touch informal.
  5. "The same goes for mutations: global keys in `meta.invalidates` are published server-side after the telefunction succeeds — so `mutationFn` must be a telefunction call too." — C 9 / N 9 / Overall 9 — em-dash sets off the consequence, easing the three-clause chain while keeping the parallel with the `queryFn` bullet.
- **Decision:** Applied → "The same goes for mutations: global keys in `meta.invalidates` are published server-side after the telefunction succeeds — so `mutationFn` must be a telefunction call too." (new Overall 9)
- **Why:** The em-dash visually separates the conclusion, lightening the clause-stacking without breaking the deliberate parallel structure with the preceding `queryFn` rule.

### [54] `docs/pages/tanstack-query/+Page.mdx` — Server-side invalidation (line 128)
- **Original:** "Prefix matching — invalidating `['global:documents']` matches `['global:documents', docId]` too."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "Prefix matching —" is a label-style fragment fronting the sentence, clipped for body prose.
- **Candidates:**
  1. "Invalidation is prefix-based: invalidating `['global:documents']` matches `['global:documents', docId]` too." — C 10 / N 9 / Overall 9.5 — full clause; colon introduces the concrete example; avoids "match...matches" repetition.
  2. "Matching is prefix-based — invalidating `['global:documents']` matches `['global:documents', docId]` too." — C 9 / N 9 / Overall 9.5 — gives the fronted label a verb, keeps the em-dash.
  3. "Keys match by prefix: invalidating `['global:documents']` matches ... too." — C 9 / N 8 / Overall 8.5 — "match ... matches" repeats.
  4. "Matching is by prefix — invalidating `['global:documents']` matches ... too." — C 9 / N 9 / Overall 9.5 — verb plus em-dash.
- **Decision:** Applied → "Invalidation is prefix-based: invalidating `['global:documents']` matches `['global:documents', docId]` too." (new Overall 9.5)
- **Why:** Converts the label fragment into a full sentence and uses a colon to introduce the example; inline code preserved.

### [55] `docs/pages/tanstack-query/+Page.mdx` — Server-side invalidation (line 128)
- **Original:** "Same behavior as TanStack Query's [`invalidateQueries`](https://tanstack.com/query/latest/docs/framework/react/guides/query-keys)."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the verbless fragment "Same behavior as ..." reads as a clipped note rather than a full sentence.
- **Candidates:**
  1. "This is the same behavior as TanStack Query's [`invalidateQueries`](...)." — C 10 / N 10 / Overall 10 — adds subject and verb; clean cross-reference.
  2. "It behaves the same as TanStack Query's [`invalidateQueries`](...)." — C 9 / N 9 / Overall 9.5 — verb "behaves."
  3. "This matches the behavior of TanStack Query's [`invalidateQueries`](...)." — C 9 / N 8 / Overall 8.5 — "matches" collides with the prefix-matching sentence just before it.
- **Decision:** Applied → "This is the same behavior as TanStack Query's [`invalidateQueries`](https://tanstack.com/query/latest/docs/framework/react/guides/query-keys)." (new Overall 10)
- **Why:** Promotes the fragment to a full sentence; the external markdown link and URL are preserved verbatim.

### [56] `docs/pages/tanstack-query/+Page.mdx` — How it works, Local keys step 1 (line 134)
- **Original:** "Mutation succeeds."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — article-less "Mutation succeeds" is telegraphic; acceptable as a step but clipped.
- **Candidates:**
  1. "The mutation succeeds." — C 10 / N 10 / Overall 10 — adds the article; matches the full-sentence style of the other steps.
  2. "A mutation succeeds." — C 9 / N 9 / Overall 9 — indefinite, slightly less specific.
  3. "The mutation completes successfully." — C 9 / N 8 / Overall 8.5 — wordier.
- **Decision:** Applied → "The mutation succeeds." (new Overall 10)
- **Why:** Restoring the article aligns step 1 with the full-sentence steps that follow.

### [62] `docs/pages/tanstack-query/+Page.mdx` — callout (line 143)
- **Original:** "Works across multiple servers via a <Link text="broadcast transport" href="/channel#multi-server" /> such as <Link href="/redis" text={<code>@telefunc/redis</code>} />."
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — the subjectless opener "Works across multiple servers ..." is a clipped fragment.
- **Candidates:**
  1. "This works across multiple servers via a <Link.../> such as <Link.../>." — C 10 / N 9 / Overall 9.5 — adds "This" subject, though slightly vague.
  2. "Invalidation works across multiple servers via a <Link.../> such as <Link.../>." — C 10 / N 10 / Overall 10 — names the mechanism explicitly; full body prose.
  3. "It works across multiple servers via a <Link.../> such as <Link.../>." — C 9 / N 9 / Overall 9.5 — "It" subject, ambiguous referent.
- **Decision:** Applied → "Invalidation works across multiple servers via a <Link text="broadcast transport" href="/channel#multi-server" /> such as <Link href="/redis" text={<code>@telefunc/redis</code>} />." (new Overall 10)
- **Why:** An explicit subject ("Invalidation") is clearer than a pronoun and turns the fragment into a full sentence; both `<Link/>` components and the `<code>` JSX are preserved exactly.

## Summary

- **Targets:** 15 (7 in `rxjs/+Page.mdx`, 8 in `tanstack-query/+Page.mdx`)
- **Applied:** 15
- **Retained:** 0
- **New score distribution (Overall):** 10 → 6 targets ([6], [7], [29], [37], [55], [56], [62] — note 7 entries reach 10); 9.5 → 6 targets ([2], [11], [12], [14], [30], [54]); 9 → 2 targets ([8], [41]).
  - Precisely: Overall 10 → [6], [7], [29], [37], [55], [56], [62] (7); Overall 9.5 → [2], [11], [12], [14], [30], [54] (6); Overall 9 → [8], [41] (2).
- **Nature of fixes:** restored dropped articles ([6], [56]), promoted verbless/label/subjectless fragments to full sentences ([54], [55], [62]), integrated bolted-on adverbials/appositives ([2], [7], [11]), de-stacked clauses with em-dashes or sentence splits ([12], [14], [30], [41]), restored an elided predicate ([37]), reduced front-loading and fixed an awkward verb ([8]), and tightened a loose verb phrase ([29]). All MDX/JSX (`<Link/>`, `<code>`, blockquotes, emphasis, inline code, the external markdown link) confirmed intact via read-back.
