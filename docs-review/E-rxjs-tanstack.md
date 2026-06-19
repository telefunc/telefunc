# Sentence review — RxJS & TanStack Query integration docs (PR #264)

Reviewer E. Both files are NEW (no `/tmp/diffs/` entry), so every prose sentence is in scope.
Bare component tags (`<StreamingBeta />`, `<PoweredByStreaming .../>`) render prose from
separate component files owned by other reviewers and are skipped here. External bare-markdown
links (`[RxJS]`, `[TanStack Query]`, `[invalidateQueries]`) are allowed — the "internal links
must use `<Link>`" rule applies only to internal links.

---

## `docs/pages/rxjs/+Page.mdx`

### [1] `docs/pages/rxjs/+Page.mdx` — intro (line 6)
- **Original:** "For reactive streams with full operator support, you can use [RxJS](https://rxjs.dev)."
- **Clarity:** 9/10 — "full operator support" is slightly marketing-flavored but unambiguous in an RxJS context.
- **Naturalness:** 9/10 — reads naturally; "you can use" is a touch soft for a lead sentence but standard.
- **Overall:** 9/10 — solid intro; not flawless because the lead-in clause front-loads the benefit before naming the subject.
- **Action:** Kept

### [2] `docs/pages/rxjs/+Page.mdx` — intro (line 8)
- **Original:** "The `@telefunc/rxjs` integration lets you pass RxJS `Observable` and `Subject` directly between client and server — both directions, all operators."
- **Clarity:** 9/10 — clear; the trailing fragment "both directions, all operators" is a punchy appositive that reads as emphasis rather than a precise claim.
- **Naturalness:** 8/10 — the clipped fragment after the em-dash is in Telefunc's voice but slightly more promotional than typical reference prose.
- **Overall:** 8/10 — effective and clear, but the appended fragment keeps it from flawless.
- **Action:** Kept

### [3] `docs/pages/rxjs/+Page.mdx` — intro callout (line 10)
- **Original:** "Values you stream to the server are **shield-validated at runtime** against your TypeScript types — see <Link href="#shields" />."
- **Clarity:** 9/10 — precise; "shield-validated" is project terminology a first-time reader meets here, but the link resolves it.
- **Naturalness:** 9/10 — natural; the trailing "— see <Link/>" is a common Telefunc pattern.
- **Overall:** 9/10 — strong; only the as-yet-undefined term "shield-validated" stops it short of 10.
- **Action:** Kept

### [4] `docs/pages/rxjs/+Page.mdx` — Setup (line 22)
- **Original:** "The Telefunc bundler plugin (Vite, webpack, Next.js, Babel) detects `@telefunc/rxjs` in your dependencies and registers it automatically."
- **Clarity:** 9/10 — clear; the parenthetical list of supported bundlers is helpful and unambiguous.
- **Naturalness:** 9/10 — reads naturally as setup prose.
- **Overall:** 9/10 — very good; the long inline parenthetical is the only minor drag on flow.
- **Action:** Kept

### [5] `docs/pages/rxjs/+Page.mdx` — Setup (line 22)
- **Original:** "Without a Telefunc bundler plugin, register it manually — `import '@telefunc/rxjs/server'` in your server entry and `import '@telefunc/rxjs/client'` in your client entry."
- **Clarity:** 9/10 — unambiguous instructions with explicit import targets.
- **Naturalness:** 9/10 — natural; the em-dash introducing the two imports fits the voice.
- **Overall:** 9/10 — clear and complete; mild density from the two inline `import` snippets keeps it from 10.
- **Action:** Kept

### [6] `docs/pages/rxjs/+Page.mdx` — Live stock ticker (line 27)
- **Original:** "Server pushes prices every second."
- **Clarity:** 9/10 — clear; the article-less "Server" is telegraphic but understood.
- **Naturalness:** 8/10 — dropping the article ("Server pushes" vs "The server pushes") reads as headline-style and is slightly clipped for body prose.
- **Overall:** 8/10 — fine and concise, but the missing article makes it read a hair terse.
- **Action:** Kept

### [7] `docs/pages/rxjs/+Page.mdx` — Live stock ticker (line 27)
- **Original:** "The client filters and limits them locally, with standard RxJS operators."
- **Clarity:** 9/10 — clear; "limits them" maps cleanly to the `take(10)` in the example.
- **Naturalness:** 8/10 — the comma before "with standard RxJS operators" makes the trailing phrase feel slightly bolted on rather than integrated.
- **Overall:** 8/10 — clear; the appended adverbial keeps it from flawless.
- **Action:** Kept

### [8] `docs/pages/rxjs/+Page.mdx` — Collaborative editor (line 58)
- **Original:** "A shared `Subject` returned to multiple clients multicasts across them: when one client emits with `next()`, every other client's subscribers receive the value through the server."
- **Clarity:** 8/10 — accurate but dense; "A shared `Subject` returned to multiple clients multicasts across them" stacks a heavy subject before the verb, so the reader parses it twice.
- **Naturalness:** 8/10 — "multicasts across them" is technically fine but a slightly unusual verb-phrasing.
- **Overall:** 8/10 — correct and informative; the front-loaded noun phrase and the unusual verb keep it below 9.
- **Action:** Kept

### [9] `docs/pages/rxjs/+Page.mdx` — Collaborative editor (line 58)
- **Original:** "The emitting client's own subscribers receive it locally too — the server doesn't echo a client's `next()` back to that same client."
- **Clarity:** 9/10 — precisely disambiguates the echo behavior, which is exactly the subtle point a reader would wonder about.
- **Naturalness:** 9/10 — natural; the em-dash clarification is well-formed.
- **Overall:** 9/10 — strong; very slight redundancy between "own subscribers ... locally" and "doesn't echo ... back" keeps it from 10.
- **Action:** Kept

### [10] `docs/pages/rxjs/+Page.mdx` — Collaborative editor callout (line 83)
- **Original:** "A module-level `Subject` lives in one server process." (after the bold label "**Single server.**")
- **Clarity:** 9/10 — clear and concrete.
- **Naturalness:** 9/10 — natural reference prose.
- **Overall:** 9/10 — good; "lives in" is mildly informal, the only thing short of flawless.
- **Action:** Kept

### [11] `docs/pages/rxjs/+Page.mdx` — Collaborative editor callout (line 83)
- **Original:** "These multicast examples (the editor above, and *Live cursors* below) work as-is on a single instance."
- **Clarity:** 9/10 — clear; the parenthetical pins down exactly which examples.
- **Naturalness:** 8/10 — the mid-sentence parenthetical with an internal comma ("the editor above, and *Live cursors* below") interrupts the flow a bit.
- **Overall:** 8/10 — clear and useful; the bulky parenthetical is the only weakness.
- **Action:** Kept

### [12] `docs/pages/rxjs/+Page.mdx` — Collaborative editor callout (line 83)
- **Original:** "Across multiple instances, each server process has its own `Subject` — so route shared state through a broadcast transport instead, see <Link href="/stream/scale" />."
- **Clarity:** 9/10 — the consequence ("so route ... instead") is clear and actionable.
- **Naturalness:** 8/10 — "so route ... instead, see <Link/>" chains an imperative onto a trailing "see X", which is slightly overloaded though consistent with Telefunc's voice.
- **Overall:** 8/10 — clear and in-voice; the comma-joined "instead, see" tail keeps it below 9.
- **Action:** Kept

### [13] `docs/pages/rxjs/+Page.mdx` — Click heatmap (line 89)
- **Original:** "Pass an Observable as a telefunction argument."
- **Clarity:** 9/10 — clear, imperative, unambiguous.
- **Naturalness:** 9/10 — natural how-to phrasing.
- **Overall:** 9/10 — crisp; "an Observable" unbackticked while other type names are inline-coded is a minor inconsistency, not a clarity issue.
- **Action:** Kept

### [14] `docs/pages/rxjs/+Page.mdx` — Click heatmap (line 89)
- **Original:** "The server subscribes and processes the stream — useful for telemetry, analytics, or any client-driven event stream."
- **Clarity:** 8/10 — clear, but "useful for ..." dangles slightly: it modifies the overall pattern rather than the immediately preceding "the stream."
- **Naturalness:** 8/10 — the dangling "useful for" clause is common in docs but a touch loose.
- **Overall:** 8/10 — informative; the loosely attached trailing clause keeps it from 9.
- **Action:** Kept

### [15] `docs/pages/rxjs/+Page.mdx` — Live cursors (line 120)
- **Original:** "A shared Subject multicasts cursor positions among all connected users."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — natural; "multicasts ... among ... users" reads fine.
- **Overall:** 9/10 — good; "Subject" is unbackticked here while inline-coded elsewhere — minor stylistic inconsistency only.
- **Action:** Kept

### [16] `docs/pages/rxjs/+Page.mdx` — Live cursors (line 120)
- **Original:** "The server attaches the user identity from context."
- **Clarity:** 9/10 — clear; "from context" maps to `getContext()` in the example.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — concise and clear; "the user identity" is very slightly formal vs "the user's identity," the only nit.
- **Action:** Kept

### [17] `docs/pages/rxjs/+Page.mdx` — Live cursors code comment (line 135)
- **Original:** "release the per-client subscription on disconnect"
- **Clarity:** 9/10 — clear comment explaining the `onClose` cleanup.
- **Naturalness:** 9/10 — fine for a code comment.
- **Overall:** 9/10 — good; no typo or grammar issue. (Code comment — flagged-only, no rating of code.)
- **Action:** Kept

### [18] `docs/pages/rxjs/+Page.mdx` — Angular (line 156)
- **Original:** "Angular uses RxJS extensively."
- **Clarity:** 9/10 — clear, factual.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — fine; a generic framing sentence, not flawless prose but does its job.
- **Action:** Kept

### [19] `docs/pages/rxjs/+Page.mdx` — Angular (line 156)
- **Original:** "With `@telefunc/rxjs`, telefunctions return Observables directly — pipe them into templates with `| async`, use them in services, or compose them with the rest of your RxJS code."
- **Clarity:** 9/10 — clear; the three options after the em-dash are concrete.
- **Naturalness:** 9/10 — natural Angular/RxJS phrasing.
- **Overall:** 9/10 — strong; it is a fairly long compound sentence, the only thing keeping it from 10.
- **Action:** Kept

### [20] `docs/pages/rxjs/+Page.mdx` — Angular (line 171)
- **Original:** "The `| async` pipe subscribes and auto-unsubscribes when the component is destroyed:"
- **Clarity:** 9/10 — clear and accurate.
- **Naturalness:** 9/10 — natural; "auto-unsubscribes" is idiomatic Angular phrasing.
- **Overall:** 9/10 — good; "subscribes ... when the component is destroyed" momentarily attaches "when ..." to both verbs (only the unsubscribe is destroy-triggered), a tiny ambiguity that keeps it from 10.
- **Action:** Kept

### [21] `docs/pages/rxjs/+Page.mdx` — Shields (line 198)
- **Original:** "When you pass a `Subject<T>` or `Observable<T>` as a telefunction argument, each `next(v)` value is shield-validated against `T` as it arrives at the server."
- **Clarity:** 9/10 — precise; the relationship between `next(v)`, `T`, and arrival timing is spelled out clearly.
- **Naturalness:** 9/10 — natural technical prose.
- **Overall:** 9/10 — strong; a long sentence with two time/condition clauses, the only minor drag.
- **Action:** Kept

### [22] `docs/pages/rxjs/+Page.mdx` — Shields (line 198)
- **Original:** "Terminal signals (`error`, `complete`) are untyped and pass through unshielded."
- **Clarity:** 9/10 — clear; "untyped" and "unshielded" are precise.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — good; relies on the reader knowing "terminal signals," but the inline examples cover it.
- **Action:** Kept

### [23] `docs/pages/rxjs/+Page.mdx` — Shields (line 200)
- **Original:** "See <Link href="/shield" /> for how telefunction arguments are validated in general."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — natural cross-reference phrasing.
- **Overall:** 9/10 — fine; "in general" is mildly filler but adds the intended contrast with the stream-specific case above.
- **Action:** Kept

### [24] `docs/pages/rxjs/+Page.mdx` — Lifecycle (line 205)
- **Original:** "Unsubscribing stops data flow immediately."
- **Clarity:** 9/10 — clear and direct.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — good; "data flow" is slightly generic but unambiguous in context.
- **Action:** Kept

### [25] `docs/pages/rxjs/+Page.mdx` — Lifecycle (line 205)
- **Original:** "The underlying <Link href="/channel" text="channel" /> is cleaned up automatically via GC, or immediately with <Link href="/close" text="close()" />."
- **Clarity:** 9/10 — clear; the GC-vs-`close()` contrast is explicit.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "via GC" abbreviates "garbage collection" without expansion, a tiny accessibility nit only.
- **Action:** Kept

---

## `docs/pages/tanstack-query/+Page.mdx`

### [26] `docs/pages/tanstack-query/+Page.mdx` — intro (line 6)
- **Original:** "Live queries for [TanStack Query](https://tanstack.com/query) — invalidate a query key on the server, and every connected client with a matching query refetches automatically."
- **Clarity:** 9/10 — clear; the mechanism (invalidate → matching clients refetch) is stated up front.
- **Naturalness:** 9/10 — natural; the headline-style "Live queries for X —" opener is in-voice.
- **Overall:** 9/10 — strong intro; it is a sentence fragment plus an independent clause joined by an em-dash, slightly informal for a lead, keeping it from 10.
- **Action:** Kept

### [27] `docs/pages/tanstack-query/+Page.mdx` — Setup (line 18)
- **Original:** "Wrap your `QueryClient` with `withTelefunc()`:"
- **Clarity:** 9/10 — clear, imperative.
- **Naturalness:** 9/10 — natural setup phrasing.
- **Overall:** 9/10 — crisp; a routine instruction sentence, not flawless prose but does its job.
- **Action:** Kept

### [28] `docs/pages/tanstack-query/+Page.mdx` — Setup (line 35)
- **Original:** "`withTelefunc()` returns the same `QueryClient` instance."
- **Clarity:** 9/10 — clear; "the same instance" precisely signals no wrapping/proxy object.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — good; concise and correct, only minor that "the same" lacks an explicit referent ("the one you passed in") though context supplies it.
- **Action:** Kept

### [29] `docs/pages/tanstack-query/+Page.mdx` — Setup (line 35)
- **Original:** "All options and APIs keep working unchanged."
- **Clarity:** 9/10 — clear reassurance.
- **Naturalness:** 8/10 — "keep working unchanged" is slightly informal/loose ("continue to work as before" would be tighter), but acceptable.
- **Overall:** 8/10 — clear; the slightly colloquial verb phrase keeps it from 9.
- **Action:** Kept

### [30] `docs/pages/tanstack-query/+Page.mdx` — Setup callout (line 37)
- **Original:** "The integration lives in `QueryClient` (the peer dependency is `@tanstack/query-core` v5+), so any TanStack Query adapter works: React, Vue, Svelte, Solid."
- **Clarity:** 9/10 — clear; the parenthetical justifies the "any adapter" claim well.
- **Naturalness:** 8/10 — "lives in `QueryClient`" plus a peer-dependency parenthetical plus a colon list packs three ideas into one sentence, which reads a bit crowded.
- **Overall:** 8/10 — informative and clear; the density of clauses is the only weakness.
- **Action:** Kept

### [31] `docs/pages/tanstack-query/+Page.mdx` — Setup callout (line 37)
- **Original:** "The examples above use React."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — fine; a small standalone clarifier, unremarkable but correct.
- **Action:** Kept

### [32] `docs/pages/tanstack-query/+Page.mdx` — Setup callout (line 39)
- **Original:** "There's no server-side setup: Telefunc finds `@telefunc/*` packages in your `package.json` and auto-loads their server extension."
- **Clarity:** 9/10 — clear; the colon links the claim to its mechanism.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "their server extension" (singular "extension" for plural "packages") is a slight number mismatch, the only nit.
- **Action:** Kept

### [33] `docs/pages/tanstack-query/+Page.mdx` — Local vs global keys (line 44)
- **Original:** "Keys prefixed with `global:` invalidate through the server — every connected client with a matching query refetches."
- **Clarity:** 9/10 — clear; "invalidate through the server" plus the consequence is precise.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; near-verbatim echo of the intro sentence, so it reads slightly repetitive in the full page, keeping it from 10.
- **Action:** Kept

### [34] `docs/pages/tanstack-query/+Page.mdx` — Local vs global keys (line 44)
- **Original:** "All other keys invalidate locally (current tab only)."
- **Clarity:** 9/10 — clear; the parenthetical scopes "locally" concretely.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — good; "current tab only" as a bare parenthetical is terse but unambiguous.
- **Action:** Kept

### [35] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 47)
- **Original:** "local — current tab only"
- **Clarity:** 9/10 — clear inline comment.
- **Naturalness:** 9/10 — fine for a comment.
- **Overall:** 9/10 — good; no typo/grammar issue. (Code comment — flagged-only.)
- **Action:** Kept

### [36] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 48)
- **Original:** "global — all clients with a matching query refetch"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — fine for a comment.
- **Overall:** 9/10 — good; consistent with the prose. (Code comment — flagged-only.)
- **Action:** Kept

### [37] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 52, blockquote)
- **Original:** "A key is global when its *first* element is a string starting with `global:` — e.g. `['global:todos']` is global, while `['todos', 'global:x']` is local (its first element isn't)."
- **Clarity:** 8/10 — mostly clear, but the closing "(its first element isn't)" elides the predicate ("isn't a string starting with `global:`"); the reader must reconstruct it.
- **Naturalness:** 8/10 — the elliptical parenthetical is concise but slightly clipped.
- **Overall:** 8/10 — the rule and both examples are clear; the elided parenthetical is the only weak spot.
- **Action:** Kept

### [38] `docs/pages/tanstack-query/+Page.mdx` — callout (line 54)
- **Original:** "Global keys are carried by Telefunc calls, so two rules apply:"
- **Clarity:** 9/10 — clear; sets up the two rules and gives the reason.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — good; "are carried by Telefunc calls" is slightly abstract, but the rules that follow make it concrete.
- **Action:** Kept

### [39] `docs/pages/tanstack-query/+Page.mdx` — rule 1 (line 56)
- **Original:** "The invalidation subscription piggybacks on that call." (after bold label "**`queryFn` must call a telefunction.**")
- **Clarity:** 9/10 — clear; "piggybacks on that call" precisely explains the coupling.
- **Naturalness:** 9/10 — natural; "piggybacks" is idiomatic technical English.
- **Overall:** 9/10 — strong; "that call" depends on the bold label's "a telefunction," a tiny referential reach, keeping it from 10.
- **Action:** Kept

### [40] `docs/pages/tanstack-query/+Page.mdx` — rule 1 (line 56)
- **Original:** "With a non-telefunc `queryFn` (e.g. plain `fetch()`), a global key silently behaves like a local one."
- **Clarity:** 9/10 — clear; "silently behaves like a local one" names the failure mode precisely.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "non-telefunc" as an adjective is mild jargon, the only nit.
- **Action:** Kept

### [41] `docs/pages/tanstack-query/+Page.mdx` — rule 1 (line 56)
- **Original:** "The same goes for mutations: global keys in `meta.invalidates` are published server-side after the telefunction succeeds, so `mutationFn` must be a telefunction call too."
- **Clarity:** 8/10 — accurate but long; it chains "the same goes for mutations" → mechanism → conclusion, so the reader holds three clauses to reach "must be a telefunction call too."
- **Naturalness:** 8/10 — natural but dense for a single sentence.
- **Overall:** 8/10 — correct and complete; the length/clause-stacking is the only weakness.
- **Action:** Kept

### [42] `docs/pages/tanstack-query/+Page.mdx` — rule 2 (line 57)
- **Original:** "The integration reads the invalidation subscription from whatever `queryFn` returns, so transform the result with `select` instead of inside `queryFn`:" (after bold label "**Return the telefunction call directly.**")
- **Clarity:** 9/10 — clear; the "so transform ... instead of inside `queryFn`" is precise and actionable.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "from whatever `queryFn` returns" is slightly informal ("whatever"), the only nit.
- **Action:** Kept

### [43] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 59)
- **Original:** "✗ Broken: inside queryFn, getTodos() resolves to an internal wrapper"
- **Clarity:** 9/10 — clear; explains why the pattern is broken.
- **Naturalness:** 9/10 — fine for a code comment.
- **Overall:** 9/10 — good; no typo/grammar issue. (Code comment — flagged-only.)
- **Action:** Kept

### [44] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 62)
- **Original:** "✓ Return the call directly, transform with select"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — fine for a code comment.
- **Overall:** 9/10 — good; matches the prose rule. (Code comment — flagged-only.)
- **Action:** Kept

### [45] `docs/pages/tanstack-query/+Page.mdx` — Mutations (line 70)
- **Original:** "Use `meta.invalidates` on mutations to invalidate matching queries after the mutation succeeds."
- **Clarity:** 9/10 — clear; states what, where, and when.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "invalidate ... after the mutation succeeds" repeats "mutations/mutation" closely, a tiny echo keeping it from 10.
- **Action:** Kept

### [46] `docs/pages/tanstack-query/+Page.mdx` — Mutations callout (line 72)
- **Original:** "The `invalidates` field is a `@telefunc/tanstack-query` convention: TanStack Query itself attaches no behavior to `meta`."
- **Clarity:** 9/10 — clear; the colon contrasts the convention with stock TanStack Query behavior.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "attaches no behavior to `meta`" is precise but slightly abstract, the only nit.
- **Action:** Kept

### [47] `docs/pages/tanstack-query/+Page.mdx` — Mutations (line 86)
- **Original:** "Global keys work the same way — when a collaborator edits a document, every client viewing it refetches:"
- **Clarity:** 9/10 — clear; the concrete collaborator example grounds the claim.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "work the same way" leans on the preceding example for its referent, a small dependency keeping it from 10.
- **Action:** Kept

### [48] `docs/pages/tanstack-query/+Page.mdx` — Mutations (line 100)
- **Original:** "Local and global keys can be mixed in a single mutation:"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — fine; routine lead-in to a code block, unremarkable but correct.
- **Action:** Kept

### [49] `docs/pages/tanstack-query/+Page.mdx` — code comments (lines 104, 105)
- **Original:** "local — this client only" / "global — every connected client"
- **Clarity:** 9/10 — clear inline comments.
- **Naturalness:** 9/10 — fine for comments.
- **Overall:** 9/10 — good; consistent with the prose, no issues. (Code comments — flagged-only.)
- **Action:** Kept

### [50] `docs/pages/tanstack-query/+Page.mdx` — Server-side invalidation (line 114)
- **Original:** "For changes not triggered by a client mutation (background jobs, webhooks, other services), use `invalidate()` directly."
- **Clarity:** 9/10 — clear; the parenthetical enumerates the non-client triggers concretely.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; the mid-sentence parenthetical list slightly delays the main instruction, keeping it from 10.
- **Action:** Kept

### [51] `docs/pages/tanstack-query/+Page.mdx` — Server-side invalidation (line 114)
- **Original:** "This only works with global keys since it broadcasts to all clients:"
- **Clarity:** 9/10 — clear; "since it broadcasts to all clients" gives the rationale.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — good; "This" refers back to `invalidate()`, a small referential reach, the only nit.
- **Action:** Kept

### [52] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 121)
- **Original:** "e.g. a CMS publishes new content"
- **Clarity:** 9/10 — clear example comment.
- **Naturalness:** 9/10 — fine for a comment.
- **Overall:** 9/10 — good; no issue. (Code comment — flagged-only.)
- **Action:** Kept

### [53] `docs/pages/tanstack-query/+Page.mdx` — code comment (line 124)
- **Original:** "a specific document was updated"
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — fine for a comment.
- **Overall:** 9/10 — good; no issue. (Code comment — flagged-only.)
- **Action:** Kept

### [54] `docs/pages/tanstack-query/+Page.mdx` — Server-side invalidation (line 128)
- **Original:** "Prefix matching — invalidating `['global:documents']` matches `['global:documents', docId]` too."
- **Clarity:** 9/10 — clear; the example makes the prefix-matching rule concrete.
- **Naturalness:** 8/10 — "Prefix matching —" is a label-style fragment fronting the sentence, slightly clipped for body prose.
- **Overall:** 8/10 — clear and concrete; the fronted fragment keeps it from 9.
- **Action:** Kept

### [55] `docs/pages/tanstack-query/+Page.mdx` — Server-side invalidation (line 128)
- **Original:** "Same behavior as TanStack Query's [`invalidateQueries`](https://tanstack.com/query/latest/docs/framework/react/guides/query-keys)."
- **Clarity:** 9/10 — clear cross-reference.
- **Naturalness:** 8/10 — verbless fragment ("Same behavior as ...") reads as a clipped note rather than a full sentence.
- **Overall:** 8/10 — clear and useful; the fragment form keeps it from 9.
- **Action:** Kept

### [56] `docs/pages/tanstack-query/+Page.mdx` — How it works, Local keys step 1 (line 134)
- **Original:** "Mutation succeeds."
- **Clarity:** 9/10 — clear in a numbered sequence.
- **Naturalness:** 8/10 — article-less "Mutation succeeds" is telegraphic; acceptable as a numbered step but clipped.
- **Overall:** 8/10 — fine for a step list; the missing article is the only nit.
- **Action:** Kept

### [57] `docs/pages/tanstack-query/+Page.mdx` — How it works, Local keys step 2 (line 135)
- **Original:** "`withTelefunc()` calls `invalidateQueries()` on the current client for matching local keys."
- **Clarity:** 9/10 — clear and precise.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "on the current client" is precise, no real weakness beyond ordinary density.
- **Action:** Kept

### [58] `docs/pages/tanstack-query/+Page.mdx` — How it works, Global keys step 1 (line 138)
- **Original:** "`withTelefunc()` sends each global query's `queryKey` alongside the telefunc call."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "the telefunc call" (vs "telefunction call" used elsewhere) is a minor term inconsistency, the only nit.
- **Action:** Kept

### [59] `docs/pages/tanstack-query/+Page.mdx` — How it works, Global keys step 2 (line 139)
- **Original:** "The telefunction executes (including auth). Only after it succeeds, the server subscribes to invalidation events for that key."
- **Clarity:** 8/10 — clear, but the second sentence's fronted "Only after it succeeds," with a following comma reads slightly stilted.
- **Naturalness:** 7/10 — "Only after it succeeds, the server subscribes ..." is grammatical but awkward; end-weighting the qualifier reads more naturally.
- **Overall:** 7/10 — the timing point is clear, but the inverted/fronted "Only after ..." phrasing is awkward enough to warrant an edit.
- **Action:** Edited
- **Edit:** "The telefunction executes (including auth). The server subscribes to invalidation events for that key only after it succeeds."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — end-weighting "only after it succeeds" reads naturally and keeps the timing unambiguous; still a touch dense, so not 10.

### [60] `docs/pages/tanstack-query/+Page.mdx` — How it works, Global keys step 3 (line 140)
- **Original:** "Mutations work the same way — the telefunction runs first, then global keys from `meta.invalidates` are published through <Link text={<code>Broadcast</code>} href="/channel#broadcast" />."
- **Clarity:** 9/10 — clear; "runs first, then ... published" gives the order explicitly.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "work the same way" again leans on the prior steps for its referent, a small dependency keeping it from 10.
- **Action:** Kept

### [61] `docs/pages/tanstack-query/+Page.mdx` — How it works, Global keys step 4 (line 141)
- **Original:** "Every connected client with a matching subscription calls `invalidateQueries()`."
- **Clarity:** 9/10 — clear.
- **Naturalness:** 9/10 — natural.
- **Overall:** 9/10 — strong; "matching subscription" (vs "matching query" used earlier) is a slight term shift, the only nit.
- **Action:** Kept

### [62] `docs/pages/tanstack-query/+Page.mdx` — callout (line 143)
- **Original:** "Works across multiple servers via a <Link text="broadcast transport" href="/channel#multi-server" /> such as <Link href="/redis" text={<code>@telefunc/redis</code>} />."
- **Clarity:** 9/10 — clear; gives the requirement and a concrete example.
- **Naturalness:** 8/10 — the subjectless opener "Works across multiple servers ..." is a clipped fragment (the implied subject is the whole mechanism), slightly informal.
- **Overall:** 8/10 — clear and useful; the subjectless fragment keeps it from 9.
- **Action:** Kept

---

## Summary

- **Sentences reviewed:** 62 (including 10 code-comment units flagged but not edited)
- **Kept:** 61
- **Edited:** 1 (entry [59], `tanstack-query/+Page.mdx`, line 139 — fronted "Only after it succeeds," → end-weighted "only after it succeeds")
- **Second-PR candidates:** 0

No typos or grammatical errors were found in any code comment. No MDX/JSX, inline code, emphasis,
anchors, links, or code logic were altered; the single edit changed prose word order only. The edited
region was re-read after editing and confirmed intact.
