# Review report — small/modified files (reviewer I)

Scope: only sentences introduced or changed by PR #264 on the assigned files.
Many of these diffs are **code-block-only** changes (import/usage updates for the new
streaming API). Per the methodology, code is not rated; code comments are scanned for
typos/unclear text. Where a diff only changed link *markup* (not the prose words), the
underlying sentence is pre-existing and is not re-rated.

---

## `docs/pages/RPC/+Page.mdx`

Diff region: the "On the server-side, the Telefunc middleware" code block (lines ~194–207).

The diff is **entirely inside a code block** — it swaps `telefunc(req)` for
`serve({ request })` and updates the `Response` construction. The comments in the changed
region (`// server.js`, `// Server (Express.js/Hono/Fastify/...)`, `// Telefunc middleware`)
are pre-existing context lines (no `+`), and the surrounding prose ("Replies following HTTP
response:", "In other words, …") is untouched.

**No introduced prose sentences. No new or changed code comments. Nothing to rate.**

---

## `docs/pages/error-handling/+Page.mdx`

Two diff regions, both **code blocks**:
1. Server middleware block (lines ~114–127): swaps `telefunc()` for `serve({ request: c.req.raw })`; adds the annotation `// Environment: server`.
2. Network-errors HTML block (lines ~136–159): adds `import { NetworkError } from 'telefunc/client'` and changes `err.isConnectionError` to `err instanceof NetworkError`.

The added `// Environment: server` line is a code annotation (a documented convention, see
CONTRIBUTING.md), not prose. All explanatory comments in the changed blocks
(`// Telefunc exposes any error thrown by a telefunction at httpResponse.err`,
`// There is a network problem:` …, `// Prints 'No Server Connection'`) are pre-existing
context lines and are correct/clear.

**No introduced prose sentences. Code comments scanned — no typos or unclear text. Nothing to rate.**

---

## `docs/pages/next/+Page.mdx`

Diff is **entirely inside the route-handler code block** (lines ~29–45): switches to
`new Telefunc()` from `telefunc/node` and `telefunc.serve({ request })`, returning
`response ?? new Response('Not found', { status: 404 })`. The comment lines
(`// app/api/telefunc/route.ts`, `// Environment: server`) are pre-existing context.

**No introduced prose sentences. Nothing to rate.**

---

## `docs/pages/svelte-kit/+Page.mdx`

Diff is **entirely inside the `+server.ts` code block** (lines ~29–43): switches to
`new Telefunc()` from `telefunc/node` and `telefunc.serve(...)` with a `?? new Response('Not found', …)`
fallback. Comment lines are pre-existing context.

**No introduced prose sentences. Nothing to rate.**

---

## `docs/pages/vike/+Page.mdx` — section "3. Server integration"

### [1] `docs/pages/vike/+Page.mdx` — "Server integration", code-block intro
- **Original:** "Install the Telefunc server middleware. For example with Hono:"
- **Clarity:** 9/10 — unambiguous; "the Telefunc server middleware" is a known concept and the line clearly introduces the example below.
- **Naturalness:** 7/10 — "For example with Hono:" is missing the comma that American English expects after an introductory "For example", making it read clipped.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Install the Telefunc server middleware. For example, with Hono:"
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — the comma fixes the abruptness; matches the common docs pattern "For example, with <framework>:". Not 10 because it remains a deliberately terse two-part intro line.

> Note: the old line linked `<Link href="/telefunc">`telefunc()`</Link>`. The PR's removal of
> that link/`telefunc()` reference is a deliberate API change (the page now uses
> `Telefunc`/`serve`), not a prose defect, so the rewording itself is what is reviewed above.

---

## `docs/pages/shield/+Page.mdx`

### [2] `docs/pages/shield/+Page.mdx` — new blockquote after the intro paragraph
- **Original:** "Telefunc guarantees that **every** value arriving at the server is validated — not just a telefunction's top-level arguments, but also every value the client sends through a <Link href="/stream" text="streaming primitive" /> (callbacks, channels, streams)."
- **Clarity:** 9/10 — the claim is strong and unambiguous, and the parenthetical "(callbacks, channels, streams)" pins down what "streaming primitive" means. Slight density from the "not just …, but also …" construction plus the parenthetical, but the reader never has to guess.
- **Naturalness:** 9/10 — reads like idiomatic Telefunc prose; the em-dash and bold emphasis match the house voice. Just shy of 10 because it is a fairly long, multi-clause sentence.
- **Overall:** 9/10
- **Action:** Kept

The new `## See also` is a plain heading (no custom `{#anchor}`) and `- <Link href="/permissions" />`
is a bare component link with no prose — both skipped per methodology.

---

## `docs/pages/permissions/+Page.mdx`

### [3] `docs/pages/permissions/+Page.mdx` — new blockquote after the `TodoItem` example
- **Original:** "You can use the same `getContext()` + `throw Abort()` pattern <Link href="/stream#authorization">to protect streams</Link>."
- **Clarity:** 8/10 — meaning is clear (re-use the same pattern shown above), but the `+` joining two inline-code tokens reads like code shorthand rather than prose and momentarily looks like an operator.
- **Naturalness:** 7/10 — a literal `+` between two code calls is unusual for typed-out documentation prose; English would use "and".
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "You can use the same `getContext()` and `throw Abort()` pattern <Link href="/stream#authorization">to protect streams</Link>."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — "and" is the natural conjunction and preserves the "combination of both" meaning exactly; inline code and `<Link>` untouched. Not 10 because naming two API calls inline before "pattern" is still slightly dense.

---

## `docs/pages/RPC-vs-GraphQL-REST/+Page.mdx`

Diff changes a markdown link to a `<Link>` component **only**:
`[Telefunc Server](#telefunc-server)` → `<Link text="Telefunc Server" href="#telefunc-server" />`.
The prose words are identical to the pre-existing sentence
("You can develop multiple frontends as well with RPC by having one Telefunc Server per
frontend."), so per the methodology this is a pre-existing sentence and is **not re-rated**.
The markup change itself is correct (internal links must use `<Link>`).

**No introduced prose to rate.**

---

## `docs/pages/fetch/+Page.mdx`

Diff changes a "See also" link target only: `<Link href="/httpHeaders" />` → `<Link href="/headers" />`.
Bare component link, no prose.

**No introduced prose sentences. Nothing to rate.**

---

## `docs/pages/vite-plugin/+Page.mdx`

Diff changes a `<Link>` `href` only (`/telefunc` → `/serve`); the bullet's prose
("Automatically adds the Telefunc Server Middleware to Vite's development server as well as
Vite's preview server.") is unchanged pre-existing text.

**No introduced prose to rate** (markup-only change).

---

## `docs/pages/onBug/+Page.mdx`

Pure deletion: removes the `- <Link href="/error-handling#error-tracking" />` "See also" entry.

**No introduced sentences.**

---

## `CONTRIBUTING.md` — new "3. Conventions" block

`**3. Conventions**` is a bold section label (consistent with `**2. Develop**`), not a prose
sentence — skipped.

### [4] `CONTRIBUTING.md` — list intro
- **Original:** "Enforced by `pnpm run docs:lint`:"
- **Clarity:** 9/10 — clearly states the following rules are lint-enforced.
- **Naturalness:** 8/10 — a bare "Enforced by X:" fragment is idiomatic as a list lead-in but reads slightly clipped.
- **Overall:** 8/10
- **Action:** Kept

### [5] `CONTRIBUTING.md` — conventions bullet (links)
- **Original:** "Internal links use `<Link href="/foo" />`, never markdown `[…](/foo)`."
- **Clarity:** 9/10 — unambiguous rule with a positive and a negative example; the `[…]` ellipsis placeholder is obvious.
- **Naturalness:** 9/10 — reads like a normal style-guide rule. Not 10 only because "never markdown `[…](/foo)`" is a terse appositive.
- **Overall:** 9/10
- **Action:** Kept

### [6] `CONTRIBUTING.md` — conventions bullet (anchors)
- **Original:** "Every `#anchor` link resolves to a real heading."
- **Clarity:** 9/10 — clear requirement that anchor links must point to existing headings.
- **Naturalness:** 9/10 — natural. Slightly descriptive ("resolves to") rather than imperative, but consistent with the sibling bullet.
- **Overall:** 9/10
- **Action:** Kept

### [7] `CONTRIBUTING.md` — transition label
- **Original:** "Also:"
- **Clarity:** 9/10 — clearly introduces a second (non-lint-enforced) list.
- **Naturalness:** 8/10 — a one-word "Also:" header is slightly abrupt, though acceptable in a terse contributing guide.
- **Overall:** 8/10
- **Action:** Kept

### [8] `CONTRIBUTING.md` — conventions bullet (environment annotations)
- **Original:** "Annotate code with `// Environment: server` / `// Environment: client`; single-environment pages also carry a page-level `**Environment**: server|client`."
- **Clarity:** 7/10 — the ` / ` between the two comment forms is mildly ambiguous (one-or-the-other vs. both), and two distinct rules are packed into one semicolon-joined sentence.
- **Naturalness:** 7/10 — the slash between two code annotations reads compressed for prose.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Annotate code with `// Environment: server` or `// Environment: client`; single-environment pages also carry a page-level `**Environment**: server|client`."
- **Edit rating:** Clarity 8/10, Naturalness 8/10, Overall 8/10 — "or" makes the alternative explicit while keeping every code token intact. Not higher because it is still a compound bullet joining two rules with a semicolon (kept faithful to the original structure). The literal `**Environment**: server|client` token keeps its own `|` because it is a verbatim example.

### [9] `CONTRIBUTING.md` — conventions bullet (callouts)
- **Original:** "Match the callout to the note: `<Warning>` for must-heed / security, `<Advanced>` for a skippable deep-dive, and a plain `>` blockquote for ordinary notes."
- **Clarity:** 8/10 — the three-way mapping is clear, but "must-heed / security" with a bare slash is mildly fuzzy (and/or?).
- **Naturalness:** 7/10 — "must-heed / security" reads awkwardly; the slash plus the unusual "must-heed" compound make it the least natural part of the bullet.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "Match the callout to the note: `<Warning>` for must-heed or security notes, `<Advanced>` for a skippable deep-dive, and a plain `>` blockquote for ordinary notes."
- **Edit rating:** Clarity 8/10, Naturalness 8/10, Overall 8/10 — "or" disambiguates the slash and "… notes" parallels the closing "ordinary notes". "must-heed" is preserved to avoid changing meaning (heed = act on, which matches `<Warning>` semantics), so it is not a perfect 10.

---

## Summary

- **Sentences reviewed:** 9 prose units (plus 7 code-block-only / markup-only / deletion diffs that contributed no rateable prose).
- **Kept:** 5 (shield blockquote; CONTRIBUTING "Enforced by…", links bullet, anchors bullet, "Also:").
- **Edited:** 4 (vike intro comma; permissions `+`→"and"; CONTRIBUTING environment-annotation slash→"or"; CONTRIBUTING callout "must-heed / security"→"must-heed or security notes").
- **Second-PR candidates:** 0.

All four edits reached Overall ≥ 8 on first attempt, so no alternative-wording rounds were
needed and there are no second-PR candidates. Edited regions were re-read; all inline code,
`<Link>` components, and the `**Environment**: server|client` literal remain intact.
