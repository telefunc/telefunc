# Telefunc Stream — Documentation Audit

A 360° technical-writing audit of the new docs introduced by PR
[#264 (`feat: stream`)](https://github.com/telefunc/telefunc/pull/264).

## Scope

17 new pages plus supporting infrastructure (nav, redirects, components, a link
checker):

- **Concept / hub**: `stream`
- **Reference**: `channel`, `channel-config`, `transport`, `onClose`, `close`,
  `withContext`, `serve`, `Telefunc`, `provideTelefuncContext`
- **How-to / guides**: `testing`, `file-download`, `stream/scale`,
  `stream/cloudflare`, `redis`
- **Integrations**: `tanstack-query`, `rxjs`

---

## State-of-the-art principles audited against

1. **Diátaxis separation** — concept vs. reference vs. how-to vs. explanation are
   distinct modes, not blended on one page.
2. **Progressive disclosure** — simple example first, full/advanced example after;
   a hub page that fans out to deep dives.
3. **Minimalism / action-orientation** (Carroll) — get the reader doing the task
   fast, no filler.
4. **Explain the *why*, not just the *how*** — rationale for constraints and
   defaults.
5. **Show, don't tell** — complete, copy-pasteable, *runnable* examples.
6. **Accuracy above all** — in docs a broken example is a P1; examples must
   actually run.
7. **Consistency** — uniform terminology, code-block conventions, page skeleton.
8. **Scannability** — headings, decision tables, bold leads, callouts (F-pattern).
9. **Appropriate altitude / audience awareness** — link out for platform
   primitives, don't re-teach JS.
10. **DRY / single source of truth** — link rather than duplicate.
11. **Navigability & discoverability** — cross-links, "See also", nav hierarchy.
12. **Plain, active, present-tense language.**
13. **Honest signposting of maturity** — beta status, limitations, caveats.
14. **Docs-as-code** — CI lint gates, redirects for moved/renamed content.
15. **Failure-mode & edge-case coverage** — errors, recovery, security.

---

## Audit findings

### Strengths

- **Code-block convention is excellent and uniform** — every snippet leads with
  a filename + `// Environment: server|client` comment. Orients the reader
  instantly to *where* code runs, which is the core mental model of an RPC lib.
- **Progressive disclosure done well** — `/stream` is a true hub (primitives →
  integrations → DX), and primitive pages go *countdown* → *AI chat*
  (simple → full). `channel` opens with the minimal clock before types/acks/binary.
- **Strong Diátaxis instincts** — concept (`stream`), reference (`channel`,
  `transport`, `channel-config`), and how-to (`testing`, `file-download`, `scale`,
  `cloudflare`) are cleanly separated.
- **Decision-support tables are a highlight** — transport trade-offs, `Channel`
  vs `BroadcastChannel`, reading/streaming strategies, delivery guarantees,
  error/recovery matrix. Scannable and genuinely decision-oriented.
- **Rationale is consistently given** — "otherwise it leaks", "otherwise it keeps
  burning AI tokens", the Cloudflare stateless/Durable-Object explanation. Teaches
  the *why*.
- **Maturity is signposted** — the `TelefuncStreamBeta` callout sets expectations
  up front on every stream page; `<Advanced>` and `Limitations` sections are honest.
- **Security surfaced proactively** — "Keys are capabilities" warning on broadcast,
  authorization-at-open-time, the TanStack "broadcast only a refetch signal" pattern.
- **Docs-as-code quality gate** — the PR ships `check-docs.mjs` enforcing anchor
  integrity and `<Link>` convention. Verified: **113 anchor links resolve, 0 broken
  cross-page links.**
- **Redirects added** for every moved page (`httpHeaders→headers`,
  `telefunc→serve`, `server→Telefunc`) — preserves inbound links.
- **Multi-runtime tabs** (Node/Express/Bun/Deno/Cloudflare) on the `Telefunc` page
  meet users where they are; a real Node perf caveat (`req/res` vs `Request`) is cited.
- **Consistent "See also" footer** on every page aids lateral discovery.

### Issues — correctness (highest priority)

- **`stream` Channel client example is broken** (`docs/pages/stream/+Page.mdx:423`):
  the snippet `await onDashboard()` into `const dashboard`, then calls
  `channel.listen(...)` — `channel` is undefined; it must be `dashboard.listen(...)`.
  The example also omits the `import { onDashboard }` line that sibling examples include.
- **Dead commented-out block shipped** (`docs/pages/file-download/+Page.mdx:455–499`):
  a large `{/* TODO/after-merge … */}` section containing an internal PR link and
  "TO-DO" notes. Should not be in a published page.
- **Undocumented API surface** (`file-download`): `dl.onProgress()` and `dl.cancel()`
  appear in the "progress + cancel" example, but neither is in the reading-strategies
  table — the section that would document `cancel()` is the commented-out block above.
- **Inconsistent illustrative `ai` API**: `ai.streamText({ prompt })` (stream overview,
  `onClose`) vs `await ai(prompt)` returning `{ stream, cancel }` (stream AI example).
  Same fictional API, three shapes — trips readers scanning across examples.

### Issues — links

- **`close` page, "How to close"** (`docs/pages/close/+Page.mdx:1470`): "manually
  close a stream early with `close()`" links to `/onClose` instead of the on-page
  `#close` (or `/close`).
- **`close` page table self-links** (lines ~1479–1480): `channel.close()` and
  `channel.abort()` link to `/close` (the page itself) rather than `/channel`.
  > These pass the lint gate because it only validates `#anchor` existence, not the
  > correctness of plain `/page` targets.

### Issues — typos / grammar

- `stream/scale/+Page.mdx:70` — "an **apadter**" → "an adapter".
- `rxjs/+Page.mdx:8` — "**all value** sent … are validated" → "all values".
- `tanstack-query/+Page.mdx:81` — "every connected **clients refetches**" →
  "every connected client refetches".
- `close/+Page.mdx:11` — "For **listing** to streams closing" → "For listening".

### Issues — consistency & structure (nits)

- **`/Telefunc` (capitalized URL) is a footgun**: it sits oddly beside lowercase
  URLs, is case-sensitive, and `/telefunc` now redirects to `/serve` (not the new
  page) while `/server` redirects to `/Telefunc`. Easy to mis-link.
- **`stream/scale` repeats the Cloudflare note twice** in adjacent sections (mild
  redundancy / DRY).
- **`config.channel.connectTtl`** uses `Ttl` while every sibling option uses
  `Timeout`/`Interval` — reference naming inconsistency.
- **`channel-config` asymmetric defaults** (server replay 256 KB vs client 1 MB)
  given without a one-line rationale a reference reader will want.
- **`transport` table legend** lists `❌ none` but the channel-transport table never
  uses `❌` (legend/table mismatch).
- **`new Channel<never, Metrics>()`** uses an undefined `Metrics` type in the stream
  example (illustrative, but an inline comment or import would help copy-paste).
- **No note that Telefunc's `Channel`/`BroadcastChannel` shadow the Web-platform
  `BroadcastChannel` global** — a real import-confusion risk.

### Coverage

- Generally thorough. The main discoverability gap: there's no single "**which
  primitive should I use?**" chooser at the top of `/stream` — the "good fit" hints
  are scattered across sections.
- The GC-based auto-close mechanism (`close` page) is honestly caveated, but for
  *server-held* resources the non-determinism deserves a slightly stronger nudge
  toward explicit cleanup.

---

## Suggestions (prioritized)

1. **Fix the broken `stream` Channel example** — `channel.listen` → `dashboard.listen`,
   and add the missing `import`. (Correctness — do first.)
2. **Delete the commented-out TODO block** in `file-download` before publish.
3. **Document `dl.onProgress()` and `dl.cancel()`** in the `file-download`
   reading-strategies table (resurrect the trimmed "Cancelling" section, or fold its
   API into the table).
4. **Repoint the three mis-targeted links** on the `close` page (`#close`, `/channel`,
   `/channel`).
5. **Fix the four typos/grammar issues** listed above.
6. **Unify the illustrative `ai` API** to one shape across `stream` and `onClose`.
7. **Extend `check-docs.mjs`** to also validate plain `/page` link targets and flag
   self-links — it already proves the docs-as-code approach; closing this gap would
   have caught items 1 and 4.
8. **Add a "Choose a primitive" decision table** at the top of `/stream`
   (AsyncGenerator vs Callback vs Multiple Promise vs ReadableStream vs Channel) keyed
   on use case — consolidates the scattered "good fit" notes and improves discovery.
9. **Reconsider the `/Telefunc` capitalized URL** (or at least add a `/telefunc-server`
   redirect) to avoid the case-sensitivity + redirect footgun.
10. **Add a one-line note** that Telefunc's `Channel`/`BroadcastChannel` shadow the
    DOM globals, on first use in `channel`.
11. **De-duplicate the Cloudflare note** in `stream/scale`; rename `connectTtl` to
    match the `Timeout`/`Interval` convention; add a one-line rationale for the
    asymmetric replay-buffer defaults; reconcile the `transport` legend with its table.
12. **Strengthen the cleanup nudge** on the `close` page for server-held resources
    (GC timing is best-effort).
