# Docs sentence-review — PR #264 (`feat: stream`)

A sentence-by-sentence review of every piece of documentation **introduced by PR #264**
(the streaming / real-time feature). Each introduced sentence was rated on two criteria —
**clarity** (zero ambiguity, no fuzzy words) and **naturalness** (reads like normal
JavaScript/TypeScript documentation) — and weak sentences were rewritten.

- Method and rating rules: [`METHODOLOGY.md`](./METHODOLOGY.md)
- This folder is a **review artifact**, not shipped docs. It can be deleted before the PR merges.
- The actual prose improvements are the 26 edits applied to the doc files (see below); the docs
  quality gate (`node docs/check-docs.mjs`) still passes — no anchors or links were broken.

## Results at a glance

| Report | Files reviewed | Sentences | Kept | Edited | 2nd-PR |
|---|---|---:|---:|---:|---:|
| [A](./A-stream.md) | `stream/+Page.mdx` (new) | 51 | 44 | 7 | 0 |
| [B](./B-channel.md) | `channel/+Page.mdx` (new) | 121 | 120 | 1 | 0 |
| [C](./C-cloudflare-scale-transport.md) | `stream/cloudflare`, `stream/scale`, `transport` (new) | 103 | 100 | 2 | 1 |
| [D](./D-file-download-upload.md) | `file-download` (new), `file-upload` (mod) | 68 | 67 | 1 | 0 |
| [E](./E-rxjs-tanstack.md) | `rxjs`, `tanstack-query` (new) | 62 | 61 | 1 | 0 |
| [F](./F-server-getcontext.md) | `server`, `getContext` (mod) | 25 | 25 | 0 | 0 |
| [G](./G-close-serve-telefunc.md) | `close`, `serve`, `Telefunc`, `withContext`, `provideTelefuncContext` (new) | 52 | 45 | 7 | 0 |
| [H](./H-redis-testing-components.md) | `redis`, `redis/README`, `testing`, 2 components (new) | 27 | 24 | 3 | 0 |
| [I](./I-small-modified.md) | 12 small modified files | 9 | 5 | 4 | 0 |
| **Total** | **~35 files** | **518** | **491** | **26** | **1** |

## All 26 applied edits

Each edit changed prose wording only — meaning, code, links, and MDX/JSX were preserved.

### `docs/pages/stream/+Page.mdx` (7)
1. "…can [mix] **— generators**, streams, and promises side by side, each resolving independently **of each other**." → "…can [mix] generators, streams, and promises side by side, each resolving independently." *(dangling em-dash + redundant tail)*
2. "even more seamless and **egornomic** DX" → "…and **ergonomic** DX" *(typo)*
3. "automatic backpressure when **network** is the bottleneck" → "…when **the network** is the bottleneck" *(missing article)*
4. "automatic **recover** from network issues" → "automatic **recovery** from network issues" *(wrong word form)*
5. "compressing a **bulky video into a compact, universally-playable MP4, streamed back as it encodes**" → "compressing a **large video into a smaller MP4 and streaming it back to the client as it encodes**" *(fuzzy adjectives + dangling clause)*
6. "…see `<Link/>`**;** authorizing a channel works like any streaming primitive…" → "…see `<Link/>`**.** Authorizing a channel works…" *(semicolon run-on split)*
7. "**Drop** every reference without closing **and** Telefunc still cleans up automatically, via garbage collection, a few seconds later." → "**Even if you drop** every reference without closing, Telefunc still cleans up automatically via garbage collection a few seconds later." *(imperative-as-condition ambiguity + comma-walled aside)*

### `docs/pages/channel/+Page.mdx` (1)
8. code comment: "**Sent** a toast to all clients **match** the user id" → "**Send** a toast to all clients **matching** the user id" *(tense + grammar)*

### `docs/pages/stream/cloudflare/+Page.mdx` (1)
9. "A new subscriber may miss publishes **until its KV presence record is written, which typically takes a few milliseconds**." → "A new subscriber may miss publishes **during the few milliseconds it takes to write its KV presence record**." *(dangling modifier)*

### `docs/pages/transport/+Page.mdx` (1)
10. "Use `'ws'` for **chatty** real-time traffic **where you want** a full-duplex connection." → "…for **high-frequency, real-time** traffic **that benefits from** a full-duplex connection." *(colloquial "chatty")*

### `docs/pages/file-upload/+Page.mdx` (1)
11. "The file bytes stream while `onProgress()` **updates** the client." → "The file bytes stream **to disk** while `onProgress()` **reports progress to** the client." *(implicit destination + vague "updates")*

### `docs/pages/tanstack-query/+Page.mdx` (1)
12. "**Only after it succeeds, the server subscribes** to invalidation events for that key." → "**The server subscribes** to invalidation events for that key **only after it succeeds**." *(stilted fronted qualifier)*

### `docs/pages/close/+Page.mdx` (2)
13. "Applies to all streaming and real-time values **—** `AsyncGenerator`, … `BroadcastChannel` **—** they all close the same way." → "Applies to all streaming and real-time values**:** `AsyncGenerator`, …, and `BroadcastChannel` all close the same way." *(em-dash splice)*
14. "channel-specific closing is **covered** on `<Link/>`." → "…is **documented** on `<Link/>`." *(repeat of "covers" earlier in sentence)*

### `docs/pages/serve/+Page.mdx` (1)
15. "a pure function: stateless and **has no side effects**." → "a pure function: stateless and **side-effect-free**." *(parallel adjectives)*

### `docs/pages/Telefunc/+Page.mdx` (2)
16. "For per-runtime setup see `<Link/>`, **and `<Link/>` for Cloudflare specifics**." → "For per-runtime setup see `<Link/>`**; for Cloudflare specifics see `<Link/>`**." *(asymmetric coordination)*
17. "Provide the **`getContext()` data** per request" → "Provide the **data for `getContext()`** per request" *(noun-stacking)*

### `docs/pages/provideTelefuncContext/+Page.mdx` (2)
18. "(e.g. in **a test's setup, or your** SSR request handler)" → "(e.g. in **your test setup or your** SSR request handler)" *(spurious comma + non-parallel)*
19. "**Through** `serve()` / `new Telefunc()`, the context is provided for you" → "**With** `serve()` / `new Telefunc()`, …" *(odd preposition)*

### `docs/pages/redis/README.md` (1)
20. "**publishes on any instance reach** subscribers on every other instance." → "**a `publish()` on any instance reaches** subscribers on every other instance." *(noun/verb agreement)*

### `docs/pages/testing/+Page.mdx` (2)
21. "assert the telefunction returns or **`throw Abort()`s** accordingly." → "…returns or **throws `Abort()`** accordingly." *(plural-"s" on a code span)*
22. "the wire protocol … **only exists** over a real connection" → "…**only comes into play** over a real connection" *(odd "exists")*

### `docs/pages/vike/+Page.mdx` (1)
23. "For example **with** Hono:" → "For example**, with** Hono:" *(missing comma)*

### `docs/pages/permissions/+Page.mdx` (1)
24. "the same `getContext()` **+** `throw Abort()` pattern" → "…`getContext()` **and** `throw Abort()` pattern" *(informal `+`)*

### `CONTRIBUTING.md` (2)
25. "`// Environment: server` **/** `// Environment: client`" → "…server` **or** `// Environment: client`" *(ambiguous slash)*
26. "`<Warning>` for must-heed **/ security**, `<Advanced>` for…" → "`<Warning>` for must-heed **or security notes**, …" *(awkward slash, added head noun)*

## Second-PR candidate (1)

One sentence could not be brought to a confident ≥ 8 without an author decision, so it is **not**
applied here and is proposed in a companion PR instead:

- **`docs/pages/stream/cloudflare/+Page.mdx`** — "Once all channels close, no clients are
  connected, and the reconnect and idle windows have expired, the Durable Object can hibernate."
  The best rewrite (Overall 7/10) reorders the clauses for flow, but "all channels close" and "no
  clients are connected" look semantically redundant — collapsing them needs the author to confirm
  whether the two conditions are genuinely independent. Full analysis with 10 rated alternatives:
  report [C], entry **[51]**.

## Rating-calibration note

Reviewers were deliberately critical; the score histograms differ in how 10/10 was awarded.
Reviewer A awarded no 10s at all (capping clear-but-dense prose at 9), while Reviewer B awarded
10 to trivially short, flawless fragments (e.g. table cells like "Close gracefully."). Read scores
**within** each report rather than across them. The one rule that was applied uniformly is the
decision rule that governs the deliverable: **rewrite any sentence rated Overall ≤ 7, ship the
rewrite only when it reaches ≥ 8, otherwise send it to the second PR.**
