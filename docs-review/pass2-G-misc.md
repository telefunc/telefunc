# Pass-2 report — G (misc)

Second pass on PR #264 docs. Targets previously rated 7–8 (clear/natural with a documented minor weakness), pushed toward 10/10 via prose-only rewording. Grouped by file.

## `packages/redis/README.md`

### [14] `packages/redis/README.md` — Setup, after code
- **Original:** "`Channel` is per-instance — its reconnects need to land on the instance holding the channel's state."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "its reconnects" (a channel's reconnects) is slightly loose; the page version's "Reconnects must land on the instance holding the channel's state" reads cleaner; "need to land" is a touch awkward versus "must land."
- **Candidates:**
  1. "`Channel` is per-instance — a channel's reconnects must land on the instance holding its state." — C 10 / N 9 / Overall 9.5 — fixes "its" referent and "need to"→"must", but the "a channel's…its" pairing is slightly redundant.
  2. "`Channel` is per-instance — reconnects must land on the instance holding the channel's state." — C 10 / N 10 / Overall 10 — drops the loose "its reconnects" entirely; "reconnects" is unambiguous after the lead-in; mirrors the cleaner page version verbatim.
  3. "`Channel` is per-instance, so reconnects must land on the instance holding the channel's state." — C 10 / N 9 / Overall 9.5 — replaces the em-dash with "so"; em-dash is the established Telefunc voice here, so this is a slight regression in voice.
- **Decision:** Applied → "`Channel` is per-instance — reconnects must land on the instance holding the channel's state." (new Overall 10)
- **Why:** Removes the loose "its reconnects" and the awkward "need to land," matching the documented cleaner phrasing while keeping the em-dash voice.

### [16] `packages/redis/README.md` — Sharing an existing client
- **Original:** "Pass an [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options (TLS, retry strategy, etc):"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — "(TLS, retry strategy, etc):" with an unpunctuated "etc" and a trailing colon is slightly informal.
- **Candidates:**
  1. "...set custom options such as TLS or a retry strategy:" — C 9 / N 10 / Overall 9.5 — removes "etc" and the parenthetical; "such as" signals a non-exhaustive list; drops the README-idiomatic parenthetical form.
  2. "...set custom options (TLS, retry strategy, etc.):" — C 9 / N 9 / Overall 9 — only adds the period to "etc."; the "etc.):" punctuation pileup is still slightly clunky.
  3. "...set custom options (e.g. TLS or a retry strategy):" — C 10 / N 10 / Overall 10 — "e.g." signals examples without a trailing "etc"; clean parenthetical; matches the "e.g." style used elsewhere in these docs (NeedsLongRunningServer).
- **Decision:** Applied → "...set custom options (e.g. TLS or a retry strategy):" (new Overall 10)
- **Why:** Keeps the idiomatic parenthetical intro while removing the awkward unpunctuated "etc," using the doc's existing "e.g." convention.

## `docs/pages/testing/+Page.mdx`

### [20] `docs/pages/testing/+Page.mdx` — Providing context
- **Original:** "If a telefunction reads <Link text="getContext()" href="/getContext" /> (for the user, headers, …), provide the context in your test setup with <Link text={<code>provideTelefuncContext()</code>} href="/provideTelefuncContext" /> before calling it:"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — the parenthetical "(for the user, headers, …)" is a compressed list with an ellipsis, slightly telegraphic.
- **Candidates:**
  1. "...reads <Link …getContext… /> (e.g. the user or request headers), provide..." — C 9.5 / N 9.5 / Overall 9.5 — replaces the telegraphic ellipsis with "e.g." (still open-ended) plus two clear examples; "request headers" sharpens the bare "headers."
  2. "...reads <Link …getContext… /> (for the user, request headers, etc.), provide..." — C 9 / N 9 / Overall 9 — keeps the "for the" framing and adds "request," but reintroduces "etc."
  3. "...reads <Link …getContext… /> (for the user, request headers, and so on), provide..." — C 9 / N 8.5 / Overall 8.5 — spells the ellipsis out, but "for the user… and so on" mixes the opening "for" with a closing list connector awkwardly.
  4. "...reads <Link …getContext… /> — e.g. the user or request headers — provide..." — C 9 / N 8.5 / Overall 8.5 — em-dash aside competes with the sentence's existing comma structure and reads long.
- **Decision:** Applied → "If a telefunction reads <Link text="getContext()" href="/getContext" /> (e.g. the user or request headers), provide the context in your test setup with <Link text={<code>provideTelefuncContext()</code>} href="/provideTelefuncContext" /> before calling it:" (new Overall 9.5)
- **Why:** "e.g." keeps the list open-ended while removing the telegraphic ellipsis; "request headers" clarifies the example. Both `<Link/>` elements preserved exactly.

## `docs/components/NeedsLongRunningServer.mdx`

### [26] `docs/components/NeedsLongRunningServer.mdx` — callout
- **Original:** "A channel holds a connection open for its entire lifetime, so channels and broadcasts don't work on most serverless platforms (e.g. Vercel, AWS Lambda), which terminate a connection after a short time limit."
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "after a short time limit" is slightly redundant ("after a limit" vs "after a timeout").
- **Candidates:**
  1. "...which terminate a connection after a short time." — C 10 / N 10 / Overall 10 — drops the redundant "limit"; "after a short time" is clean and idiomatic; minimal change.
  2. "...which close connections after a short timeout." — C 9 / N 9 / Overall 9 — "timeout" is precise, but changes "terminate"→"close" and "a connection"→"connections" beyond what's needed.
  3. "...which enforce a short connection time limit." — C 9 / N 9 / Overall 9 — restructures the cause; "enforce a limit" subtly shifts meaning away from "terminate the connection."
  4. "...which terminate a connection after a short timeout." — C 9.5 / N 9.5 / Overall 9.5 — "timeout" fixes the redundancy precisely with a one-word swap, but "after a short time" reads slightly more natural.
- **Decision:** Applied → "...which terminate a connection after a short time." (new Overall 10)
- **Why:** Removes the "limit" redundancy with the most natural minimal edit while keeping the verb and singular "a connection." `<Link/>` elements untouched.

## `CONTRIBUTING.md`

### [4] `CONTRIBUTING.md` — list intro
- **Original:** "Enforced by `pnpm run docs:lint`:"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — a bare "Enforced by X:" fragment is idiomatic as a list lead-in but reads slightly clipped.
- **Candidates:**
  1. "These rules are enforced by `pnpm run docs:lint`:" — C 10 / N 10 / Overall 10 — completes the sentence, names the subject ("these rules"), still a clean list lead-in.
  2. "The following are enforced by `pnpm run docs:lint`:" — C 9 / N 9 / Overall 9 — "the following" is filler-ish next to the list it already precedes.
  3. "`pnpm run docs:lint` enforces:" — C 9 / N 8.5 / Overall 8.5 — active voice but even more clipped than the original.
- **Decision:** Applied → "These rules are enforced by `pnpm run docs:lint`:" (new Overall 10)
- **Why:** Turns the clipped fragment into a complete, natural sentence while keeping it a tight list lead-in.

### [7] `CONTRIBUTING.md` — transition label
- **Original:** "Also:"
- **Previous:** Clarity 9 / Naturalness 8 / Overall 8 — a one-word "Also:" is slightly abrupt, though acceptable in a terse contributing guide.
- **Candidates:**
  1. "Also follow these conventions:" — C 10 / N 10 / Overall 10 — a full clause naming what follows ("conventions"); parallels the "These rules…" lead-in just above; resolves the abruptness.
  2. "Not enforced, but expected:" — C 9 / N 9 / Overall 9 — captures the lint-vs-not contrast but is itself a clipped fragment and adds a claim not in the original.
  3. "Also keep in mind:" — C 9 / N 9 / Overall 9 — natural but vaguer than "follow these conventions."
  4. "Additionally:" — C 9 / N 8 / Overall 8.5 — a one-word swap that stays abrupt.
- **Decision:** Applied → "Also follow these conventions:" (new Overall 10)
- **Why:** Replaces the abrupt one-word label with a parallel, descriptive lead-in matching target [4]'s fix; meaning (a second, non-lint-enforced list) preserved.

## Summary

- **Targets:** 6
- **Applied:** 6
- **Retained:** 0
- **New score distribution (Overall):** 10 × 5 (targets 14, 16, 26, 4, 7), 9.5 × 1 (target 20).

All six targets had genuine, documented minor weaknesses (loose pronoun, unpunctuated "etc," telegraphic ellipsis, redundant "limit," two clipped fragments). Each was improved with prose-only edits that preserve meaning, technical facts, and all MDX/JSX (`<Link/>`, inline code, markdown links, emphasis). Changed regions were re-read and confirmed intact.
