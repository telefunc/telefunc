# Pass-2 methodology — raise the "kept" sentences toward 10/10

Second pass over the docs introduced by PR #264. Pass 1 rewrote every sentence rated Overall ≤ 7.
This pass works on sentences that were *kept* at **7 or 8** — clear and natural, but with a real
(if minor) documented weakness — and tries to push each to **10/10**. (Sentences rated 9 are out of
scope for this pass and must not be touched.)

## Your input

A **TARGETS file**: specific sentences, each with its previous `Clarity` / `Naturalness` /
`Overall` scores and the **documented reason** it fell short of 10. Every target is a real sentence
in a current doc file. Work on **every** target.

## Goal

Fix the documented weakness so the sentence becomes crystal clear *and* natural — **without changing
its meaning, technical facts, or the surrounding MDX**. Because every target has a real documented
issue, you should expect to genuinely improve most of them (not just retain them).

## Process (per target)

1. **Locate** the sentence in the current doc file by matching its `Original` text (line numbers
   have shifted; match on text). If the exact text is gone, note "not found" and skip.
2. **Generate candidates** — at least **3** rewordings addressing the documented reason; ≥ **10**
   for stubborn ones.
3. **Rate each** on Clarity / Naturalness / Overall.
4. **Decide:**
   - Best candidate genuinely beats the current score **and** preserves meaning/voice → **apply it**
     in place (Edit tool).
   - No candidate genuinely improves it → **Retain** unchanged, and document why. Retaining is
     legitimate but should be the exception here (these are flagged 7–8 sentences with real issues).

## Quality over quota — do not force changes

A change that does not actually raise clarity or naturalness is a regression. Only apply a rewrite
you genuinely believe is better. You must still *try* (and document candidates) for every target.

## Hard constraints

- Change **prose wording only**. Preserve meaning, technical facts, and code logic.
- Preserve all MDX/JSX exactly: `<Link/>`, `<Warning>`, `<Advanced>`, components, inline code,
  emphasis, anchors, URLs, table structure.
- **Do not edit heading text** unless it carries a custom `{#anchor}` (never touch the `{#...}`).
- Internal links use `<Link>`, never bare markdown. American English. Telefunc voice: concise,
  direct, technical; em-dashes common. Fix any typo. Make Edit `old_string`s uniquely matchable.

## Report format (one entry per target)

```
### [N] `<file>` — <locator>
- **Original:** "<exact original sentence>"
- **Previous:** Clarity X / Naturalness Y / Overall Z — <prior reason it wasn't 10>
- **Candidates:**
  1. "<candidate>" — C n / N n / Overall n — <note>
  2. "<candidate>" — … 
  3. "<candidate>" — …
- **Decision:** Applied → "<final text>" (new Overall n)  |  Retained (no candidate beat Overall Z)
- **Why:** <one line>
```

End with a `## Summary`: targets, applied, retained, new score distribution.
