# Docs sentence-review methodology

This folder is an **audit trail** for a sentence-by-sentence review of the documentation
introduced by PR #264 (`feat: stream`). It is a review artifact, not shipped docs — it can
be dropped before the PR is merged.

## Scope: what counts as "introduced by this PR"

- **New files** (whole file is new): review every prose sentence in the file.
- **Modified files**: review **only** sentences on lines this PR added or changed. Read the
  whole file for context, but do **not** rate pre-existing, untouched sentences. The exact
  added/changed lines are in the per-file diff under `/tmp/diffs/`.

## What is a "sentence"

A sentence is a standalone unit of prose in the rendered docs:

- Each markdown paragraph sentence, each bullet/list item, each blockquote/callout sentence,
  each `<Link>` description, and each prose cell in a table.
- Treat each list item and each table cell with prose as its own unit.
- **Code blocks**: do **not** rate code. But **do** scan code comments — flag any comment that
  has a typo, a grammatical error, or an unclear/misleading explanation, and fix obvious typos.
- **Skip**: `import` lines, frontmatter, bare component tags with no prose, anchor slugs, URLs,
  and pure-syntax table cells (e.g. `send()` / `listen()`).

## Rating (per sentence)

Give three scores, each 1–10:

- **Clarity** — Is it crystal clear? Zero ambiguity, no fuzzy words, reader never second-guesses.
- **Naturalness** — Does it read naturally in JavaScript/TypeScript documentation? No weird,
  awkward, or unfamiliar phrasing.
- **Overall** — Holistic, governed by the weaker of the two: a sentence cannot be great if
  either clarity or naturalness is poor.

**For any score below 10, you MUST write a concise reason.** Do not hand out 10s by default —
reserve 10 for a genuinely flawless sentence. Be a critical reviewer: most solid sentences land
in the 7–9 range. If most of your scores are 10, you are being lazy.

## Editing

- Edit threshold: a sentence needs editing when **Overall ≤ 7**.
- For every such sentence, write an improved version and **re-rate it**.
  - If the edit reaches **Overall ≥ 8**, apply it to the file in place.
  - If the first edit is still **≤ 7**, generate **at least 10 alternative wordings**, rate each,
    and pick the best.
    - If the best reaches **≥ 8**, apply it.
    - If the best is **still ≤ 7**, do **NOT** apply it. Record it under `## SECOND-PR CANDIDATES`
      with the exact original text, file path, the best edit, its rating, and why it is still
      imperfect. (These are collected into a separate, second PR.)
- Sentences rated **8 or 9** are kept as-is, but their reason (why not 10) is still documented.

## Hard constraints on edits

- Change **prose wording only**. Never change meaning, technical facts, or code logic.
- Preserve all MDX/JSX exactly: `<Link .../>`, `<Warning>`, other components, inline code,
  emphasis, anchors, and URLs must stay intact.
- **Do not edit heading text** unless the heading carries a custom `{#anchor}` (editing a plain
  heading would break the anchor links the docs validator checks). Never alter the `{#...}` part.
- Do not introduce bare markdown internal links — internal links must use `<Link>`.
- American English. Match Telefunc's voice: concise, direct, technical; em-dashes are common.
- Fix obvious typos/grammar regardless of score.
- When using the Edit tool, read the file first and make the `old_string` uniquely matchable.

## Report format

Write one numbered entry per reviewed sentence:

```
### [N] `<file path>` — <section / brief locator>
- **Original:** "<exact sentence>"
- **Clarity:** X/10 — <reason if <10>
- **Naturalness:** Y/10 — <reason if <10>
- **Overall:** Z/10
- **Action:** Kept | Edited | Second-PR candidate
- **Edit:** "<new sentence>"                         (omit if Kept)
- **Edit rating:** Clarity X/10, Naturalness Y/10, Overall Z/10 — <reason if <10>   (omit if Kept)
- **Alternatives:** (only when the first edit was ≤ 7)
  1. "<alt 1>" — Overall n/10 — <note>
  ... (at least 10)
  - **Chosen:** #k
```

End the report with a `## Summary` (totals: reviewed, kept, edited, second-PR candidates) and,
if any, a `## SECOND-PR CANDIDATES` section.
