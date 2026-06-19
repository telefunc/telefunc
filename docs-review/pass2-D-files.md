# Pass 2-D report — file-download & file-upload prose polish

Second-pass polish of sentences previously rated 7–8 in the PR #264 streaming/real-time docs.
Targets grouped by file. Format per `docs-review/METHODOLOGY-pass2.md`.

## `docs/pages/file-download/+Page.mdx`

### [4] `docs/pages/file-download/+Page.mdx` — "Which API?" blockquote, bullet 2
- **Original:** "**Large files, bytes streamed from an upstream source, or when you need progress / cancel / save-to-disk** → wrap them with `download()`."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — the three list members are not parallel ("Large files" and "bytes streamed…" are noun phrases, "when you need…" is a clause), making the condition slightly harder to parse and the enumeration uneven.
- **Candidates:**
  1. "**Large files, bytes streamed from an upstream source, or anything needing progress / cancel / save-to-disk**" — C 9 / N 9 / Overall 9 — all three members now noun phrases; reads even.
  2. "**Large files, upstream-streamed bytes, or progress / cancel / save-to-disk needs**" — C 8 / N 8 / Overall 8 — parallel but "needs" is clunky and "upstream-streamed" compresses meaning.
  3. "**Large files, bytes streamed from an upstream source, or a need for progress / cancel / save-to-disk**" — C 8 / N 8 / Overall 8 — parallel as noun phrases but "a need for" reads stiff.
  4. "**Large files, bytes streamed from an upstream source, or progress / cancel / save-to-disk**" — C 9 / N 9 / Overall 9 — elliptical (drops "when you need"); matches the table's "Best for" style but slightly loses the "you need these" sense.
- **Decision:** Applied → "**Large files, bytes streamed from an upstream source, or anything needing progress / cancel / save-to-disk** → wrap them with `download()`." (new Overall 9)
- **Why:** "anything needing …" turns the dangling clause into a parallel noun phrase, fixing the uneven enumeration while keeping the meaning.

### [12] `docs/pages/file-download/+Page.mdx` — "Reading strategies" intro
- **Original:** "The streaming download is a standard `File` / `Blob`-shaped object — `dl.stream()`, `dl.text()`, `dl.arrayBuffer()`, `dl.bytes()`, and `dl.slice()` all work and pull from the streaming source as you read:"
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "the streaming download" is slightly jargon-y as a standalone noun and "`Blob`-shaped" is a coined adjective; long sentence joining a definition and a method list.
- **Candidates:**
  1. "The streaming download behaves like a standard `File` / `Blob` — …" — C 9 / N 9 / Overall 9 — drops the coined "-shaped" adjective; "behaves like" reads naturally and is accurate.
  2. "The streaming download exposes the same API as a standard `File` / `Blob` — …" — C 9 / N 8 / Overall 8 — precise but more technical; lead noun unchanged.
  3. "`dl` is a standard `File` / `Blob`-shaped object — …" — C 8 / N 8 / Overall 8 — names the variable but keeps the coined adjective.
  4. "The streaming download behaves like a standard `File` / `Blob`: `dl.stream()`, … as you read:" — C 9 / N 8 / Overall 8 — colon is weaker than the on-voice em-dash.
- **Decision:** Applied → "The streaming download behaves like a standard `File` / `Blob` — `dl.stream()`, `dl.text()`, `dl.arrayBuffer()`, `dl.bytes()`, and `dl.slice()` all work and pull from the streaming source as you read:" (new Overall 9)
- **Why:** "behaves like" removes the coined "`Blob`-shaped" adjective; "the streaming download" stays as the section's established subject. MDN links preserved.

### [14] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.bytes()` Best-for cell
- **Original:** "Process binary bytes"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "binary bytes" is mildly redundant (bytes are inherently binary), though the intent (raw bytes, not decoded text) is clear.
- **Candidates:**
  1. "Process raw bytes" — C 9 / N 9 / Overall 9 — removes the redundancy; "raw" still signals undecoded; keeps the "Process" verb parallel with the `arrayBuffer()` row.
  2. "Process raw binary data" — C 9 / N 9 / Overall 9 — clear but longer; "data" is vaguer than "bytes" for a `Uint8Array`.
  3. "Process bytes directly" — C 8 / N 9 / Overall 8 — "directly" adds little.
  4. "Work with raw bytes" — C 9 / N 9 / Overall 9 — natural but breaks the "Process" verb pattern of the sibling cell.
- **Decision:** Applied → "Process raw bytes" (new Overall 9)
- **Why:** "raw" replaces the redundant "binary" while preserving the undecoded-bytes intent and the sibling-cell verb.

### [25] `docs/pages/file-download/+Page.mdx` — blockquote on real `File`/`Blob` requirement (sentence 1)
- **Original:** "Web APIs that require a real `File` / `Blob` (`URL.createObjectURL`, `<img src>`, `fetch({body})`, `FormData.append`) need `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first."
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — heavy: a four-API parenthetical plus a three-method slash-list in one sentence makes it dense.
- **Candidates:**
  1. "Web APIs that require a real `File` / `Blob` — `URL.createObjectURL`, `<img src>`, `fetch({body})`, `FormData.append` — need `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first." — C 9 / N 9 / Overall 9 — em-dashes set the list off more clearly than parentheses.
  2. "Some Web APIs — `URL.createObjectURL`, `<img src>`, `fetch({body})`, `FormData.append` — require a real `File` / `Blob`, so call `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first." — C 9 / N 9 / Overall 9 — splits the requirement (cause) from the action with "so"; reads as two beats instead of one pile-up.
  3. "To use a streaming download with Web APIs that need a real `File` / `Blob` (`URL.createObjectURL`, `<img src>`, `fetch({body})`, `FormData.append`), call `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first." — C 8 / N 8 / Overall 8 — front-loads purpose but adds words and stays dense.
  4. "Web APIs that need a real `File` / `Blob` — like `URL.createObjectURL`, `<img src>`, `fetch({body})`, or `FormData.append` — require `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first." — C 9 / N 9 / Overall 9 — em-dashes plus "like … or" mark the list as illustrative.
- **Decision:** Applied → "Some Web APIs — `URL.createObjectURL`, `<img src>`, `fetch({body})`, `FormData.append` — require a real `File` / `Blob`, so call `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first." (new Overall 9)
- **Why:** Em-dashes pull the example list out of the noun phrase and "so" separates requirement from action, breaking the density; every code token and the following sentence are preserved. "Some" is accurate (a subset of Web APIs).

## `docs/pages/file-upload/+Page.mdx`

### [41] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, Channel Best-for cell
- **Original:** "Low-latency frames — skip the `await` for fire-and-forget"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — "skip the `await` for fire-and-forget" assumes the reader connects it to the Backpressure column's "`await` each send"; slightly terse in isolation.
- **Candidates:**
  1. "Low-latency frames — skip the `await` to fire-and-forget" — C 8 / N 9 / Overall 8 — "to" marginally smoother but doesn't improve self-containment.
  2. "Low-latency frames — omit the `await` for fire-and-forget" — C 8 / N 9 / Overall 8 — synonym swap; same issue.
  3. "Low-latency frames — drop the `await` for fire-and-forget" — C 8 / N 9 / Overall 8 — same.
  4. "Low-latency frames — fire-and-forget by skipping the `await`" — C 9 / N 9 / Overall 9 — concept-first ("fire-and-forget") then mechanism; reads more self-explanatory standalone.
- **Decision:** Applied → "Low-latency frames — fire-and-forget by skipping the `await`" (new Overall 9)
- **Why:** Leading with the concept and following with the mechanism improves standalone readability while preserving the causal relation and the contrast with the Backpressure column.

### [44] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, BroadcastChannel Backpressure cell
- **Original:** "Publish-side only"
- **Previous:** Clarity 8 / Naturalness 9 / Overall 8 — terse; meaning (backpressure applies only on the publishing side) is recoverable but compact for a reader new to the term.
- **Candidates:**
  1. "Publish side only" — C 8 / N 8 / Overall 8 — drops hyphen; "publish side" as a noun reads less crisp than the adjectival "publish-side".
  2. "Publisher-side only" — C 8 / N 9 / Overall 8 — arguably a hair clearer but no real gain, and shifts wording without need.
  3. "Only on the publish side" — C 9 / N 8 / Overall 8 — fuller clause but breaks the column's terse-label rhythm ("TCP-level", "Yes").
  4. "Publish-side only" (retain) — matches the column's compact-label convention; the Backpressure column header already supplies the missing context.
- **Decision:** Retained (no candidate beat Overall 8)
- **Why:** The terseness is a deliberate table convention (sibling cells "TCP-level", "Yes", "Opt-in — `await` each send"); the column header resolves the ambiguity, and any fuller phrasing breaks the label register without a true clarity gain.

### [54] `docs/pages/file-upload/+Page.mdx` — "Limitations" intro (sentence 2)
- **Original:** "This makes uploads memory-efficient, with two trade-offs:"
- **Previous:** Clarity 8 / Naturalness 8 / Overall 8 — "with two trade-offs" is appended as a loose modifier rather than a full clause; the comma-plus-fragment lead-in reads slightly clipped/informal.
- **Candidates:**
  1. "This makes uploads memory-efficient, but it comes with two trade-offs:" — C 9 / N 9 / Overall 9 — full clause and explicit contrast, slightly wordy.
  2. "This makes uploads memory-efficient, at the cost of two trade-offs:" — C 8 / N 8 / Overall 8 — "at the cost of … trade-offs" is mildly redundant.
  3. "This makes uploads memory-efficient — but there are two trade-offs:" — C 9 / N 9 / Overall 9 — em-dash (on-voice) plus full clause; crisp contrast.
  4. "This makes uploads memory-efficient. There are two trade-offs:" — C 8 / N 8 / Overall 8 — clean but drops the efficiency/cost linkage.
  5. "This makes uploads memory-efficient, with two trade-offs to keep in mind:" — C 8 / N 8 / Overall 8 — pads the fragment without fixing the construction.
- **Decision:** Applied → "This makes uploads memory-efficient — but there are two trade-offs:" (new Overall 9)
- **Why:** The em-dash plus a full "there are two trade-offs" clause replaces the clipped fragment and signals the contrast, matching Telefunc's em-dash voice.

## Summary

- **Targets:** 7
- **Applied:** 6 (targets 4, 12, 14, 25, 41, 54)
- **Retained:** 1 (target 44)
- **New score distribution:** 6 sentences raised from Overall 8 → 9; 1 retained at Overall 8.
- All `<Link>` components, MDN URLs, inline code, bold emphasis, and table structures verified intact after edits.
