# Sentence review — file-download & file-upload (PR #264)

Reviewer D. Methodology: `/home/user/telefunc/docs-review/METHODOLOGY.md`.

- `docs/pages/file-download/+Page.mdx` — NEW file, all prose reviewed.
- `docs/pages/file-upload/+Page.mdx` — MODIFIED file, only sentences introduced/changed by the diff (`/tmp/diffs/docs_pages_file-upload_+Page.mdx.diff`) reviewed.

---

## `docs/pages/file-download/+Page.mdx` (NEW)

### [1] `docs/pages/file-download/+Page.mdx` — intro, first sentence
- **Original:** "Return a `File` or `Blob` from a telefunction like any other value."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [2] `docs/pages/file-download/+Page.mdx` — intro, second sentence
- **Original:** "The client receives a standard `File` / `Blob` ready for `URL.createObjectURL`, `<img src>`, `fetch({ body })`, `FormData`, etc."
- **Clarity:** 9/10 — the trailing list with "etc." is slightly open-ended, but the examples are concrete and unambiguous.
- **Naturalness:** 9/10 — reads like normal API docs; the dense inline-code list is a touch heavy but idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [3] `docs/pages/file-download/+Page.mdx` — "Which API?" blockquote, bullet 1
- **Original:** "**Bytes already in memory** → return a native `File` / `Blob` (as above)."
- **Clarity:** 9/10 — the `→` arrow convention is clear in context but is shorthand rather than full prose.
- **Naturalness:** 9/10 — concise decision-table style; idiomatic for a callout.
- **Overall:** 9/10
- **Action:** Kept

### [4] `docs/pages/file-download/+Page.mdx` — "Which API?" blockquote, bullet 2
- **Original:** "**Large files, bytes streamed from an upstream source, or when you need progress / cancel / save-to-disk** → wrap them with `download()`."
- **Clarity:** 8/10 — understandable, but the three list members are not parallel ("Large files" and "bytes streamed…" are noun phrases, "when you need…" is a clause), which makes the condition slightly harder to parse.
- **Naturalness:** 8/10 — the non-parallel enumeration reads a little uneven.
- **Overall:** 8/10
- **Action:** Kept (prose-only parallelism fix is borderline and risks touching the bolded condition; original is clear enough)

### [5] `docs/pages/file-download/+Page.mdx` — "Which API?" blockquote, bullet 2 continuation
- **Original:** "When the source is a stream, the full payload is never buffered on the server."
- **Clarity:** 9/10 — clear; "never buffered" is precise.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [6] `docs/pages/file-download/+Page.mdx` — "Which API?" blockquote, closing line
- **Original:** "The tables below break down the memory trade-offs."
- **Clarity:** 9/10 — "tables below" is mildly imprecise (one table follows immediately; another is much further down), but the meaning is clear.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [7] `docs/pages/file-download/+Page.mdx` — "Which one should I use?" table footnote †
- **Original:** "Constant when consumed via `dl.stream()`, `dl.saveToOpfs()`, or `dl.saveToDisk()` (except its `'memory'` fallback); full payload when consumed via `dl.saveToMemory()` / `dl.text()` / `dl.arrayBuffer()`."
- **Clarity:** 9/10 — precise; the parenthetical exception and the two-clause structure are dense but unambiguous.
- **Naturalness:** 9/10 — long for a footnote, but reads correctly.
- **Overall:** 9/10
- **Action:** Kept

### [8] `docs/pages/file-download/+Page.mdx` — "Example" section intro
- **Original:** "Return a native `File` or `Blob` like any other value."
- **Clarity:** 9/10 — clear; close echo of sentence [1] (intentional, as a section lead-in).
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [9] `docs/pages/file-download/+Page.mdx` — "Streaming with `download()`" intro
- **Original:** "Wrap a `ReadableStream` with `download()` to stream bytes from an upstream source through your server without buffering."
- **Clarity:** 9/10 — clear; "through your server" precisely conveys pass-through.
- **Naturalness:** 9/10 — "stream bytes … through your server without buffering" is a slightly long chain of qualifiers but reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [10] `docs/pages/file-download/+Page.mdx` — after ImageView example, blockquote
- **Original:** "`dl.name`, `dl.type`, `dl.size`, `dl.lastModified` are available as soon as `await onGetImage(...)` settles — bytes stream in the background."
- **Clarity:** 9/10 — clear; "settles" is correct Promise terminology that some readers may find slightly formal but it is accurate.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [11] `docs/pages/file-download/+Page.mdx` — "Progress + cancel", closing line
- **Original:** "Calling `dl.cancel()` aborts the stream — the pending `saveToMemory` call rejects with a "Stream cancelled" error."
- **Clarity:** 9/10 — clear; precisely states the abort and the rejection.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [12] `docs/pages/file-download/+Page.mdx` — "Reading strategies" intro
- **Original:** "The streaming download is a standard `File` / `Blob`-shaped object — `dl.stream()`, `dl.text()`, `dl.arrayBuffer()`, `dl.bytes()`, and `dl.slice()` all work and pull from the streaming source as you read:"
- **Clarity:** 8/10 — clear overall, but "the streaming download" as a standalone noun is slightly jargon-y on first encounter, and "`File` / `Blob`-shaped" is a coined adjective; meaning is recoverable.
- **Naturalness:** 8/10 — long sentence joining a definition and a method list; the hyphenated "`Blob`-shaped" is a touch unusual.
- **Overall:** 8/10
- **Action:** Kept

### [13] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.stream()` Best-for cell
- **Original:** "Pipe to disk, S3, or any writable stream"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic table fragment.
- **Overall:** 9/10
- **Action:** Kept

### [14] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.bytes()` Best-for cell
- **Original:** "Process binary bytes"
- **Clarity:** 8/10 — "binary bytes" is mildly redundant (bytes are inherently binary), but the intent (raw bytes, not decoded text) is clear.
- **Naturalness:** 9/10 — fine as a fragment.
- **Overall:** 8/10
- **Action:** Kept (table-cell fragment; minor redundancy not worth a risky edit)

### [15] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.arrayBuffer()` Best-for cell
- **Original:** "Process entire file at once"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [16] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.saveToMemory()` Best-for cell
- **Original:** "Use with Web APIs (`URL.createObjectURL`, `<img src>`, ...)"
- **Clarity:** 9/10 — clear; trailing "..." is open-ended but acceptable in a cell.
- **Naturalness:** 9/10 — fine fragment.
- **Overall:** 9/10
- **Action:** Kept

### [17] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.saveToDisk()` Best-for cell
- **Original:** "Save to user's filesystem (picker or Downloads folder)"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [18] `docs/pages/file-download/+Page.mdx` — "Reading strategies" table, `dl.saveToOpfs()` Best-for cell
- **Original:** "Stash in browser-private storage (OPFS)"
- **Clarity:** 9/10 — clear; "Stash" is informal but unambiguous and matched elsewhere in the file.
- **Naturalness:** 9/10 — fine.
- **Overall:** 9/10
- **Action:** Kept

### [19] `docs/pages/file-download/+Page.mdx` — table footnote ‡
- **Original:** "`File` for `FileDownload`, `Blob` for `BlobDownload`."
- **Clarity:** 9/10 — concise mapping, clear.
- **Naturalness:** 9/10 — fragment, idiomatic for a footnote.
- **Overall:** 9/10
- **Action:** Kept

### [20] `docs/pages/file-download/+Page.mdx` — table footnote §
- **Original:** "Constant in `'picker'` / `'opfs'` mode; the `'memory'` fallback (default when `showSaveFilePicker` is unavailable) buffers the full file in RAM."
- **Clarity:** 9/10 — precise; dense but unambiguous.
- **Naturalness:** 9/10 — reads well for a footnote.
- **Overall:** 9/10
- **Action:** Kept

### [21] `docs/pages/file-download/+Page.mdx` — "Reading strategies" blockquote
- **Original:** "For large files, prefer `dl.stream()` / `dl.saveToDisk()` / `dl.saveToOpfs()` — memory stays constant regardless of file size."
- **Clarity:** 9/10 — clear; the slash-separated method list reads as "any of these."
- **Naturalness:** 9/10 — idiomatic guidance line.
- **Overall:** 9/10
- **Action:** Kept

### [22] `docs/pages/file-download/+Page.mdx` — code comment "Pass to a Web API:"
- **Original:** "// Pass to a Web API:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept (comment scan: no typo)

### [23] `docs/pages/file-download/+Page.mdx` — code comment "Save to user's filesystem…"
- **Original:** "// Save to user's filesystem — picker if available, else Downloads folder:"
- **Clarity:** 9/10 — clear comment; "else Downloads folder" is terse but understandable.
- **Naturalness:** 9/10 — fine as a code comment.
- **Overall:** 9/10
- **Action:** Kept (comment scan: no typo)

### [24] `docs/pages/file-download/+Page.mdx` — code comment "Stash in browser-private storage…"
- **Original:** "// Stash in browser-private storage; URL.createObjectURL on the result has no RAM cost:"
- **Clarity:** 9/10 — clear; explains the no-RAM-cost benefit precisely.
- **Naturalness:** 9/10 — fine comment.
- **Overall:** 9/10
- **Action:** Kept (comment scan: no typo)

### [25] `docs/pages/file-download/+Page.mdx` — blockquote on real `File`/`Blob` requirement (sentence 1)
- **Original:** "Web APIs that require a real `File` / `Blob` (`URL.createObjectURL`, `<img src>`, `fetch({body})`, `FormData.append`) need `await dl.saveToMemory()` / `saveToDisk()` / `saveToOpfs()` first."
- **Clarity:** 8/10 — clear, but heavy: a parenthetical of four APIs plus a slash-list of three methods inside one sentence makes it dense.
- **Naturalness:** 8/10 — reads correctly but is a long, code-dense sentence.
- **Overall:** 8/10
- **Action:** Kept (splitting would help readability but the meaning is intact and unambiguous)

### [26] `docs/pages/file-download/+Page.mdx` — blockquote on real `File`/`Blob` requirement (sentence 2)
- **Original:** "These APIs check for an internal marker that only real `File` / `Blob` instances have."
- **Clarity:** 9/10 — clear explanation of why.
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [27] `docs/pages/file-download/+Page.mdx` — `saveToDisk({ mode })` table, `(omitted)` row
- **Original:** "`'picker'` if `showSaveFilePicker` is available, else `'memory'`."
- **Clarity:** 9/10 — clear fallback rule.
- **Naturalness:** 9/10 — concise, idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [28] `docs/pages/file-download/+Page.mdx` — `saveToDisk({ mode })` table, `'picker'` row
- **Original:** "Opens the save-file picker so the user chooses the location. Throws if not supported."
- **Clarity:** 9/10 — clear; "the location" is unambiguous in context.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [29] `docs/pages/file-download/+Page.mdx` — `saveToDisk({ mode })` table, `'memory'` row
- **Original:** "Buffers bytes in RAM, then triggers a native browser download to the Downloads folder. Cross-browser."
- **Clarity:** 9/10 — clear; "Cross-browser." as a one-word fragment is terse but understandable as a tag.
- **Naturalness:** 9/10 — fine for a table cell.
- **Overall:** 9/10
- **Action:** Kept

### [30] `docs/pages/file-download/+Page.mdx` — `saveToDisk({ mode })` table, `'opfs'` row
- **Original:** "Streams bytes through browser-private storage (OPFS), then triggers a native browser download. For large files where in-memory buffering isn't viable. Cross-browser."
- **Clarity:** 9/10 — clear; three short statements packed into one cell, each unambiguous.
- **Naturalness:** 9/10 — fragment-heavy but normal for a table.
- **Overall:** 9/10
- **Action:** Kept

### [31] `docs/pages/file-download/+Page.mdx` — "Multiple / nested downloads" intro (sentence 1)
- **Original:** "Return any number of `File`, `Blob`, or `download()` values anywhere in the response — in arrays, in nested objects, or mixed with regular data."
- **Clarity:** 9/10 — clear; the examples after the em-dash concretize "anywhere."
- **Naturalness:** 10/10
- **Overall:** 9/10
- **Action:** Kept

### [32] `docs/pages/file-download/+Page.mdx` — "Multiple / nested downloads" intro (sentence 2)
- **Original:** "Each one independently exposes its own `onProgress` / `cancel` / `saveTo*` methods on the client."
- **Clarity:** 9/10 — clear; "independently" + "its own" slightly doubles the same idea but reinforces correctly.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [33] `docs/pages/file-download/+Page.mdx` — "Limitations → One-shot reads"
- **Original:** "The streaming download can only be consumed once — calling `.stream()`, `.bytes()`, `.text()`, `.arrayBuffer()`, `.slice()`, or any `saveTo*` method a second time throws."
- **Clarity:** 9/10 — clear; "throws" without an object is idiomatic JS-doc shorthand for "throws an error."
- **Naturalness:** 9/10 — long method list but reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [34] `docs/pages/file-download/+Page.mdx` — "One-shot reads" blockquote
- **Original:** "If you need the data more than once, materialize it once via `dl.saveToMemory()` (or `saveToOpfs()` for large files) and reuse the returned `File` / `Blob`."
- **Clarity:** 9/10 — clear; "materialize" is precise and well-chosen here.
- **Naturalness:** 9/10 — idiomatic, concise.
- **Overall:** 9/10
- **Action:** Kept

---

## `docs/pages/file-upload/+Page.mdx` (MODIFIED — introduced sentences only)

### [35] `docs/pages/file-upload/+Page.mdx` — intro, first sentence
- **Original:** "Pass `File` or `Blob` arguments to a telefunction like any other value."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [36] `docs/pages/file-upload/+Page.mdx` — intro, second sentence
- **Original:** "A single file, multiple files, `File[]` arrays, or files mixed with other arguments — all are supported."
- **Clarity:** 9/10 — clear; the four list items vary in form (count vs. type vs. mixing) but each is concrete.
- **Naturalness:** 9/10 — the "— all are supported" tag is a natural Telefunc-voice closer.
- **Overall:** 9/10
- **Action:** Kept

### [37] `docs/pages/file-upload/+Page.mdx` — intro blockquote (changed)
- **Original:** "When a call contains files, Telefunc automatically switches from JSON to a binary format that streams file bytes without buffering."
- **Clarity:** 9/10 — clear; "a binary format" is appropriately general (the specifics live in "How it works").
- **Naturalness:** 9/10 — reads well; "switches from JSON to a binary format" is idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [38] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" intro
- **Original:** "Passing a `File` / `Blob` argument is one of several ways to move binary data with Telefunc:"
- **Clarity:** 9/10 — clear; sets up the table well.
- **Naturalness:** 9/10 — "move binary data" is concise and natural.
- **Overall:** 9/10
- **Action:** Kept

### [39] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, `File`/`Blob` Best-for cell
- **Original:** "Finite uploads — files, images, CSV imports"
- **Clarity:** 9/10 — clear fragment; "Finite" precisely contrasts with the "long-lived" row below.
- **Naturalness:** 9/10 — idiomatic table fragment.
- **Overall:** 9/10
- **Action:** Kept

### [40] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, ReadableStream Best-for cell
- **Original:** "Long-lived streams — video feed, sensor data, continuous audio"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [41] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, Channel Best-for cell
- **Original:** "Low-latency frames — skip the `await` for fire-and-forget"
- **Clarity:** 8/10 — clear, though "skip the `await` for fire-and-forget" assumes the reader connects it to the Backpressure column's "`await` each send"; in isolation it is slightly terse.
- **Naturalness:** 9/10 — idiomatic; "fire-and-forget" is standard.
- **Overall:** 8/10
- **Action:** Kept (table fragment; context resolves it)

### [42] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, BroadcastChannel Best-for cell
- **Original:** "Broadcast binary — video to multiple subscribers"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [43] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, Channel Backpressure cell
- **Original:** "Opt-in — `await` each send"
- **Clarity:** 9/10 — clear once paired with the link target; "Opt-in" + the mechanism is precise.
- **Naturalness:** 9/10 — fine fragment.
- **Overall:** 9/10
- **Action:** Kept

### [44] `docs/pages/file-upload/+Page.mdx` — "Choosing a method" table, BroadcastChannel Backpressure cell
- **Original:** "Publish-side only"
- **Clarity:** 8/10 — terse; meaning (backpressure applies only on the publishing side) is recoverable but compact for a reader new to the term.
- **Naturalness:** 9/10 — idiomatic label.
- **Overall:** 8/10
- **Action:** Kept (table fragment)

### [45] `docs/pages/file-upload/+Page.mdx` — code comment (Example, changed)
- **Original:** "// Stream to disk — constant memory, no matter the file size"
- **Clarity:** 9/10 — clear comment; "constant memory" reads as "constant memory usage," idiomatic in a code comment.
- **Naturalness:** 9/10 — natural code comment.
- **Overall:** 9/10
- **Action:** Kept (comment scan: no typo)

### [46] `docs/pages/file-upload/+Page.mdx` — "Upload with progress" intro
- **Original:** "Combine file upload with function passing to report progress while the server reads the stream."
- **Clarity:** 9/10 — clear; "while the server reads the stream" precisely states when progress fires.
- **Naturalness:** 9/10 — idiomatic; "Combine X with Y to Z" is a natural docs pattern.
- **Overall:** 9/10
- **Action:** Kept

### [47] `docs/pages/file-upload/+Page.mdx` — closing line of "Upload with progress" (EDITED)
- **Original:** "The file bytes stream while `onProgress()` updates the client."
- **Clarity:** 7/10 — "stream while … updates" leaves the destination of the stream implicit and pairs an intransitive "stream" with a parallel clause that is slightly ambiguous about ordering/concurrency.
- **Naturalness:** 8/10 — reads acceptably but is terse.
- **Overall:** 7/10
- **Action:** Edited
- **Edit:** "The file bytes stream to disk while `onProgress()` reports progress to the client."
- **Edit rating:** Clarity 9/10, Naturalness 9/10, Overall 9/10 — names the destination (to disk, matching the code that pipes to `fs.createWriteStream`) and makes the verb in the second clause concrete ("reports progress" instead of bare "updates"); not 10 because "stream to disk … reports progress to the client" repeats the direction-preposition pattern slightly.

### [48] `docs/pages/file-upload/+Page.mdx` — "Reading strategies" intro
- **Original:** "Each file argument is a standard `File` / `Blob` object:"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept (pre-existing wording, but it sits on a line touched by the diff; flawless either way)

### [49] `docs/pages/file-upload/+Page.mdx` — "Reading strategies" table, `file.stream()` Best-for cell (changed)
- **Original:** "Pipe to disk, S3, or any writable stream"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [50] `docs/pages/file-upload/+Page.mdx` — "Reading strategies" table, `file.arrayBuffer()` Best-for cell (changed)
- **Original:** "Process entire file at once"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [51] `docs/pages/file-upload/+Page.mdx` — "Reading strategies" table, `file.text()` Best-for cell
- **Original:** "Read text content"
- **Clarity:** 9/10 — clear fragment.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [52] `docs/pages/file-upload/+Page.mdx` — "Reading strategies" blockquote (changed)
- **Original:** "For large files, always use `file.stream()` — memory stays constant regardless of file size."
- **Clarity:** 9/10 — clear directive; "always" is strong but matches the advice.
- **Naturalness:** 9/10 — idiomatic guidance line.
- **Overall:** 9/10
- **Action:** Kept

### [53] `docs/pages/file-upload/+Page.mdx` — "Limitations" intro (sentence 1)
- **Original:** "File bytes flow directly from the HTTP stream to your code with zero internal buffering."
- **Clarity:** 9/10 — clear; "zero internal buffering" is precise.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [54] `docs/pages/file-upload/+Page.mdx` — "Limitations" intro (sentence 2)
- **Original:** "This makes uploads memory-efficient, with two trade-offs:"
- **Clarity:** 8/10 — clear, but "with two trade-offs" is appended as a loose modifier rather than a full clause, which reads slightly clipped.
- **Naturalness:** 8/10 — acceptable but the comma-plus-fragment construction is a touch informal for a lead-in.
- **Overall:** 8/10
- **Action:** Kept (clear and concise; the lead-in correctly sets up the two `###` subsections)

### [55] `docs/pages/file-upload/+Page.mdx` — "One-shot reads" body (changed)
- **Original:** "Each file can only be read once — calling `.stream()`, `.text()`, or `.arrayBuffer()` a second time throws."
- **Clarity:** 9/10 — clear; bare "throws" is idiomatic JS-doc shorthand.
- **Naturalness:** 9/10 — reads naturally.
- **Overall:** 9/10
- **Action:** Kept

### [56] `docs/pages/file-upload/+Page.mdx` — "One-shot reads" blockquote (changed)
- **Original:** "If you need the data more than once, buffer it into a variable first."
- **Clarity:** 9/10 — clear; "buffer it into a variable" is concrete.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [57] `docs/pages/file-upload/+Page.mdx` — "Read in order" intro (changed)
- **Original:** "Multiple file arguments must be read in signature order:"
- **Clarity:** 9/10 — clear; "signature order" is precise and defined by the following example.
- **Naturalness:** 9/10 — concise, idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [58] `docs/pages/file-upload/+Page.mdx` — code comment "Correct order" (changed)
- **Original:** "// ✅ Correct order"
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept (comment scan: no typo)

### [59] `docs/pages/file-upload/+Page.mdx` — code comment "Out of order — file1 is discarded" (changed)
- **Original:** "// ❌ Out of order — file1 is discarded"
- **Clarity:** 10/10 — clear and states the consequence.
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept (comment scan: no typo)

### [60] `docs/pages/file-upload/+Page.mdx` — "Read in order" blockquote (changed)
- **Original:** "All files of a single call share one forward-only HTTP stream. Reading `file2` before `file1` would require buffering `file1` in memory."
- **Clarity:** 9/10 — clear; "forward-only" precisely justifies the constraint.
- **Naturalness:** 9/10 — idiomatic; two clean sentences.
- **Overall:** 9/10
- **Action:** Kept

### [61] `docs/pages/file-upload/+Page.mdx` — concurrency lead-in (changed)
- **Original:** "You can start reads concurrently — they stream in the correct order automatically:"
- **Clarity:** 9/10 — clear; resolves the apparent tension with "read in order" by clarifying that ordering is handled for you.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [62] `docs/pages/file-upload/+Page.mdx` — code comment "both start in parallel, streamed in order" (changed)
- **Original:** "// ✅ Works — both start in parallel, streamed in order"
- **Clarity:** 9/10 — clear; "streamed in order" reinforces the guarantee.
- **Naturalness:** 9/10 — natural code comment.
- **Overall:** 9/10
- **Action:** Kept (comment scan: no typo)

### [63] `docs/pages/file-upload/+Page.mdx` — "How it works" blockquote (changed)
- **Original:** "You can skip this section — read it only if you're curious."
- **Clarity:** 10/10
- **Naturalness:** 10/10
- **Overall:** 10/10
- **Action:** Kept

### [64] `docs/pages/file-upload/+Page.mdx` — "How it works" intro (changed)
- **Original:** "Telefunc uses a custom binary protocol — no multipart/form-data, no internal buffering."
- **Clarity:** 9/10 — clear; the two "no …" clauses precisely state what it is not.
- **Naturalness:** 9/10 — idiomatic Telefunc voice.
- **Overall:** 9/10
- **Action:** Kept

### [65] `docs/pages/file-upload/+Page.mdx` — "How it works" step 1 (changed)
- **Original:** "The client serializes the call into a binary request: metadata first, followed by raw file bytes."
- **Clarity:** 9/10 — clear; the colon + ordering is precise.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [66] `docs/pages/file-upload/+Page.mdx` — "How it works" step 2 (changed)
- **Original:** "The server parses metadata and creates lazy `File`/`Blob` objects that **reference the HTTP body stream** without reading it."
- **Clarity:** 9/10 — clear; "lazy … reference … without reading it" precisely conveys deferral.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [67] `docs/pages/file-upload/+Page.mdx` — "How it works" step 3 (changed)
- **Original:** "When your telefunction calls `file.stream()`, bytes are pulled directly from the HTTP stream on demand."
- **Clarity:** 9/10 — clear; "on demand" reinforces laziness.
- **Naturalness:** 9/10 — idiomatic.
- **Overall:** 9/10
- **Action:** Kept

### [68] `docs/pages/file-upload/+Page.mdx` — "How it works" closing line (changed)
- **Original:** "File bytes only flow through memory when you read them — and if you stream to disk, memory consumption is constant regardless of file size."
- **Clarity:** 9/10 — clear; precise statement of the memory guarantee.
- **Naturalness:** 9/10 — idiomatic; the em-dash + conditional reads naturally.
- **Overall:** 9/10
- **Action:** Kept

---

## Summary

- **Sentences reviewed:** 68 (34 in file-download NEW; 34 introduced/changed in file-upload, including reviewed code comments).
- **Kept:** 67
- **Edited:** 1 (file-upload [47])
- **Second-PR candidates:** 0

No MDX/JSX, code logic, headings, anchors, or URLs were altered. The single edit changed prose wording only; the surrounding region was re-read and confirmed intact. No comment typos were found; comments were scanned and rated where they carried explanatory prose.
