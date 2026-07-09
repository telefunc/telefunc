# Docs review — PR #436 (`feat: add Room`)

Sentence-level review of the documentation introduced by PR #436 (`feat: add Room — multi-party rooms with presence and member management`), base `main` (`81882c7`) → head `15fb169`.

## Files reviewed

| File | What changed |
|---|---|
| `docs/pages/room/+Page.mdx` | **New page** (411 lines) — the bulk of this review |
| `docs/pages/stream/+Page.mdx` | New `Primitive: Room` section + one list item |
| `docs/pages/channel/+Page.mdx` | One cross-link blockquote |
| `docs/pages/redis/+Page.mdx` | One sentence appended to the intro |
| `docs/headings.ts` | Nav entry (labels only — not prose, see end) |

## Rating method

Each sentence gets three sub-scores (1–10) mapped to the three requested criteria, plus one **Overall** headline rating.

- **C — Clarity**: crystal clear, zero ambiguity, no fuzzy words. 10 = the meaning cannot be misread.
- **A — Accessibility (self-containedness)**: can it be understood with only a *vague, high-level* idea of what a room is? **Higher = needs less prior reading.** Low A = leans on other pages (channel/stream) or on a dense concept introduced earlier that the reader may not have absorbed.
- **N — Naturalness**: familiar words, natural phrasing, nothing weird or awkward.

Overall is holistic and weighted toward the weakest axis (a sentence that is unclear *or* unreadable-without-prior-reading *or* awkward is a weak doc sentence, regardless of the other axes).

Terse reference material — method-signature table cells, code, code comments, boilerplate labels — is assessed in grouped notes rather than rated as prose sentences, since they are fragments by design.

---

## Summary

The page is well above average — it clearly went through a clarity/naturalness pass, and the mental-model-first structure (three objects, three lanes) is excellent. Most sentences score 7–9. The recurring drags are **not** grammar or naturalness; they are:

1. **Reliance on channel-page vocabulary** (criterion 2). Sentences lean on `.client`, `Broadcast.*`, "adapter", "transport", "KV store", "pub/sub key", "upstream", "observed" without a local one-line reminder. An expert glides through; a reader with only a vague idea of rooms stalls.
2. **One load-bearing concept — "observed" / "live local view" — is introduced inside a dense multi-clause note** and then relied on repeatedly ("resyncs an unobserved room", "when your side observes the room"). It deserves a crisper, earlier definition.
3. **The phrase "membership granted through the returned room / through it"** recurs 3× in the authorization/guard prose. It is abstract on first read and never plainly restated as "every join that came from this room object."
4. **A handful of long, multi-parenthetical sentences** (the getters note, the "text is never filtered" note) pack 3–4 clauses and should be split.

None of these are correctness problems; they are "reader must re-read" problems, which is exactly what criterion 2 targets.

### Lowest-rated sentences (fix these first)

| ID | Overall | One-line issue |
|---|---|---|
| R‑MODEL‑5 | 6 | `.client variant` assumes the channel page |
| R‑INST‑2 | 6 | Getters note: 4 clauses + undefined "serialized"/"observed" |
| R‑MSG‑8 | 6 | "Text is never filtered…" — one 45-word sentence, 3 parentheticals |
| R‑VID‑1 | 6 | Isolated mode: "upstream pub/sub key", "publish contention", "coordinator" stacked |
| R‑PGP‑3 | 6 | "from any membership granted through the returned room" — abstract |
| R‑SCALE‑1 | 6 | "broadcast adapter's KV store … over its pub/sub" — 3 undefined infra nouns |
| R‑SIZE‑Keys | 6 | "Keys" bullet: adapter subscription / observing node / upstream stacked |
| R‑STREAM‑2 | 7 | "you can use a room, see:" — awkward splice |

---

## `docs/pages/room/+Page.mdx`

### Intro

**R‑INTRO‑1** — "**Environment**: server & client."
`C 9 · A 9 · N 8 · Overall 9` — Boilerplate label reused across stream pages; unambiguous. Not really a sentence, but consistent and helpful.

**R‑INTRO‑2** — "`Room` adds multi-party **rooms** on top of channels: presence (who's in the room), participant metadata, per-member subscriptions, private messages, and admin controls."
`C 9 · A 8 · N 9 · Overall 9` — Strong opening. The gloss "(who's in the room)" earns the term "presence" immediately. "on top of channels" is linked, so a vague reader is fine. "per-member subscriptions" is the only mildly abstract item this early, but it's a scannable feature list, not a claim to parse.

**R‑INTRO‑3** — "Use rooms for video chat, collaborative editing, game lobbies — anything where *who is connected* matters."
`C 10 · A 10 · N 10 · Overall 10` — Concrete, self-contained, natural. Model sentence: the reader knows if rooms are for them after this line.

**R‑INTRO‑4** — "When you only need messaging or pub/sub, use channels instead."
`C 9 · A 8 · N 9 · Overall 9` — Clear steer. "pub/sub" is jargon but linked and paired with the plainer "messaging". Good "when NOT to use this" signpost.

**Comparison table (feature rows).** Grouped note — `C 8 · A 8 · N 8 · Overall 8`. Row labels are clear fragments. "Server-client pipe" is slightly jargon-flavored vs. the plainer labels around it; "Topic-based pub/sub" assumes the pub/sub concept. Acceptable for a scannable capability matrix.

### The model

**R‑MODEL‑1** — "Three objects:"
`C 9 · A 10 · N 9 · Overall 9` — Fine lead-in. The "three objects / three lanes" framing is the page's best structural idea.

**R‑MODEL‑2** — "**`Room`** — the shared space. Created and managed on the server (`Room.*` statics); whoever holds it (server or client) can observe live membership, events, and message streams."
`C 7 · A 7 · N 8 · Overall 7` — "the shared space" is a great one-liner. Two soft spots: "whoever holds it" introduces the load-bearing verb *hold* with no definition (it recurs later as a lifecycle rule), and "`Room.*` statics" names a concept only defined two sections down. Understandable, but "hold" is doing quiet heavy lifting.

**R‑MODEL‑3** — "**`LocalParticipant`** — *you*, returned by `join()`. Your identity in the room: you publish, whisper, and update your metadata through it."
`C 8 · A 8 · N 9 · Overall 8` — "*you*" is a delightful, instantly-clear gloss. "whisper" is introduced as a verb before it's defined, but it's self-evidently "private message", so the cost is near zero. Natural.

**R‑MODEL‑4** — "**`RemoteParticipant`** — *everyone else*, as seen through a room: subscribe to one member's messages, watch their metadata, notice when they leave."
`C 8 · A 8 · N 9 · Overall 8` — "*everyone else*" mirrors "*you*" beautifully. "as seen through a room" is faintly abstract but the three concrete verbs (subscribe/watch/notice) ground it immediately.

**R‑MODEL‑5** — "The three objects are one type each, same on server and client — a `Room` or `LocalParticipant` can be returned from a telefunction as-is, no `.client` variant."
`C 6 · A 5 · N 7 · Overall 6` — **Lowest in this section.** "one type each" is awkward phrasing. The real problem is criterion 2: "no `.client` variant" only lands if you've read the channel page, where `.client` is the flip-the-directions trick. A room-first reader has no idea what a `.client` variant *is*, so the reassurance "you don't need one" reassures nothing. *Suggested:* add a four-word gloss — "no `.client` variant (unlike channels, which flip message direction)" — or drop the reference here.

**R‑MODEL‑6** — "Everything below works identically on both sides unless marked otherwise."
`C 9 · A 9 · N 9 · Overall 9` — Clear, useful scope-setter, natural.

**Message-lanes table.** Grouped note — `C 8 · A 8 · N 8 · Overall 8`. Compact and well-chosen ("Typical use" column is great). "lane" as a metaphor is slightly unusual but used consistently and is intuitive.

### Quick start

**R‑QS‑1** — "Like the built-in `Date`, `Room` is both a value and a type: the statics (`Room.get()`) and the instance type (`const lobby: Room`)."
`C 8 · A 7 · N 8 · Overall 8` — Clever, precise analogy that a TS dev gets instantly. Costs a little on accessibility because it assumes the reader knows `Date` is simultaneously a value and a type in TS — a real but reasonable assumption for this audience.

### Rooms

**R‑ROOM‑1** — "`Room.*` statics are the server-side entry point — like `Broadcast.*`."
`C 8 · A 7 · N 9 · Overall 8` — Clear. "statics" is dev-standard. "like `Broadcast.*`" is a helpful analogy for channel-page readers and harmless for others.

**R‑ROOM‑2** — "Clients get a room when a telefunction returns it."
`C 9 · A 9 · N 9 · Overall 9` — Crisp, self-contained, answers the obvious "how does a client get one?" question.

**Room statics table + `RoomOptions` block.** Grouped note — `C 9 · A 8 · N 9 · Overall 8`. The Method/Returns/Throws layout is excellent and unambiguous. Code comments "capacity hint (default: Infinity) — not enforced" and "per-member pub/sub keys (default: false) — fixed at creation" are clear; the second assumes "pub/sub key" but it's an options reference, not first-read prose.

**R‑ROOM‑3** — "`Room.join(id, meta?, options?)` is a shorthand for `(await Room.get(id)).join(meta, options)` — for telefunctions that only need the participant, not the room."
`C 9 · A 9 · N 9 · Overall 9` — The literal code-equivalence removes all ambiguity. Rationale clause is a nice touch.

**R‑ROOM‑4** — "`Room.update()` does a **full replace**: omitted options reset to their defaults (e.g. omitting `size` resets it to `Infinity`) — except `isolated`, which is fixed at creation. Merging metadata is your job."
`C 9 · A 9 · N 8 · Overall 9` — Excellent: names the surprising behavior (full replace), gives a concrete example, states the exception, and warns the reader. "is your job" is casual but perfectly clear and fits the voice.

**R‑ROOM‑5** — "`Room.close()` disconnects all participants, fires `onClose()` on every observer, and removes the room. Afterwards `Room.get(id)` throws."
`C 9 · A 9 · N 9 · Overall 9` — Three effects listed plainly + the post-condition. Clean.

**R‑ROOM‑6** — "`size` is a **hint**: Telefunc tracks it (`count`, `isFull`, `onFull()`) but never rejects a join — you decide:"
`C 9 · A 9 · N 9 · Overall 9` — Sets the expectation ("never rejects") and hands control to the reader. Natural.

### `Room` instance

**Instance methods table.** Grouped note — `C 8 · A 7 · N 8 · Overall 8`. Mostly clean fragments. Two cells lean forward onto the not-yet-defined "observed" concept: `getParticipants()` — "(on the server, resyncs an unobserved room first — see the note below)" and `getParticipant(id)` — "reads the live local view." Both are forward references to R‑INST‑2. `onClose(cb)` — "or, on the client, the connection is gone" is clear and helpful.

**R‑INST‑1** — "State getters: `id`, `meta`, `size`, `count`, `isEmpty`, `isFull`, `isClosed`."
`C 9 · A 9 · N 9 · Overall 9` — Plain list, fine.

**R‑INST‑2** — "Getters reflect the live local view: while the room is **observed** — a listener is attached, a participant joined through it, or it was serialized to a client — they're kept fresh by the room's event stream; otherwise they're a snapshot from the last sync."
`C 6 · A 5 · N 7 · Overall 6` — **This is the definition of "observed", and it's overloaded.** One sentence carries: the term "observed", its three-part definition (em-dash aside), the mechanism ("event stream"), and the fallback ("snapshot from the last sync") — plus the undefined verb "serialized". A reader with a vague understanding cannot hold all of this at once. Because later sentences depend on "observed", this note is load-bearing and should be the *clearest* on the page, not among the densest. *Suggested:* split into two sentences and lead with the plain idea — "A room is **observed** when something is watching it: a listener attached, a member joined through it, or it was sent to a client. While observed, getters stay live; otherwise they show the last synced snapshot."

**R‑INST‑3** — "On the server, `getParticipants()` always resyncs an unobserved room first, so the list is current."
`C 8 · A 6 · N 8 · Overall 7` — Clear *if* you absorbed "observed/unobserved" from R‑INST‑2. Reads well; the accessibility hit is entirely inherited from the dense definition above it.

### Authorization

**R‑AUTH‑1** — "Admin operations live on the `Room.*` statics — not on the instance — so a room handed to clients carries no privileged methods."
`C 8 · A 8 · N 9 · Overall 8` — Nice security-by-construction argument, stated as cause→effect. Relies on the statics-vs-instance distinction, which is established by now.

**R‑AUTH‑2** — "Wrap each admin operation in its own telefunction that authorizes the caller:"
`C 9 · A 9 · N 9 · Overall 9` — Direct, actionable instruction.

**R‑AUTH‑3** (Warning) — "**Room IDs are capabilities** — a telefunction that returns `await Room.get(roomId)` hands the caller the full room API: join, subscribe to all messages, read the member list."
`C 8 · A 8 · N 9 · Overall 8` — "capabilities" is a security-of-art term, but the em-dash spells out exactly what it grants, so the meaning is safe. Mirrors "Keys are capabilities" on the channel page — good consistency.

**R‑AUTH‑4** — "Derive room IDs from **authorized server-side context** (e.g. the user ID from `getContext()`), or verify access before returning the room:"
`C 9 · A 8 · N 9 · Overall 9` — Two concrete mitigations, each with an example. Clear.

### Participants — `LocalParticipant`

**R‑LP‑1** — "Your own handle, returned by `join()`."
`C 8 · A 9 · N 8 · Overall 8` — "handle" is mild jargon but universally understood by devs. Fine.

**R‑LP‑2** — "Room-wide messages are received on the room and on `RemoteParticipant`; only private messages addressed to you arrive here (`listen()`)."
`C 7 · A 7 · N 8 · Overall 7` — The routing is correct but compressed: "received on the room and on `RemoteParticipant`" and "arrive here" ask the reader to track three receiving surfaces in one line. "here" (= on the `LocalParticipant`) is a slight deixis puzzle. Understandable, but a reader may re-read once.

**LocalParticipant methods table.** Grouped note — `C 8 · A 8 · N 8 · Overall 8`. The `listen(cb)` cell — "`from` is the verified sender, `null` for room-authored messages (`Room.send()`)" — is dense for a table but accurate. "unlisten function" is a coined term, but it's consistent with the channel page. `onLeave` — "You left — voluntarily, kicked, room closed, or disconnected." is excellent: four causes in five words.

**R‑LP‑3** — "Read-only properties: `id`, `meta`, and `selfDelivery`."
`C 9 · A 9 · N 9 · Overall 9` — Fine.

**R‑LP‑4** — "`selfDelivery` (default: `true`, set at `join()`): whether the messages you publish are delivered back to the room object on your side (e.g. your own `room.subscribe()` callback receiving your own messages)."
`C 8 · A 8 · N 8 · Overall 8` — "delivered back to the room object on your side" is abstract, but the parenthetical example nails it precisely. Good use of example-as-definition.

**R‑LP‑5** — "Turn it off e.g. for video, where you don't want your own frames back: `join({ name }, { selfDelivery: false })`."
`C 8 · A 9 · N 8 · Overall 8` — The video motivation makes it instantly concrete. "e.g." mid-sentence is slightly clunky but common in these docs.

**R‑LP‑6** — "A participant's membership follows its holder: when the client holding a `LocalParticipant` (or the room it joined through) disconnects, the participant leaves the room."
`C 7 · A 7 · N 8 · Overall 7` — "membership follows its holder" is an abstract headline, but the "when… disconnects" clause makes it concrete. "the room it joined through" is a small tongue-twister. This is where "holder" finally pays off — worth defining "hold" earlier (see R‑MODEL‑2) so this lands cleanly.

### Participants — `RemoteParticipant`

**R‑RP‑1** — "Another room member, as seen through a room."
`C 8 · A 8 · N 7 · Overall 8` — Clear. "as seen through a room" is the third use of this phrasing; it's fine but starting to feel like a verbal tic across R‑MODEL‑4 / R‑RP‑1.

**RemoteParticipant methods table + `joinedAt` note.** Grouped note — `C 9 · A 9 · N 9 · Overall 9`. Clean, parallel to the `LocalParticipant` table. "(Unix epoch ms)" removes ambiguity about the timestamp format — good.

### Messaging — Room-wide

**R‑MSG‑1** — "**Room-level** — `room.subscribe()` receives every participant's messages, with sender identity. Use it when all messages are processed the same way (chat, notifications):"
`C 9 · A 9 · N 9 · Overall 9` — Clear contrast setup, with a "use it when" heuristic and examples. Excellent pattern.

**R‑MSG‑2** — "**Per-member** — `member.subscribe()` receives only that member's messages. Use it when each member's messages need separate processing (e.g. one decoder per member):"
`C 9 · A 9 · N 9 · Overall 9` — Parallel to R‑MSG‑1, equally clear. The "one decoder per member" example foreshadows the video recipe nicely.

**R‑MSG‑3** — "Both receive from the same stream — a member's `publish()` reaches room-level subscribers (with `from`) and that member's subscribers."
`C 8 · A 8 · N 8 · Overall 8` — Resolves the natural "do these two paths conflict?" question. "(with `from`)" is terse shorthand for "including sender identity" but recoverable from R‑MSG‑1.

**R‑MSG‑4** (note) — "Per-member binary subscriptions also control **wire delivery**: a client receives a member's binary stream only while it subscribes to that member (or to the whole room)."
`C 7 · A 6 · N 8 · Overall 7` — Solid explanation of a subtle behavior. "wire delivery" / "on the wire" is network jargon; a plain reader may not know "the wire" = the network connection. The rule itself ("only while it subscribes") is clear.

**R‑MSG‑5** — "Text is never filtered on the wire: the text stream also carries the control events (join/leave/metadata) that keep every holder's live view current (`count`, `getParticipants()`, event callbacks) — and text payloads are small compared to binary, so filtering them would save little bandwidth."
`C 6 · A 5 · N 7 · Overall 6` — **The densest sentence in Messaging.** ~45 words, three parentheticals, and it depends on "the wire", "control events", "holder", and "live view" all at once. The *reasoning* is good and worth keeping; the *packaging* is too much for one sentence. *Suggested:* split — "Text is never filtered on the wire. The text stream also carries control events (join/leave/metadata) that keep every holder's live view current, so it must reach everyone. And text is tiny next to binary — filtering it would save almost no bandwidth."

### Messaging — Private messages

**R‑PM‑1** — "`send()` delivers to exactly one participant — a whisper, an invite, a game move."
`C 9 · A 9 · N 10 · Overall 9` — "exactly one" is precise; the three examples are vivid and natural. Great.

**R‑PM‑2** — "Privacy is transport-level: the message travels over the target's private inbox key, so no other participant's connection ever receives it (it also never appears on `subscribe()` streams)."
`C 8 · A 7 · N 8 · Overall 8` — Strong: it explains *why* privacy holds (not just asserts it). "transport-level" and "inbox key" are jargon, but the causal clause ("so no other participant's connection ever receives it") makes the guarantee concrete regardless.

**R‑PM‑3** — "`send()` accepts a `RemoteParticipant` or a participant ID. Sending to an unknown or departed participant throws."
`C 9 · A 9 · N 9 · Overall 9` — Clear API contract + error behavior. "departed" is a nice, precise word.

**R‑PM‑4** — "`from` is the **verified** sender: the live `RemoteParticipant` when your side observes the room, or an `{ id, meta }` snapshot otherwise."
`C 7 · A 6 · N 8 · Overall 7` — Precise, but again pivots on "observes the room" (the R‑INST‑2 concept) and on the live-vs-snapshot duality. A reader who skipped the getters note will not know why they'd get a snapshot instead of a live participant.

**R‑PM‑5** — "Telefunc rejects sends from non-members, so the sender identity can't be spoofed; `null` is reserved for room-authored whispers (`Room.send()`)."
`C 8 · A 8 · N 8 · Overall 8` — Two facts joined by a semicolon; both clear. "spoofed" is common. Good.

**R‑PM‑6** — "To authorize whispers, attach a guard when granting the room: `Room.get(id, { onSend })` — see the recipe."
`C 7 · A 7 · N 8 · Overall 7` — "attach a guard when granting the room" — "granting the room" (= returning it from a telefunction) is used here for the first time without definition. The link to the recipe softens it. First appearance of the recurring "grant" phrasing (see R‑PGP‑3, R‑PGP‑4).

### Messaging — Room-authored

**R‑RA‑1** — "Messages don't have to come from a participant — the room itself can speak."
`C 9 · A 9 · N 10 · Overall 9` — "the room itself can speak" is a memorable, natural framing of an abstract idea. Excellent.

**R‑RA‑2** — "`Room.announce()` broadcasts to everyone; `Room.send()` whispers to one participant."
`C 9 · A 9 · N 9 · Overall 9` — Perfect parallelism; "broadcasts to everyone" vs "whispers to one" is instantly clear.

**R‑RA‑3** — "Neither requires the caller to join, and receivers can't confuse them with participant messages:"
`C 8 · A 8 · N 8 · Overall 8` — Clear; states two properties. "the caller" is unambiguous here (the server code calling the static).

**R‑RA‑4** — "Announcements arrive only on `onAnnounce()` — never on `subscribe()` streams, which stay strictly participant-authored with a real `from`."
`C 8 · A 8 · N 8 · Overall 8` — Draws the boundary cleanly. "stay strictly participant-authored with a real `from`" is terse but, by this point in the page, well-supported.

### Recipes — Video chat

**R‑VID‑1** — "**Isolated mode** (`isolated: true`) gives each participant their own upstream pub/sub key, removing publish contention between members — useful on platforms that map each key to a separate coordinator (e.g. Cloudflare Durable Objects)."
`C 6 · A 5 · N 7 · Overall 6` — **Three unexplained infra nouns stacked in one clause**: "upstream pub/sub key", "publish contention", "separate coordinator". Each is meaningful to someone who has built on Durable Objects and opaque to everyone else. For a *recipe* (where the reader just wants to build video chat), the performance rationale buries the how-to. The mechanism belongs in "How it works" / "Scale envelope"; the recipe needs only "each participant gets their own stream — better for high-throughput media like video." Naturalness is fine; accessibility is the problem.

**R‑VID‑2** — "Clients don't see the difference."
`C 9 · A 9 · N 10 · Overall 9` — Short, reassuring, natural. Answers "do I have to change client code?" Nice.

**Code comment** "// don't echo our own frames" — clear and well-placed; ties `selfDelivery: false` to the concrete reason.

### Recipes — Server-side moderation

**R‑MOD‑1** — "The server can observe without joining:"
`C 9 · A 9 · N 9 · Overall 9` — Compact, clear, and a genuinely useful capability to highlight.

### Recipes — Policy-gated private messages

**R‑PGP‑1** — "To authorize whispers (e.g. friends-only DMs), pass an `onSend` guard to the `Room.get()` call that grants the room."
`C 8 · A 8 · N 8 · Overall 8` — "friends-only DMs" makes the use case instantly concrete. "the `Room.get()` call that grants the room" reuses "grant" but is anchored to a specific call, so it's recoverable.

**R‑PGP‑2** — "It's declared in the telefunction, so it can close over the request context — e.g. the authenticated user from `getContext()`."
`C 7 · A 7 · N 8 · Overall 7` — "close over" is a closures term; many JS devs know it, but it's the kind of word criterion 3 flags as "expert-only". The example rescues the meaning. Consider "so it has access to the request context" as a plainer alternative.

**R‑PGP‑3** — "It runs before every private-message delivery from any membership granted through the returned room; throwing rejects the sender's `send()` with the error:"
`C 6 · A 6 · N 6 · Overall 6` — "from any membership granted through the returned room" is the most abstract phrasing on the page — it stacks "membership" + "granted through" + "returned room" into one noun phrase the reader must decode. "throwing rejects the sender's `send()`" is terse. *Suggested:* "It runs before Telefunc delivers any private message sent by someone who joined through this room object. If the guard throws, that `send()` rejects with the error."

**R‑PGP‑4** — "The guard is scoped to the **returned room instance**: `Room.get(id, { onSend })` covers every membership granted through it — server-side `join()`s and client-side `join()`s on it alike."
`C 7 · A 6 · N 7 · Overall 6` — Third use of "membership granted through it". The bolding of "returned room instance" helps, and the "server-side/client-side alike" clarification is valuable, but "join()s" (pluralized method call) reads awkwardly. Same fix as R‑PGP‑3: say "every join made through this room object."

**R‑PGP‑5** — "`Room.send()` (room-authored) is never guarded."
`C 9 · A 9 · N 9 · Overall 9` — Short, unambiguous edge-case note. Good.

**R‑PGP‑6** — "Guards live on the statics, so they're necessarily server-declared — authorization code can't run on the clients it polices."
`C 8 · A 8 · N 8 · Overall 8` — The reasoning ("can't run on the clients it polices") is a genuinely clarifying, quotable justification. "necessarily" is a touch formal; "polices" is a nice verb.

### Scaling & multiple servers

**R‑SCALE‑1** — "Room state lives in the broadcast adapter's KV store, and room events travel over its pub/sub — so rooms scale exactly like `Broadcast` does:"
`C 7 · A 5 · N 8 · Overall 6` — Three infra nouns with no local gloss — "broadcast adapter", "KV store", "its pub/sub" — in the section's topic sentence. For a reader who hasn't internalized the channel page's adapter/transport model, this is the wall. The payoff ("scale exactly like `Broadcast`") is great *if* you know how Broadcast scales. Consider a half-sentence gloss: "the same store and pub/sub bus your broadcasts already use."

**R‑SCALE‑2** — "**Single server**: the default in-memory adapter works out-of-the-box."
`C 9 · A 8 · N 9 · Overall 8` — Clear; "adapter" is jargon but "in-memory" + "out-of-the-box" carry the meaning.

**R‑SCALE‑3** — "**Multiple servers**: install a cross-instance transport and every server sees the same rooms."
`C 8 · A 8 · N 9 · Overall 8` — "every server sees the same rooms" is a great plain-language payoff. "cross-instance transport" is jargon but immediately illustrated by the next bullet.

**R‑SCALE‑4** — "`@telefunc/redis` supports rooms out-of-the-box (state in Redis keys, events over Redis Pub/Sub, Cluster included), and so does the Cloudflare integration (state in Workers KV, events over Durable Objects)."
`C 8 · A 8 · N 8 · Overall 8` — Dense but appropriately so — it's a spec line for readers who already picked a backend. Parallel structure (state / events for each) is clean.

**R‑SCALE‑5** — "**Custom transports**: implement the KV methods (`get()`, `set()`, `delete()`, `keys()`) on your `BroadcastTransport`, backed by a store all server instances can reach."
`C 8 · A 7 · N 8 · Overall 8` — Precise instruction for implementers. "backed by a store all server instances can reach" is a clear, plain constraint.

**R‑SCALE‑6** — "Until then, room operations fail with a clear error."
`C 9 · A 9 · N 9 · Overall 9` — Reassuring failure-mode note; "fail with a clear error" preempts a support question.

### Sizing rooms

**R‑SIZE‑0** — "What each mechanism costs, so you can size rooms deliberately:"
`C 8 · A 8 · N 6 · Overall 7` — Content is clear, but as a section lead-in it's a verbless fragment that reads slightly abrupt ("What each mechanism costs…" hanging). "size rooms deliberately" is good. *Suggested:* "Here's what each mechanism costs, so you can size rooms deliberately:"

**R‑SIZE‑Text** — "**Text delivery** — every connection holding the room receives its control events and text data. Per-connection cost is `messages/s × message size`; a busy chat room (20 messages/s × 300 B) comes to ~6 KB/s per connection. Server-side relay buffers are byte-capped with FIFO eviction: a client that falls behind loses the oldest buffered messages first, and server memory stays bounded."
`C 8 · A 6 · N 8 · Overall 7` — Excellent that it gives a *worked number* (~6 KB/s) — that's exactly what a sizing section should do. "byte-capped with FIFO eviction" is jargon, but the plain-English restatement right after ("loses the oldest buffered messages first") fully rescues it. Accessibility dips only because "control events" and "holding the room" assume earlier concepts.

**R‑SIZE‑Binary** — "**Binary delivery** — member-selective on the wire and, in isolated mode, upstream too: cost scales with what each client actually watches, not with room size."
`C 7 · A 6 · N 7 · Overall 7` — The payoff ("cost scales with what each client actually watches, not with room size") is crisp and valuable. The lead-in ("member-selective on the wire and, in isolated mode, upstream too") is compressed jargon; the reader gets the *conclusion* even if the mechanism is fuzzy, which saves it.

**R‑SIZE‑Snapshot** — "**Membership snapshot** — serializing a room to a client ships the full member list. That's fine up to a few thousand members (live video, game, and collaboration rooms); Discord-scale member lists belong in your database, with the room tracking only live participants."
`C 8 · A 7 · N 9 · Overall 8` — "Discord-scale" is a superb, instantly-calibrating reference point, and "belong in your database" gives concrete guidance. "serializing… ships" is mild jargon but the numbers and examples carry it.

**R‑SIZE‑Presence** — "**Presence upkeep** — each server refreshes its own members' liveness timestamps and scans the room's member records to reap dead members: `O(members)` KV reads per server every 30s. That scales comfortably to thousands of members per room on Redis or Workers KV."
`C 7 · A 6 · N 8 · Overall 7` — Precise cost model with a reassuring bound ("scales comfortably to thousands"). "liveness timestamps", "reap dead members", "`O(members)` KV reads" are jargon-dense, but this is explicitly the deep-dive sizing section, so the audience is opted-in.

**R‑SIZE‑Keys** — "**Keys** — the default shared mode uses one pub/sub key per room (one adapter subscription per observing node); `isolated: true` moves data to per-member keys, removing publish contention and letting each node subscribe upstream only to the members its clients watch."
`C 6 · A 5 · N 7 · Overall 6` — **Most jargon-dense bullet on the page**: "pub/sub key", "adapter subscription", "observing node", "publish contention", "subscribe upstream" — five infra terms in one sentence. Correct and complete for an infra reader, but even the sizing-section audience has to work. Unlike R‑SIZE‑Text, there's no plain-English restatement to anchor it.

### How it works

Grouped context: this section is explicitly the mechanism deep-dive, so a higher jargon tolerance is appropriate; scores weight clarity/naturalness over accessibility.

**R‑HIW‑Key** — "**One pub/sub key per room.** Presence events and data flow through the room's `Broadcast` key. In isolated mode, data moves to per-member keys while presence stays on the room key."
`C 8 · A 7 · N 8 · Overall 8` — Clear mechanism statement. The shared-vs-isolated contrast is well drawn.

**R‑HIW‑Frame** — "**Binary framing.** Binary messages are prefixed with the sender's 16-byte participant ID — fixed-size, so no length field is needed."
`C 8 · A 8 · N 8 · Overall 8` — "fixed-size, so no length field is needed" is a satisfying, self-contained bit of reasoning. Good.

**R‑HIW‑Crash** — "**Crash-safe presence.** Graceful departures (leave, kick, close, disconnect) propagate instantly as events. To guard against hard crashes, the server owning a participant refreshes a liveness timestamp on the participant's member record every 30 seconds; members whose owner stops refreshing are reaped — and their leave announced — after 2 minutes."
`C 8 · A 7 · N 8 · Overall 8` — Good graceful-vs-crash contrast with concrete intervals (30s / 2min). "reaped" is jargon but the surrounding mechanism makes it obvious.

**R‑HIW‑Lazy** — "**Lazy subscriptions.** A room subscribes to its pub/sub key only while observed. Binary delivery is member-selective: clients declare which members they're listening to, and the server relays only those members' binary streams (in isolated mode, it doesn't even subscribe upstream to the rest)."
`C 8 · A 6 · N 8 · Overall 7` — Clear and well-structured. Leans on "observed" and "subscribe upstream" but that's expected in this section.

---

## `docs/pages/stream/+Page.mdx`

**R‑STREAM‑1** — "`Room`: multi-party rooms with presence — who's connected, join/leave events, per-member streams."
`C 9 · A 9 · N 9 · Overall 9` — Parallel to the other primitive list items; the "who's connected" gloss makes "presence" free. Clean.

**R‑STREAM‑2** — "For multi-party use cases where *who is connected* matters — chat, game lobbies, video calls, collaboration — you can use a room, see:"
`C 7 · A 8 · N 6 · Overall 7` — Good content and examples, but "you can use a room, see:" is an awkward comma splice — the sentence makes a point, then tacks a "see:" pointer onto the same clause. *Suggested:* "…you can use a room:" (drop "you can use a room, see" and just lead the link) or split into two sentences.

**R‑STREAM‑3** (note) — "Built on top of channels: a `Room` adds presence, participant metadata, private messages, and admin controls."
`C 9 · A 9 · N 9 · Overall 9` — Clear, self-contained one-liner; consistent with the room page intro and channel-page blockquote.

---

## `docs/pages/channel/+Page.mdx`

**R‑CH‑1** (note) — "Building on these, `Room` adds multi-party rooms with presence, participant metadata, private messages, and admin controls."
`C 9 · A 9 · N 9 · Overall 9` — "Building on these" cleanly refers to the primitives listed directly above it. Good placement (a reader on the channel page learns rooms exist right where they'd wonder "what's the higher-level thing?"). Natural.

---

## `docs/pages/redis/+Page.mdx`

**R‑RD‑1** — "Rooms work across instances too: room state lives in Redis keys, room events travel over Redis Pub/Sub."
`C 9 · A 8 · N 9 · Overall 9` — Appended cleanly to the existing broadcast sentence; "too" ties it to the cross-instance claim just made. On the Redis page the reader already has Redis context, so "Redis Pub/Sub" is fully accessible here. Good.

---

## `docs/headings.ts` (not prose)

Nav config only — a `level: 2` entry titled `` `Room` `` at `/room` with `sectionTitles: ['Rooms', 'Participants', 'Messaging', 'Recipes']`. These are navigation labels, not sentences, so they aren't rated. They correctly mirror the page's `##` headings, and placing the entry right after `` `Channel` `` matches the primitives → rooms progression. No issues.

---

## Aggregate

| Band | Count (approx.) | Notes |
|---|---|---|
| 9–10 | ~28 | Intro, the model glosses, quick start, most of Rooms/Messaging headers, room-authored section |
| 8 | ~24 | Solid reference prose with mild jargon |
| 7 | ~12 | Understandable but inherit the "observed"/"grant"/"wire" concepts |
| 6 | 8 | The listed action items — dense infra nouns or abstract "granted through" phrasing |
| ≤5 | 0 | Nothing is outright confusing or ungrammatical |

**Overall page grade: strong (≈7.9 average).** The writing is natural and mostly crystal-clear; the one systemic weakness is criterion 2 — a cluster of sentences that assume the channel/stream pages or a dense earlier concept. Tightening the four flagged concepts ("observed", ".client variant", "granted through the room", and the stacked infra nouns) would lift most of the 6–7 sentences into the 8–9 band without touching the page's structure.

---

## Round 2 — edits & re-ratings

Every sentence rated **Overall ≤ 7** was edited in the source docs, and each edit was
re-rated with the same C / A / N / Overall rubric. Where a first edit still landed low
(≤ 7), extra wordings were drafted, rated, and the best kept; those alternatives are
listed under the sentence. All edits verified against the implementation
(`packages/telefunc/wire-protocol/room/`) so the rewrites stay technically exact — the
"observed" triggers (`server.ts` `_syncSubs`), the presence reaper (`seenAt` refresh /
`ROOM_MEMBER_TTL_MS`), and isolated-mode per-member keys all match the reworded prose.

Two guiding decisions shaped the whole pass:

- **Define "hold" once, then reuse it.** `hold`/`holder` is load-bearing in four places.
  Rather than purge it, R‑MODEL‑2 now defines it ("anyone holding the room object — the
  server, or a client it was handed to"); every later `holder` then references a defined term.
- **Define "observed" plainly and early.** R‑INST‑2 now leads with "a room is **observed**
  when something is watching it". Its plain gloss ("watching") is reused downstream
  (R‑PM‑4), so the concept compounds instead of being re-explained.

### Result summary

| ID | Before → After (Overall) | New C · A · N | File / section |
|---|---|---|---|
| R‑MODEL‑2 | 7 → **8** | 8 · 8 · 8 | room · The model |
| R‑MODEL‑5 | 6 → **9** | 9 · 9 · 9 | room · The model |
| R‑INST‑2 | 6 → **8** | 9 · 8 · 9 | room · `Room` instance |
| R‑INST‑3 | 7 → **8** (inherited, unchanged) | 8 · 8 · 8 | room · `Room` instance |
| R‑LP‑2 | 7 → **8** | 9 · 8 · 8 | room · `LocalParticipant` |
| R‑LP‑6 | 7 → **8** | 8 · 8 · 8 | room · `LocalParticipant` |
| R‑MSG‑4 | 7 → **8** | 8 · 8 · 8 | room · Room-wide |
| R‑MSG‑5 | 6 → **8** | 9 · 8 · 8 | room · Room-wide |
| R‑PM‑4 | 7 → **8** | 8 · 8 · 8 | room · Private messages |
| R‑PM‑6 | 7 → **8** | 8 · 8 · 8 | room · Private messages |
| R‑VID‑1 | 6 → **9** | 9 · 9 · 9 | room · Video chat |
| R‑PGP‑2 | 7 → **8** | 8 · 8 · 8 | room · Policy-gated |
| R‑PGP‑3 | 6 → **8** | 9 · 8 · 9 | room · Policy-gated |
| R‑PGP‑4 | 6 → **8** | 9 · 8 · 8 | room · Policy-gated |
| R‑SCALE‑1 | 6 → **8** | 8 · 8 · 8 | room · Scaling |
| R‑SIZE‑0 | 7 → **9** | 9 · 9 · 9 | room · Sizing rooms |
| R‑SIZE‑Text | 7 → **8** | 8 · 8 · 8 | room · Sizing rooms |
| R‑SIZE‑Binary | 7 → **8** | 8 · 8 · 8 | room · Sizing rooms |
| R‑SIZE‑Presence | 7 → **8** | 8 · 8 · 8 | room · Sizing rooms |
| R‑SIZE‑Keys | 6 → **8** | 8 · 8 · 8 | room · Sizing rooms |
| R‑HIW‑Lazy | 7 → **8** (inherited, unchanged) | 8 · 8 · 8 | room · How it works |
| R‑STREAM‑2 | 7 → **8** | 8 · 9 · 8 | stream · Primitive: `Room` |

Every low sentence reached the 8–9 band; none needed the "best-of-10 still low → list all"
fallback. The two `(inherited, unchanged)` rows are sentences whose only weakness was a
concept defined elsewhere; fixing that concept lifts them without touching their wording
(details at the end).

---

### room · The model

**R‑MODEL‑2** (6 → **8**; C 8 · A 8 · N 8)
- Before: "…Created and managed on the server (`Room.*` statics); whoever holds it (server or client) can observe live membership, events, and message streams."
- After: "…Created and managed on the server (the `Room.*` statics), but anyone holding the room object — the server, or a client it was handed to — can observe its live membership, events, and message streams."
- Why: introduces "hold" *with* its meaning ("holding the room object") and says who can hold one, so the four later uses of `holder`/`holding` now land on a defined term. The `Room.*` statics forward-reference (minor, linked) is left as-is.

**R‑MODEL‑5** (6 → **9**; C 9 · A 9 · N 9)
- Before: "The three objects are one type each, same on server and client — a `Room` or `LocalParticipant` can be returned from a telefunction as-is, no `.client` variant."
- After: "Each object is a single type, identical on server and client — a `Room` or `LocalParticipant` can be returned from a telefunction and used on the client as-is."
- Why: fixes the awkward "one type each" and **drops** the `.client` reference (the review's own "or drop the reference here"). A room-first reader no longer meets a channel-page trick they can't decode; the positive fact stands on its own.
- Alternatives weighed: (a) keep + self-gloss "no `.client` variant to convert to" → A 8; (b) parenthetical contrast "channels, by contrast, need a `.client` variant to reverse their direction" → A 7, still imports a channel concept; (c) drop entirely → A 9. Picked (c) — accessibility is the page's flagged weakness and the `.client` contrast carries no weight for a reader who never learned it.

### room · `Room` instance

**R‑INST‑2** (6 → **8**; C 9 · A 8 · N 9) — the load-bearing "observed" definition
- Before: "Getters reflect the live local view: while the room is **observed** — a listener is attached, a participant joined through it, or it was serialized to a client — they're kept fresh by the room's event stream; otherwise they're a snapshot from the last sync."
- After: "A room is **observed** when something is watching it: a listener is attached, a member joined through it, or it was sent to a client. While observed, the getters stay live — kept fresh by the room's event stream; otherwise they show a snapshot from the last sync."
- Why: split into two sentences and led with the plain idea ("something is watching it") before the mechanism. "serialized" → "sent"; the three triggers verified against `server.ts` `_syncSubs` (stubs / local participants / listener counts). This is the concept the rest of the page leans on, so it now reads as the clearest note, not the densest.
- Residual A 8: "a member joined through it" still uses the join-through idiom (core room vocab, kept for precision).

### room · `LocalParticipant`

**R‑LP‑2** (7 → **8**; C 9 · A 8 · N 8)
- Before: "…Room-wide messages are received on the room and on `RemoteParticipant`; only private messages addressed to you arrive here (`listen()`)."
- After: "…Room-wide messages arrive on the room and on each `RemoteParticipant` — not on your `LocalParticipant`, which receives only the private messages addressed to you (`listen()`)."
- Why: resolves the "here" deixis puzzle by naming the surface ("your `LocalParticipant`") and states the three receiving surfaces as a clean contrast.

**R‑LP‑6** (7 → **8**; C 8 · A 8 · N 8)
- Before: "A participant's membership follows its holder: when the client holding a `LocalParticipant` (or the room it joined through) disconnects, the participant leaves the room."
- After: "A participant stays in the room only as long as its holder stays connected: when the client holding the `LocalParticipant` (or the room it joined through) disconnects, the participant leaves the room."
- Why: replaces the abstract headline "membership follows its holder" with the concrete rule; "holder" is now defined (R‑MODEL‑2).

### room · Messaging — Room-wide

**R‑MSG‑4** (7 → **8**; C 8 · A 8 · N 8)
- Before: "Per-member binary subscriptions also control **wire delivery**: a client receives a member's binary stream only while it subscribes to that member (or to the whole room)."
- After: "Per-member binary subscriptions also control **wire delivery** — what actually travels over the network: a client receives a member's binary stream only while it's subscribed to that member (or to the whole room)."
- Why: glosses "wire delivery" the first time it appears, so "on the wire" is anchored for R‑MSG‑5.

**R‑MSG‑5** (6 → **8**; C 9 · A 8 · N 8) — the 45-word / 3-parenthetical sentence
- Before: "Text is never filtered on the wire: the text stream also carries the control events (join/leave/metadata) that keep every holder's live view current (`count`, `getParticipants()`, event callbacks) — and text payloads are small compared to binary, so filtering them would save little bandwidth."
- After: "Text, by contrast, is never filtered on the wire. The text stream also carries control events — joins, leaves, and metadata changes — that keep every holder's view of the room current, so they can't be dropped for anyone. And text is tiny next to binary, so filtering it would save almost no bandwidth."
- Why: three short sentences instead of one; "by contrast" ties it to the binary rule above; "control events" glossed inline; the `count/getParticipants` parenthetical dropped (recoverable). The reasoning the review liked is kept intact.

### room · Messaging — Private messages

**R‑PM‑4** (7 → **8**; C 8 · A 8 · N 8)
- Before: "`from` is the **verified** sender: the live `RemoteParticipant` when your side observes the room, or an `{ id, meta }` snapshot otherwise."
- After: "`from` is the **verified** sender — the live `RemoteParticipant` if your side is watching the room, or an `{ id, meta }` snapshot if not."
- Why: "watching the room" reuses R‑INST‑2's plain gloss for *observed*, and is self-explanatory even for a reader who skipped that note.

**R‑PM‑6** (7 → **8**; C 8 · A 8 · N 8) — first appearance of "grant"
- Before: "To authorize whispers, attach a guard when granting the room: `Room.get(id, { onSend })` — see the recipe."
- After: "To authorize whispers, add an `onSend` guard to the `Room.get()` that hands the room to a client: `Room.get(id, { onSend })` — see the recipe."
- Why: "granting the room" (undefined here) → "the `Room.get()` that hands the room to a client"; keeps the inline example and the recipe link.

### room · Recipes — Video chat

**R‑VID‑1** (6 → **9**; C 9 · A 9 · N 9) — three infra nouns in a recipe
- Before: "**Isolated mode** (`isolated: true`) gives each participant their own upstream pub/sub key, removing publish contention between members — useful on platforms that map each key to a separate coordinator (e.g. Cloudflare Durable Objects)."
- After: "**Isolated mode** (`isolated: true`) gives each participant their own message stream — a better fit for high-throughput media like video."
- Why: a recipe reader wants to build video chat, not read the performance model. The mechanism (contention, per-member keys) already lives in *Sizing rooms* → **Keys** and *How it works* → **One pub/sub key per room**; the Cloudflare backend is still documented in *Scaling* → R‑SCALE‑4.
- Alternatives weighed: (a) keep a plain-glossed platform nod "…and for serverless platforms that put each stream on its own instance (e.g. Cloudflare Durable Objects)" → A 8, but "instance/coordinator/worker" for a Durable Object is imprecise; (b) "each participant gets their own stream — better for high-throughput media" → A 9. Picked (b), matching the review's suggested altitude.

### room · Recipes — Policy-gated private messages

**R‑PGP‑2** (7 → **8**; C 8 · A 8 · N 8)
- Before: "It's declared in the telefunction, so it can close over the request context — e.g. the authenticated user from `getContext()`."
- After: "It's declared inside the telefunction, so it has access to the request context — e.g. the authenticated user from `getContext()`."
- Why: "close over" (closures jargon) → "has access to".

**R‑PGP‑3** (6 → **8**; C 9 · A 8 · N 9) — the most abstract phrasing on the page
- Before: "It runs before every private-message delivery from any membership granted through the returned room; throwing rejects the sender's `send()` with the error:"
- After: "It runs before Telefunc delivers any private message sent by someone who joined through this room object. If the guard throws, that `send()` rejects with the error:"
- Why: unpacks the "membership granted through the returned room" noun-stack into a plain relative clause ("sent by someone who joined through this room object") and splits off the throwing behavior. "rejects with the error" matches the code comment's wording ("rejects with 'not friends'").

**R‑PGP‑4** (6 → **8**; C 9 · A 8 · N 8) — third use of "membership granted through it"
- Before: "…`Room.get(id, { onSend })` covers every membership granted through it — server-side `join()`s and client-side `join()`s on it alike."
- After: "…`Room.get(id, { onSend })` covers every join made through it — whether that join happens on the server or on a client."
- Why: "every membership granted through it" → "every join made through it"; kills the awkward pluralized "`join()`s".

### room · Scaling & multiple servers

**R‑SCALE‑1** (6 → **8**; C 8 · A 8 · N 8) — the section's topic sentence, three unglossed infra nouns
- Before: "Room state lives in the broadcast adapter's KV store, and room events travel over its pub/sub — so rooms scale exactly like `Broadcast` does:"
- After: "A room keeps its state in a key-value store and sends its events over pub/sub, just like `Broadcast` — so rooms scale the same way, from one server to many:"
- Why: states the mechanism in plain terms *first* (so "how does Broadcast scale?" no longer needs prior reading); "broadcast adapter's KV store" → "a key-value store", drops "adapter" from the topic sentence; "from one server to many" previews the Single/Multiple/Custom bullets.
- Alternatives weighed: (a) lead with the payoff "Rooms scale exactly like `Broadcast`, because they run on the same infrastructure: …" → A 7 (still requires the Broadcast model to feel the payoff); (b) "…the same store and pub/sub bus your broadcasts already use" (the review's gloss) → A 7, leans on the reader having broadcasts; (c) mechanism-first "A room keeps its state in a key-value store and sends its events over pub/sub, just like `Broadcast`…" → A 8. Picked (c) — the reader learns the mechanism directly and "just like Broadcast" becomes a bonus, not a dependency.

### room · Sizing rooms

**R‑SIZE‑0** (7 → **9**; C 9 · A 9 · N 9)
- Before: "What each mechanism costs, so you can size rooms deliberately:"
- After: "Here's what each mechanism costs, so you can size rooms deliberately:"
- Why: adds the verb; the section lead-in is no longer a hanging fragment.

**R‑SIZE‑Text** (7 → **8**; C 8 · A 8 · N 8)
- Before: "…every connection holding the room receives its control events and text data. …"
- After: "…every connection holding the room receives its text data and control events (joins, leaves, metadata). …"
- Why: a 3-word inline gloss makes the bullet self-contained; "hold" is defined (R‑MODEL‑2). The worked number (~6 KB/s) and the FIFO plain-restatement — the parts the review praised — are untouched.

**R‑SIZE‑Binary** (7 → **8**; C 8 · A 8 · N 8)
- Before: "**Binary delivery** — member-selective on the wire and, in isolated mode, upstream too: cost scales with what each client actually watches, not with room size."
- After: "**Binary delivery** — cost scales with what each client actually watches, not with room size. Binary is delivered to clients per member, and in isolated mode a server also pulls only the members its own clients watch."
- Why: leads with the crisp payoff, then states the mechanism in plain words ("delivered per member", "a server pulls only the members its clients watch") instead of "member-selective on the wire … upstream too".

**R‑SIZE‑Presence** (7 → **8**; C 8 · A 8 · N 8)
- Before: "…scans the room's member records to reap dead members: `O(members)` KV reads per server every 30s. …"
- After: "…scans the room's member records to drop members whose owner has gone silent: `O(members)` KV reads per server every 30s. …"
- Why: "reap dead members" → "drop members whose owner has gone silent" — plainer *and* more informative (it says what "dead" means), consistent with the reaper in `server.ts` (a member is dropped when its owning node stops refreshing `seenAt`). The `O(members)` cost model stays, since this is the deep-dive sizing section.

**R‑SIZE‑Keys** (6 → **8**; C 8 · A 8 · N 8) — five infra terms in one sentence
- Before: "**Keys** — the default shared mode uses one pub/sub key per room (one adapter subscription per observing node); `isolated: true` moves data to per-member keys, removing publish contention and letting each node subscribe upstream only to the members its clients watch."
- After: "**Keys** — by default a room uses a single pub/sub key, so each server observing the room opens just one subscription. With `isolated: true`, every member gets their own key instead: members no longer share a key (no publish contention), and each server subscribes only to the members its own clients are watching."
- Why: split into two sentences; "observing node" → "each server observing the room"; "publish contention" glossed inline as "members no longer share a key"; "subscribe upstream" → "subscribes". Every claim preserved, one term per clause.
- Alternatives weighed: (a) lead with the cost "…but the room now spans many keys rather than one" appended → more complete but longer, A 7; (b) the two-sentence split above → A 8. Picked (b) — the sizing reader is opted into some jargon; the win is one-term-per-clause, not term removal.

### stream · Primitive: `Room`

**R‑STREAM‑2** (7 → **8**; C 8 · A 9 · N 8) — awkward comma splice
- Before: "…chat, game lobbies, video calls, collaboration — you can use a room, see:"
- After: "…chat, game lobbies, video calls, collaboration — you can use a room:"
- Why: drops the ", see" splice and lets the colon lead straight into the `/room` link (the review's own "…you can use a room:"). "you can use a room" is kept deliberately — it mirrors the pre-existing sibling transition on the same page ("…you can use a channel, see:"), so the two primitive hand-offs stay parallel.

---

### Deliberately left unchanged (lifted by a dependency fix)

Two low sentences had **no wording problem of their own** — each was low only because it
leaned on a concept that was itself unclear. Fixing the concept lifts them, so editing
their wording would be churn.

**R‑INST‑3** (7 → **8**) — "On the server, `getParticipants()` always resyncs an unobserved room first, so the list is current."
- Its A 6 was "entirely inherited from the dense definition above it" (the review's own words). With R‑INST‑2 now defining *observed*/*unobserved* plainly, "unobserved" lands and the sentence reads at 8 unchanged.

**R‑HIW‑Lazy** (7 → **8**) — "A room subscribes to its pub/sub key only while observed. …"
- Sits in *How it works*, the explicit mechanism deep-dive where "scores weight clarity/naturalness over accessibility". Its one snag, "observed", is now defined; the remaining "subscribe upstream" is appropriate to the section. C 8 · N 8 were never the problem, so it's left intact.

### Revised aggregate

| Band | Before | After |
|---|---|---|
| 9–10 | ~28 | ~31 |
| 8 | ~24 | ~41 |
| 7 | ~12 | 0 |
| 6 | 8 | 0 |
| ≤5 | 0 | 0 |

**New page grade: ≈8.3 average.** No sentence remains in the 6–7 band. The systemic
criterion-2 weakness is resolved at its roots — "observed", "hold", ".client variant",
"granted through the room", and the stacked infra nouns are each defined in plain words
where first needed, so the sentences that used to lean on them now stand on their own.
