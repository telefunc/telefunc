# Docs review — `Room` and related pages

Sentence-by-sentence review of the documentation introduced by this PR.

**Files reviewed**

- `docs/pages/room/+Page.mdx` — the new `Room` page (the bulk of the review)
- `docs/pages/channel-config/+Page.mdx` — new `messageLimit` prose
- `docs/pages/channel/+Page.mdx` — new `Room` teaser + `receivers` prose
- `docs/pages/redis/+Page.mdx` — one new clause about rooms across instances
- `docs/pages/stream/+Page.mdx` — new `Room` primitive section
- `docs/headings.ts` — navigation only, no prose to rate

## How each sentence is rated

One rating from **1 (bad) to 10 (excellent)** per sentence, judged against the three requested criteria. When a rating is pulled down, the reason names the criterion:

1. **Clarity** — crystal clear, zero ambiguity, no fuzzy words. The reader should never have to second-guess the meaning.
2. **Prior-reading** — understandable with only a *vague, high-level* idea of what the page is about. Correct-but-only-for-an-expert counts against the sentence.
3. **Naturalness** — reads naturally; no weird or unfamiliar phrasing.

**Scope note.** "Sentence" means prose: body sentences, bold-led sentences, blockquotes, `<Warning>` text, and full-sentence captions. Code inside fenced blocks, table cells that are bare noun-phrases, and short in-code comments are *not* rated individually — where a table or code comment carries real doc meaning, it is called out in a per-section note instead. This keeps the review to actual sentences a reader parses.

---

## Executive summary

**Rating distribution (prose sentences, all files):** the page is mostly strong — the majority land at **7–9**. Clarity and naturalness are generally very good; the weak spots are almost all **criterion 2 (prior-reading)**: sentences that are precise for an expert but opaque to someone with only a vague idea of what a room is.

**Lowest-rated sentences (fix these first):**

| Rating | Location | Sentence (abridged) | Core problem |
|---|---|---|---|
| **4** | room L304 | "…so moving **channels** is a `leave()` + `join()`" | "channels" collides with Telefunc's own `Channel` primitive — means "rooms" here |
| **4** | room L418 | "`isolated: true` moves text to per-member keys — the Cloudflare Durable-Object publish-contention knob" | deeply internal jargon ("text", "per-member keys", "publish-contention knob") |
| **5** | room L239 | "Mic, camera, and screen share are *named tracks* multiplexed over one member's stream: publish with `{ track }`…resync:" | one sentence carries 5 ideas + A/V jargon |
| **5** | room L416 | "…owners heartbeat every 30s and stale members are reaped, with a native store-expiry backstop…" | undefined "owners"; systems jargon ("reaped", "store-expiry backstop") |
| **6** | room L75, L97, L175, L180, L198, L259, L361, L408, L417 | (see details) | expert-only concepts stated without a bridge |
| **6** | channel-config | "…a client declaring a gigabyte message is **rejected from the declared length**…" | "rejected from the declared length" is awkward phrasing |

**Recurring themes**

1. **Prior-reading is the main weakness.** The page reads beautifully if you already know distributed-systems and A/V vocabulary — *at-most-once*, *multiplexed*, *congestion control*, *jitter buffering*, *deterministic convergence*, *reaped*, *store-expiry backstop*, *Durable-Object publish-contention*. A reader with a vague understanding stalls on these. Most are in **Binary streams** and **Production**, which is the right place for depth, but several could carry a 3-word gloss.
2. **A few sentences do too much.** L239 and L259 pack multiple distinct facts into one comma-chained sentence. Splitting them would lift both clarity and prior-reading scores.
3. **Terse-to-elliptical fragments.** "Room-level, for messages you process the same way:" (L113) and "A rejected join writes nothing." (L198) are so compressed they only resolve on a second pass or from the code below.
4. **One genuine terminology clash:** "moving channels" for "switching rooms" (L304), on a page whose entire job is to distinguish `Room` from `Channel`.
5. **Naturalness is strong overall.** Only a handful of phrasings read oddly: "travels spoof-proof" (adverbial, L143), "rejected from the declared length" (channel-config), "0 truthfully means" (channel).

The detailed ratings follow.

---

## `docs/pages/room/+Page.mdx`

### Intro (lines 4–21)

**L4 — "*Environment*: server & client."**
**8/10** — Standard site-wide label; clear that the API exists on both sides. A first-time reader may not know what "Environment" formally means, but it's a page-wide convention, so it's fair. Natural.

**L8 — "`Room` adds multi-party *rooms* on top of channels: presence (who's in the room), participant metadata, per-member streams, private messages, and admin controls."**
**7/10** — Good overview and the parenthetical gloss on "presence" is exactly right. Two small drags: it's mildly circular ("`Room` adds…rooms"), and it leans on the reader knowing roughly what "channels" are (criterion 2) — the link softens that. "per-member streams" is an unexplained teaser but acceptable here.

**L10 — "Use rooms for chat, video calls, game lobbies, collaborative editing — anything where *who is connected* matters."**
**9/10** — Concrete, memorable, defines the use case crisply with near-zero prior knowledge needed. Model opening line.

**L10 — "When you only need messaging or pub/sub, use channels instead."**
**8/10** — Clear routing to the alternative. "pub/sub" is mild jargon but standard for this audience. Natural.

**Comparison table (L12–21)** — *note, not rated per cell.*
**8/10 as a unit** — Effective at-a-glance contrast of `Channel` / `BroadcastChannel` / `Room`. "Server-client pipe" and "Topic-based pub/sub" assume some familiarity, but the parenthetical glosses ("who's connected", "kick, close") carry a vague reader through.

### The model (L24–43)

**L26 — "Three objects:"** — list lead-in, clear.

**L28 — "`Room` — the shared space."**
**9/10** — Perfect one-line mental model.

**L28 — "Created and managed on the server (the `Room.*` statics), but anyone holding the room object can observe its live membership, events, and message streams."**
**7/10** — Clear on the whole. "the `Room.*` statics" is a forward-reference a vague reader won't fully grasp yet (criterion 2), though it's parenthetical and linked. "anyone holding the room object" is a nice, concrete phrase.

**L29 — "`LocalParticipant` — the current user, returned by `join()`."**
**9/10** — Plain-language gloss ("the current user") lands immediately.

**L29 — "You publish, send private messages, and update your metadata through it."**
**9/10** — Active, concrete, natural.

**L30 — "`RemoteParticipant` — everyone else, as seen through a room: subscribe to one member's messages, watch their metadata, notice when they leave."**
**8/10** — Parallel and clear; the concrete verb list resolves the slightly abstract "as seen through a room."

**L32 — "And three message lanes:"** — introduces the coined term "lane"; intuitive.

**Message-lanes table (L34–38)** — *note.*
**6/10 as a unit** — Useful, but dense for its position: the cells use `me.publish(data)`, `room.subscribe(cb)`, `member.subscribe(cb)` before `me`/`room`/`member` are bound in any example (criterion 2). "Room-authored" is a coined term explained only later. Reads better as a reference *after* the guides than as an up-front table.

**L40 — "Two things hold everywhere:"**
**7/10** — Clear, but "hold" in the mathematical sense ("things that hold") is slightly formal/unusual (criterion 3).

**L42 — "*One type, both sides.* A `Room`, `LocalParticipant`, or `RemoteParticipant` can be returned from a telefunction and used on the client exactly as on the server."**
**8/10** — Clear and important. Assumes the reader knows what a "telefunction" is — fair on the Telefunc site. Natural.

**L43 — "*The sender travels with the message.* Every received message carries its verified sender (`from.id`, `from.meta`, `from.identity`), stamped by the server."**
**8/10** — Clear; "stamped by the server" is a good, vivid choice. The `from.*` fields appear before being formally defined but are self-evident.

**L43 — "`from === null` means the room itself spoke."**
**7/10** — The "the room itself spoke" metaphor is charming but slightly cryptic on first read and forward-references room-authored messages (criterion 2). Mostly lands.

### Quick start (L46–77)

**L48 — "A telefunction creates the room and hands it out; the client joins and chats:"**
**8/10** — Clear setup for the example. "hands it out" is casual but unambiguous.

**Code comment (L58) — "// created elsewhere with `Room.create('lobby')`"** — helpful inline gloss; clear.

**L75 — "Like the built-in `Date`, `Room` is both a value and a type: the statics (`Room.get()`) and the instance type (`const lobby: Room`)."**
**6/10** — Accurate and the `Date` analogy is a good instinct, but "both a value and a type" is a TypeScript-literate idea (criterion 2). A reader with only a vague understanding won't follow the value/type duality, and it lands mid-Quick-start where momentum matters. Consider moving to the Reference.

**L77 — "The guides below cover the common tasks one at a time; the reference and production notes come after."**
**9/10** — Clear signposting of the page's structure; natural.

### Show who's here (L82–108)

**L84 — "`getParticipants()` gives you the member list; `onJoin`/`onLeave` keep it fresh; each member's `onUpdate` tracks their metadata:"**
**8/10** — Clear tricolon mapping method → purpose. "keep it fresh" is casual but understandable.

**L97 — "For UI frameworks, skip the listener bookkeeping: `snapshot()` returns an immutable whole-room view whose reference only changes when the room does, and `onChange()` fires on any change:"**
**6/10** — Precise for React/Vue users but "an immutable whole-room view whose reference only changes when the room does" encodes reference-identity/memoization semantics a vague reader won't decode (criterion 2). "the listener bookkeeping" also assumes they read the previous snippet. The payoff is real; the sentence just asks a lot.

**Code comment (L100) — "// React — this is the entire adapter"**
**8/10** — Clear and effective as a caption; "the entire adapter" is a small, forgivable flourish.

**L106 — "*Your own metadata.* `me.setMeta({ name: 'Alice', score: 42 })` replaces it wholesale; `me.setAttributes({ score: 42 })` merges by key (other keys keep their value; a key set to `undefined` is removed)."**
**8/10** — Clear replace-vs-merge contrast with a precise parenthetical. Slightly long but well-structured; "wholesale" is fine.

**L106 — "Both propagate to every observer."**
**8/10** — Clear. "observer" is consistent with the earlier "observe" vocabulary.

**L108 — "Metadata is for durable identity state (name, avatar, score)."**
**8/10** — "durable identity state" is slightly abstract but the examples ground it immediately.

**L108 — "High-frequency ephemeral state — cursors, typing indicators — belongs on `publish()` (see Chat), which touches no storage."**
**7/10** — Clear with examples; "High-frequency ephemeral state" is a touch technical but the cursor/typing examples rescue it. "touches no storage" is a crisp, useful contrast with "durable".

### Chat (L111–138)

**L113 — "Room-level, for messages you process the same way:"**
**6/10** — Too compressed (criterion 1). "for messages you process the same way" — the same way as *what*? — only resolves once you see the per-member contrast below. A few more words ("…handled uniformly, not per sender") would fix it.

**L120 — "Per-member, when each member's stream needs its own handling:"**
**7/10** — Clearer than its sibling because the room-level/per-member contrast is now visible; still elliptical ("Per-member,").

**Code comment (L127) — "// a member's listeners die with it"** — vivid, clear; the "die with it" metaphor fits.

**L130 — "A member's `publish()` reaches both room-level subscribers (with `from`) and that member's subscribers."**
**8/10** — Clear statement of the dual delivery path; "(with `from`)" is a helpful reminder.

**L132 — "By default your own publishes come back to your own side."**
**7/10** — Understandable, but "your own publishes come back to your own side" is slightly clunky — "publishes" as a noun plus the "your own…your own" repetition (criterion 3).

**L132 — "Turn that off per participant when you don't want the echo: `join(meta, { selfDelivery: false })`."**
**8/10** — Clear, and "the echo" is a good, natural name for the concept.

**L134 — "*High-frequency updates* (cursors, live reactions) can flood a slow connection."**
**9/10** — Clear, concrete, natural.

**L134 — "Pass `{ coalesce: key }` and, while a publish with that key is still in flight, newer ones collapse into a single pending send — only the latest goes out:"**
**7/10** — Accurate and mostly clear, but dense: "in flight" and "collapse into a single pending send" are mild jargon, and the comma-embedded clause makes it a heavy read. "only the latest goes out" is the saving clarification.

### Identify your users (L141–160)

**L143 — "Participant IDs are per-membership: the same person is a different participant in every room, tab, and reconnect."**
**9/10** — Excellent: an abstract term ("per-membership") immediately grounded by a concrete, surprising gloss. Zero ambiguity.

**L143 — "To correlate memberships, stamp an `identity` at join."**
**8/10** — Clear; "stamp" is consistent with the "stamped by the server" metaphor used elsewhere.

**L143 — "It's server-assigned in the telefunction (a client `join()` can't claim one), and travels spoof-proof with the member:"**
**7/10** — Content is clear and the parenthetical is helpful, but "travels spoof-proof with the member" uses "spoof-proof" adverbially, which reads awkwardly (criterion 3). "travels with the member, and can't be spoofed" would be smoother.

### Private messages (L163–175)

**L165 — "`send()` delivers to exactly one participant — no one else receives it, and it never appears on `subscribe()` streams:"**
**9/10** — Crisp and unambiguous; the em-dash elaboration reinforces the guarantee well.

**Code comment (L172) — "// a RemoteParticipant works too"** — clear.

**L175 — "Sending to an unknown or departed participant rejects."**
**7/10** — Concise, but "rejects" (promise-rejection semantics) is jargon that could momentarily read as "the participant rejects [it]" (criterion 1). "unknown or departed participant" is clear.

**L175 — "Private messages are at-most-once signaling (invites, whispers, game moves)."**
**6/10** — "at-most-once signaling" is precise distributed-systems vocabulary that a reader without that background won't decode (criterion 2). The examples hint at "fire-and-forget", but the term itself is a wall.

**L175 — "Durable messaging — offline recipients, history — belongs in your database, delivered to live participants with `Room.send()`."**
**7/10** — Mostly clear guidance; the tail "delivered to live participants with `Room.send()`" is compressed and name-drops `Room.send()` before it's defined (criterion 2).

### Moderate (L178–215)

**L180 — "`Room.guard()` attaches server-side policy to a room."**
**8/10** — Clear and compact.

**L180 — "It covers every membership granted through that room object — server-side `join()`s and the client `join()`s on the copy you hand out."**
**6/10** — Precise but demands that the reader hold the earlier "one type, both sides" model in mind (criterion 2). "the copy you hand out" (returning the room from a telefunction) is subtle and easy to miss on a vague read.

**L180 — "Declare it in the telefunction, where you have `getContext()`:"**
**8/10** — Clear; assumes familiarity with `getContext()`, which is fair on the Telefunc site.

**L198 — "Each guard's first argument is the acting member."**
**8/10** — Clear; "the acting member" is a good term.

**L198 — "Throwing rejects the caller's `join()`/`publish()`/`send()` with that error, on the client too."**
**8/10** — Clear, and the "on the client too" detail (error propagation) is valuable.

**L198 — "A rejected join writes nothing."**
**6/10** — Too terse (criterion 1): "writes nothing" assumes the reader supplies "writes to storage/state". "adds no member and leaves no trace" would remove the guesswork.

**L198 — "Guards run on the server."**
**9/10** — Short, clear, reassuring. Good.

**L200 — "*Kicks carry their reason.* Every departure says why, and a kick's reason arrives with the removal:"**
**7/10** — Mostly clear; "Every departure says why" is punchy but slightly anthropomorphic/vague, resolved by the code below.

**L213 — "`cause.type` is one of `'left'`, `'removed'`, `'closed'`, `'disconnected'`."**
**9/10** — Clear, exhaustive enumeration.

**L215 — "The other moderation tools: observe without joining (`room.subscribe()` on the server), kick (`Room.removeParticipant()`), close (`Room.close()`)."**
**8/10** — Clear labelled list; "observe without joining" is a nice capsule description.

**L215 — "Wire them to admin users through your permissions."**
**8/10** — Clear; "Wire them to" is casual but unambiguous.

### System notices (L218–234)

**L220 — "The room itself can speak — receivers can't confuse it with a member message:"**
**8/10** — Clear, and the "the room itself can speak" metaphor is by now well established. The benefit ("can't confuse it") is concrete.

**Code comments (L229, L232–233)** — "// announcements — never on subscribe()", "// room-authored (`Room.send()`)", "// participant-authored (`send()`)" — all clear, useful disambiguators.

### Binary streams & tracks (L237–259)

**L239 — "`publishBinary()` / `subscribeBinary()` carry raw bytes — canvas deltas, audio snippets, file chunks, small-scale audio/video."**
**8/10** — Clear; concrete examples ground "raw bytes".

**L239 — "Mic, camera, and screen share are *named tracks* multiplexed over one member's stream: publish with `{ track }`, subscribe with `{ track }` to receive only that substream (`{ track: null }` is the default lane), and the `keyFrame` flag rides each frame (`info.keyFrame`) so decoders can resync:"**
**5/10** — Overloaded (criteria 1 & 2). One sentence introduces: named tracks, multiplexing, publish syntax, subscribe syntax, the default lane, and keyframes. "multiplexed", "substream", "rides each frame", and "resync" all assume A/V or networking familiarity. This should be 2–3 sentences.

**L247 (Warning) — "Rooms move binary over the same reliable, ordered transport as everything else (WebSocket / SSE)."**
**7/10** — Accurate; "reliable, ordered transport" is networking jargon but the parenthetical (WebSocket/SSE) grounds it.

**L247 — "That's ideal for binary *data* and small or controlled-network A/V — but it has no congestion control, simulcast, or jitter buffering."**
**6/10** — The shape of the trade-off is clear, but "congestion control, simulcast, jitter buffering" are three specialist terms left unexplained (criterion 2). Appropriate for the target reader (someone weighing production video), less so for a general one.

**L247 — "For production multi-party video, pair rooms (presence, chat, signaling) with a WebRTC SFU such as [LiveKit](https://livekit.io) for the media."**
**7/10** — Clear, actionable recommendation; "WebRTC SFU" is jargon but the linked example (LiveKit) anchors it. Natural.

**L247 — "An unreliable WebTransport datagram lane that would lift this ceiling is being explored in [telefunc#449]."**
**6/10** — Dense jargon stack ("unreliable WebTransport datagram lane"); "lift this ceiling" is a nice metaphor. Fine as a forward-looking aside, not broadly legible (criterion 2).

**L249 — "*Render only 9 of 500 cameras?* Subscribe to those 9 members — the other 491 streams never reach you, and your server never fetches them at all."**
**9/10** — Vivid, concrete, and completely clear — the best illustration of selective subscription on the page.

**L249 — "The same holds per track: stop watching a screen share, and its bytes stop flowing to you."**
**8/10** — Clear parallel; natural.

**L251 — "*Pause the encoder when nobody's watching.* `onDemand` fires when the number of subscribers to one of your tracks changes — `0` means nobody is watching, a later non-zero means a viewer returned:"**
**8/10** — Clear cause/effect; the `0` / non-zero explanation is concrete.

**L259 — "Each `publishBinary()` receipt also reports `receivers`, the track's live subscriber count at that moment — use it while publishing, and `onDemand` to learn when to resume while paused."**
**6/10** — First half is clear. The tail "use it while publishing, and `onDemand` to learn when to resume while paused" is elliptical and stitches two different pieces of guidance together awkwardly (criteria 1 & 3). Splitting the two uses into two clauses would fix it.

### Load history, then go live (L262–299)

**L264 — "Rooms deliver *live* messages; history belongs in your database (retention, search, pagination — your rules)."**
**8/10** — Clear division of responsibility; "your rules" is a natural, friendly touch.

**L264 — "Store each message as it's published, then load history and go live in one call."**
**8/10** — Clear summary of the pattern the section will show.

**L266 — "*Store* in the `onPublish` guard — it runs once per message, on the server, with the verified sender:"**
**8/10** — Clear; the three qualifiers (once per message / on the server / with the verified sender) are each meaningful.

**L284 — "`{ tail: true }` starts buffering the room's live messages the moment the telefunction returns it, and the client holds them until your first `subscribe()`."**
**7/10** — Accurate and mostly clear, but a long single sentence describing a subtle timing guarantee; a vague reader may need a second pass (criterion 2).

**L284 — "So the history you read *after* that point can't miss a live message in between — the small overlap is deduped by message ID:"**
**7/10** — The reasoning is clear; "deduped" is mild jargon and "in between" is slightly loose, but the point lands.

**Code comments (L295–296) — "// the past", "// flushes the held live tail, then stays live"** — clear, well-placed.

**L299 — "Order history by your own stored timestamp; treat the live receipt's `info.timestamp` as display metadata."**
**7/10** — Clear to an attentive reader, but the *why* (server timestamps aren't safe for ordering across nodes) is left implicit, so "treat…as display metadata" reads as an unexplained rule (criterion 2).

### Switching rooms (L302–312)

**L304 — "Membership is per room, so moving channels is a `leave()` + `join()`:"**
**4/10** — Genuine terminology clash (criterion 1). On a page whose whole purpose is to distinguish `Room` from `Channel`, using "channels" colloquially to mean "rooms" (as in switching Discord/TV channels) is confusing. Say "switching rooms" or "moving between rooms".

**L312 — "A send on a room you've left rejects explicitly (`Participant has left the room`)."**
**7/10** — Clear, and quoting the literal error string is helpful; "rejects explicitly" again leans on promise semantics (criterion 1).

### Enforce capacity (L315–327)

**L317 — "`size` is a hint — Telefunc tracks it (`count`, `isFull`, `onFull()`) but never rejects a join."**
**8/10** — Clear; the parenthetical usefully lists the related fields, and "but never rejects a join" sets the expectation precisely.

**L317 — "You decide, in the telefunction:"**
**8/10** — Clear hand-off to the code.

**L327 — "`count` is exact at `Room.get()` and stays live while the room is observed."**
**7/10** — Precise but compressed: "exact at `Room.get()`" (accurate at call time) and "stays live while the room is observed" each need a beat of thought (criterion 2).

### Reference (L330–408)

**L334 — "The server-side entry point."**
**8/10** — Clear capsule description.

**L334 — "Clients get a room when a telefunction returns it."**
**9/10** — Clear, concise restatement of the core pattern.

**Room statics table (L336–348)** — *note.* Clear reference table; Returns/Throws columns are precise and scannable.

**RoomOptions code comments (L351–355)** — "capacity hint (default: Infinity) — not enforced" and "room metadata (default: {})" are clear; "per-member text keys (default: false) — fixed at creation" is cryptic in isolation ("text keys" undefined) but it's reference material clarified by L418.

**L358 — "`Room.get(id, { tail: true })` starts relaying live messages as soon as the room is returned to a client (see Load history)."**
**8/10** — Clear, with a back-link to the fuller explanation.

**L359 — "`Room.update()` replaces the fields you provide and keeps the rest (`isolated` is fixed at creation)."**
**8/10** — Clear replace-and-retain semantics.

**L360 — "`Room.removeParticipant(id, target, { reason? })` — `target` is a participant ID, or `{ identity }` to remove every membership of an identity at once (idempotent)."**
**7/10** — Clear for a reference; "(idempotent)" is jargon but acceptable in this context (criterion 2).

**L361 — "Typed metadata: `Room.get<ChatMeta, MemberMeta>(id)` types `room.meta`, `join`/`setMeta`, and `from.meta` everywhere."**
**7/10** — Reference-appropriate and clear to TS users; inherently TypeScript-specific (criterion 2).

**L361 — "The parameters are caller assertions (like `querySelector<T>`)."**
**6/10** — "caller assertions" is subtle TS terminology (unchecked generic assertions); the `querySelector<T>` analogy helps those who already know it but not a general reader (criterion 2).

**L381 — "Your own handle, from `join()`."**
**8/10** — Clear; "handle" is mild jargon but standard.

**L381 — "Room-wide messages arrive on the room and each `RemoteParticipant`; your `LocalParticipant` receives only the private messages addressed to you."**
**7/10** — Clear but dense — it distinguishes *where* room-wide vs private messages surface, which requires juggling three object types at once (criterion 2).

**L393 — "Read-only: `id`, `meta`, `identity`, `selfDelivery`."**
**8/10** — Clear property list.

**L393 — "A participant leaves automatically when its holder's connection is gone (`cause: 'disconnected'`)."**
**7/10** — Clear; "its holder's connection" (whoever holds the participant object) is slightly abstract but understandable.

**L397 — "Another member, seen through a room."**
**8/10** — Clear capsule description.

**L404 — "Read-only: `id`, `meta`, `identity`, `joinedAt`."**
**8/10** — Clear.

**L404 — "Cleanup in `onLeave` is enough — a member's listeners are released when it leaves."**
**8/10** — Clear reassurance about lifecycle/cleanup.

**L408 — "Every message callback receives its verified sender as `from` — `{ id, meta, identity }`, stamped by the server, impossible to spoof."**
**8/10** — Clear, good recap; the three qualifiers reinforce the guarantee.

**L408 — "It's the live `RemoteParticipant` when the room knows the sender, and a plain object with the same fields otherwise — either way you read `from.id` / `from.meta` and never branch."**
**6/10** — The reassurance (uniform shape, no special-casing) is valuable, but "when the room knows the sender" is vague about *when it wouldn't* (criterion 1), and "never branch" is casual programmer shorthand (criterion 3).

**L408 — "`from === null` only for room-authored messages."**
**8/10** — Clear given the established context; the fragment form ("only for…") is fine here.

### Production (L411–420)

**L413 — "Rooms work across multiple server nodes out of the box with `@telefunc/redis` and the Cloudflare integration."**
**8/10** — Clear headline claim for the section.

**L415 — "*Pay for what you consume.* Presence-only observers never receive the room's messages; binary flows only to members you subscribe to, and is fetched only while someone wants it."**
**7/10** — Clear resource story. "Presence-only observers" is compact but decodable; "is fetched only while someone wants it" is slightly loose ("wants it") but understandable.

**L416 — "*Crash-safe presence.* Graceful departures propagate instantly; owners heartbeat every 30s and stale members are reaped, with a native store-expiry backstop so a crashed node leaves no ghosts."**
**5/10** — Systems-heavy (criterion 2). "owners" is introduced without definition (owners of what?), and "reaped" + "native store-expiry backstop" assume infrastructure vocabulary. The payoff ("leaves no ghosts") is vivid and clear, but the mechanism between is opaque to a general reader.

**L417 — "*Deterministic convergence.* Metadata and config updates converge to the same result on every node, whatever the arrival order."**
**6/10** — The explanatory clause does clarify the header, but "Deterministic convergence" and "converge…whatever the arrival order" still presume distributed-systems literacy (criterion 2). Better than the header alone.

**L418 — "*`isolated: true`* moves text to per-member keys — the Cloudflare Durable-Object publish-contention knob."**
**4/10** — Very opaque (criteria 1 & 2). "text" (the text lane) is unexplained; "per-member keys" and "Durable-Object publish-contention knob" are deeply internal. Even a fairly advanced reader without Cloudflare Durable Objects context will stall. Needs a plain-language "what problem does this solve" first.

**L420 — "Every inbound client message is capped by `config.channel.messageLimit` (default 16 MiB); large payloads belong on streams."**
**8/10** — Clear cap statement with the default and a useful redirect to streams.

---

## `docs/pages/channel-config/+Page.mdx`

**Code comments (new) — "// Abuse protection", "// Max size of one inbound client message (bytes)"** — clear, well-labelled.

**Blockquote — "Want a tighter bound on what a client may send in one message (e.g. chat apps) → lower `messageLimit`."**
**8/10** — Consistent with the surrounding troubleshooting list; clear and actionable.

**"`messageLimit` is the hostile-input bound: a client declaring a gigabyte message is rejected from the declared length — the body is never buffered — and its connection is terminated."**
**6/10** — Strong security framing, but "rejected from the declared length" is awkward and momentarily unclear (criterion 3): it means "rejected based on the declared size before the body is read." Reword, e.g. "…is rejected on its declared size, before the body is buffered, and its connection is terminated."

**"Honest clients never get that far: the limit is advertised to them, and an oversized `send()`/`publish()` fails locally with a clear error."**
**8/10** — Clear and nicely contrasted with the hostile-client case; natural.

**"Large payloads belong on streams (`File`, `Blob`, `ReadableStream`), which move as bounded chunks and aren't subject to the cap."**
**8/10** — Clear redirect with concrete types; "bounded chunks" is mild jargon but fine.

---

## `docs/pages/channel/+Page.mdx`

**Blockquote (L14) — "Building on these, `Room` adds multi-party rooms with presence, participant metadata, private messages, and admin controls."**
**8/10** — Clear teaser linking channels to `Room`; phrasing is consistent with the `Room` page.

**"The receipt additionally carries `receivers` — the key's live subscription count (in-memory subscribers, Redis `PUBLISH` receivers, Cloudflare DO presence)."**
**7/10** — Clear core statement; the parenthetical (Redis `PUBLISH` receivers, Cloudflare DO presence) is jargon, but appropriate for this advanced page.

**"`0` truthfully means nobody anywhere is subscribed right now: the signal for a producer to pause expensive work until someone tunes in."**
**7/10** — Clear intent, and "tunes in" is a natural touch; "`0` truthfully means" is slightly awkward (criterion 3) — the "truthfully" is doing emphatic work ("accurately, across all transports") that reads oddly.

**"It counts subscriptions at the transport hop, not end viewers."**
**6/10** — Important caveat, but "at the transport hop" is jargon and the subscriptions-vs-end-viewers distinction is subtle for a vague reader (criterion 2).

---

## `docs/pages/redis/+Page.mdx`

**"[Rooms] work across instances too: room state lives in Redis keys, room events travel over Redis Pub/Sub."**
**8/10** — Clear and appropriately Redis-specific for this page; parallel structure ("state lives in… / events travel over…") reads well.

---

## `docs/pages/stream/+Page.mdx`

**List item (L68) — "`Room`: multi-party rooms with presence — who's connected, join/leave events, per-member streams."**
**8/10** — Clear one-line summary, consistent with the other primitives' descriptions in the list.

**"For multi-party use cases where *who is connected* matters — chat, game lobbies, video calls, collaboration — you can use a room:"**
**8/10** — Clear, concrete use cases; natural. Mirrors the `Room` page's opening, which is good for consistency.

**Blockquote — "Built on top of channels: a `Room` adds presence, participant metadata, private messages, and admin controls."**
**8/10** — Clear; consistent framing with the `Room` page and the `channel` page teaser.

---

## Suggested concrete fixes (highest leverage)

1. **L304** — replace "moving channels" with "switching rooms" / "moving between rooms". (Terminology clash on the one page that must keep `Room` and `Channel` distinct.)
2. **L239** — split the named-tracks sentence into: (a) what a track is, (b) how to publish/subscribe to one, (c) the `keyFrame` flag. Gloss "multiplexed".
3. **L418** — lead with the problem in plain words (e.g. "When many members publish at once on Cloudflare, they contend on one storage key; `isolated: true` gives each member its own key"), then name the knob.
4. **L416** — define "owners" (or drop it), and gloss "reaped" / "store-expiry backstop" — e.g. "each member is refreshed every 30s; members that stop refreshing are removed, and the store expires them as a backstop, so a crashed node leaves no ghosts."
5. **L113 / L198 ("writes nothing") / L259** — expand these compressed fragments by a few words each so they don't rely on the next code block to disambiguate.
6. **channel-config** — reword "rejected from the declared length".
7. **Glossing pass** — for the distributed-systems / A/V terms that a vague reader hits cold (*at-most-once*, *congestion control*, *jitter buffering*, *deterministic convergence*, *caller assertions*), add a 2–4 word inline gloss. Most sit in Binary/Production, so the fix is localized.
