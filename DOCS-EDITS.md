# Docs edits — improving the low-rated sentences

Follow-up to `DOCS-REVIEW.md`. Every sentence I rated **≤ 6** ("low") in the review is edited here, and each edit is re-rated against the same three criteria (clarity / prior-reading / naturalness). Where an edit stayed low, I explored 10+ wordings and picked the best (see the binary-transport sentence).

**Accuracy note.** The mechanism-heavy rewrites (`isolated: true`, crash-safe presence, `send()` semantics, the `Sender` fallback) were checked against the implementation before rewording:

- `isolated` moves the **text** lane (`publish()`) to a per-member key — `server.ts` `_publishText`: `this._isolated ? roomMemberDataKey(this.id, from) : roomTextKey(this.id)`. Binary is already per-`(member, track)`, so the rewrite scopes the claim to `publish()`.
- Heartbeat = 30s, member TTL = 120s, KV-expiry backstop = 180s — `constants.ts` (`ROOM_HEARTBEAT_INTERVAL_MS`, `ROOM_MEMBER_TTL_MS`, `ROOM_MEMBER_KV_TTL_MS`). The "every 30s" claim is correct.
- `Sender` is "the live `RemoteParticipant` whenever the holder's room view knows the sender, or a server-stamped snapshot otherwise (… a message racing ahead of its sender's join)" — `types.ts` JSDoc.

**Result:** all 18 low sentences now rate **7–9**. Nothing stayed in the low band. The one edit that first landed at 7 (the binary-transport warning) was pushed to 8 through the 10-wording pass below.

## Summary

| # | Location | Before | After | The fix |
|---|---|---|---|---|
| 1 | room L75 | 6 | **8** | drop the abstract "a value and a type" for concrete "the object you call statics on / the type you annotate with" |
| 2 | room L97 | 6 | **8** | name the wiring being skipped; plain-language the reference-stability |
| 3 | room L113 | 6 | **8** | "one handler for every message, whoever sent it" instead of "you process the same way" |
| 4 | room L175 | 6 | **8** | gloss "at-most-once" as "delivered … at most once, never retried or stored" |
| 5 | room L180 | 6 | **8** | spell out "the copy you hand out" as "the client-side `join()`s that run after you return the room" |
| 6 | room L198 | 6 | **8** | "adds no member and leaves no trace" instead of "writes nothing" |
| 7 | room L239 | 5 | **8** | split one 5-idea sentence into three; gloss "multiplexed" |
| 8 | room L247 (a) | 6 | **8** | frame the jargon as "what large-scale live video needs" (10-wording pass) |
| 9 | room L247 (b) | 6 | **8** | lead with the plain benefit before naming WebTransport |
| 10 | room L259 | 6 | **8** | split the two uses (`receivers` vs `onDemand`) into two clauses |
| 11 | room L304 | 4 | **9** | "switching rooms", not "moving channels" (terminology clash) |
| 12 | room L361 | 6 | **8** | "unchecked assertions you supply … doesn't validate the shape" for "caller assertions" |
| 13 | room L408 | 6 | **8** | make the fallback case concrete; "without special-casing" for "never branch" |
| 14 | room L416 | 5 | **8** | define "owners"; drop "reaped" / "store-expiry backstop" for plain English |
| 15 | room L417 | 6 | **8** | "land in the same final state … no matter what order they arrive in" |
| 16 | room L418 | 4 | **8** | explain what/why/no-op-elsewhere; drop "publish-contention knob" |
| 17 | channel-config | 6 | **8** | "rejected on that declared size — before its body is ever buffered" |
| 18 | channel | 6 | **8** | gloss "transport hop" inline and name the end-viewer gap |

---

## Details

### 1. room L75 — `Room` as value and type

- **Before (6):** "Like the built-in `Date`, `Room` is both a value and a type: the statics (`Room.get()`) and the instance type (`const lobby: Room`)."
- **After (8):** "`Room` is both the object you call statics on (`Room.get()`) and the type you annotate with (`const lobby: Room`) — like the built-in `Date`."
- **Why better:** "a value and a type" is a TypeScript abstraction a vague reader stalls on (criterion 2). The edit replaces each abstract half with a concrete action ("the object you call statics on", "the type you annotate with"), tied to the code shown, and keeps the `Date` analogy as a trailing aid rather than the lead.

### 2. room L97 — snapshot / onChange for frameworks

- **Before (6):** "For UI frameworks, skip the listener bookkeeping: `snapshot()` returns an immutable whole-room view whose reference only changes when the room does, and `onChange()` fires on any change:"
- **After (8):** "For UI frameworks, skip the manual `onJoin`/`onLeave`/`onUpdate` wiring: `onChange()` fires on any change, and `snapshot()` returns an immutable view of the whole room that keeps the same reference until something changes — exactly what `useSyncExternalStore` needs:"
- **Why better:** "the listener bookkeeping" assumed the reader had internalised the previous snippet — the edit names exactly what's being skipped (the `onJoin`/`onLeave`/`onUpdate` wiring). "keeps the same reference until something changes" is plainer than "whose reference only changes when the room does", and "exactly what `useSyncExternalStore` needs" states *why* reference-stability matters (criterion 2). Verified against the `snapshot()` JSDoc.

### 3. room L113 — room-level chat

- **Before (6):** "Room-level, for messages you process the same way:"
- **After (8):** "Room-level — one handler for every message, whoever sent it:"
- **Why better:** "the same way" left the reader asking "the same way as what?" (criterion 1). The edit says the actual thing — one handler, all senders — and sets up the per-member contrast that follows.

### 4. room L175 — private-message semantics

- **Before (6):** "Private messages are at-most-once signaling (invites, whispers, game moves)."
- **After (8):** "Private messages are one-shot signaling (invites, whispers, game moves): delivered to a live participant at most once, never retried or stored."
- **Why better:** "at-most-once signaling" is distributed-systems vocabulary (criterion 2). The edit keeps the precise guarantee ("at most once") but glosses it in plain terms ("one-shot … never retried or stored"), which also foreshadows the very next sentence about durable messaging.

### 5. room L180 — guard coverage

- **Before (6):** "It covers every membership granted through that room object — server-side `join()`s and the client `join()`s on the copy you hand out."
- **After (8):** "It covers every join made through that room object — both your server-side `join()`s and the client-side `join()`s that run after you return the room from the telefunction."
- **Why better:** "the copy you hand out" required holding the "one type, both sides" model in mind (criterion 2). The edit spells out the mechanism — client joins that run after the room is returned — and uses "join" rather than the heavier "membership granted".

### 6. room L198 — rejected join

- **Before (6):** "A rejected join writes nothing."
- **After (8):** "A rejected join adds no member and leaves no trace."
- **Why better:** "writes nothing" made the reader supply the missing object ("writes … to what?"). The edit states the observable consequence directly. Matches the `JoinGuard` JSDoc: "before any membership state is written."

### 7. room L239 — named tracks (was the worst single sentence)

- **Before (5):** "Mic, camera, and screen share are *named tracks* multiplexed over one member's stream: publish with `{ track }`, subscribe with `{ track }` to receive only that substream (`{ track: null }` is the default lane), and the `keyFrame` flag rides each frame (`info.keyFrame`) so decoders can resync:"
- **After (8):** "Mic, camera, and screen share travel over one member's stream as *named tracks* — several substreams sharing one connection. Publish with `{ track }`; subscribe with `{ track }` to receive only that substream (`{ track: null }` is the default, unnamed track). Each frame also carries a `keyFrame` flag (`info.keyFrame`) so decoders know where they can resync:"
- **Why better:** one sentence carried five distinct ideas plus "multiplexed", "substream", "rides each frame" (criteria 1 & 2). Split into three: (1) what a named track is, with "multiplexed" glossed as "several substreams sharing one connection"; (2) how to publish/subscribe; (3) the keyframe flag. Each is now a single idea.

### 8. room L247 (a) — transport limits — *see 10-wording pass below*

- **Before (6):** "That's ideal for binary *data* and small or controlled-network A/V — but it has no congestion control, simulcast, or jitter buffering."
- **After (8):** "That's ideal for binary *data* and for small or controlled-network A/V — but it lacks what large-scale live video needs: congestion control, simulcast, and jitter buffering."
- **Why better:** the three specialist terms stay (they're the right keywords for the engineer this `<Warning>` targets), but they're now framed as "what large-scale live video needs", so a reader who doesn't know the terms still gets the takeaway: this transport isn't for big video.

### 9. room L247 (b) — WebTransport future

- **Before (6):** "An unreliable WebTransport datagram lane that would lift this ceiling is being explored in [telefunc#449]."
- **After (8):** "Lifting this ceiling would need a lane that trades reliability for latency — dropping late frames instead of queuing them; an unreliable WebTransport datagram channel is being explored for that in [telefunc#449]."
- **Why better:** the original opened with a four-word jargon stack ("unreliable WebTransport datagram lane"). The edit leads with the plain trade-off ("trades reliability for latency — dropping late frames instead of queuing them"), which *is* what "unreliable datagram" means, then names the technology.

### 10. room L259 — receivers vs onDemand

- **Before (6):** "Each `publishBinary()` receipt also reports `receivers`, the track's live subscriber count at that moment — use it while publishing, and `onDemand` to learn when to resume while paused."
- **After (8):** "Each `publishBinary()` receipt also reports `receivers` — the track's live subscriber count at that moment. Use it to react while you're actively publishing; use `onDemand` (above) to learn when to resume once you've paused."
- **Why better:** the original stitched two different tools (`receivers`, `onDemand`) and two different situations (publishing, paused) into one elliptical tail. The edit gives each its own clause and states the situation each applies to.

### 11. room L304 — switching rooms (the terminology clash, lowest score)

- **Before (4):** "Membership is per room, so moving channels is a `leave()` + `join()`:"
- **After (9):** "Membership is per room, so switching rooms is a `leave()` then a `join()`:"
- **Why better:** "channels" is Telefunc's *own* separate primitive — on the one page whose job is to distinguish `Room` from `Channel`, using "channels" to mean "rooms" is a real ambiguity (criterion 1). "switching rooms" removes it; "a `leave()` then a `join()`" also reads better than "+".

### 12. room L361 — typed-metadata generics

- **Before (6):** "The parameters are caller assertions (like `querySelector<T>`)."
- **After (8):** "These type parameters are unchecked assertions you supply — Telefunc trusts them and doesn't validate the shape, exactly like `querySelector<T>`."
- **Why better:** "caller assertions" is subtle TS terminology. The edit says what it means — you supply the types, Telefunc trusts them, nothing is validated — so the `querySelector<T>` analogy lands even if the reader didn't already know the pattern.

### 13. room L408 — the `Sender` fallback

- **Before (6):** "It's the live `RemoteParticipant` when the room knows the sender, and a plain object with the same fields otherwise — either way you read `from.id` / `from.meta` and never branch."
- **After (8):** "When your room view already knows the sender, `from` is their live `RemoteParticipant`; otherwise — e.g. a message that arrived just before its sender's join — it's a plain object with the same `id`, `meta`, and `identity`. Either way the fields are identical, so you read `from.id` / `from.meta` without special-casing."
- **Why better:** "when the room knows the sender" was vague about *when it wouldn't* (criterion 1); the edit gives the concrete case (a message racing ahead of its sender's join — per the `Sender` JSDoc). "without special-casing" replaces the casual "never branch" (criterion 3).

### 14. room L416 — crash-safe presence

- **Before (5):** "Graceful departures propagate instantly; owners heartbeat every 30s and stale members are reaped, with a native store-expiry backstop so a crashed node leaves no ghosts."
- **After (8):** "A clean `leave()` shows up everywhere at once. A crash can't: the node holding each connection refreshes its members every 30s, and any member that stops being refreshed is removed and its departure announced — with the store's own key-expiry as a final backstop, so a crashed node leaves no ghosts."
- **Why better:** "owners" was undefined and "reaped" / "store-expiry backstop" were infra jargon (criterion 2). The edit defines the actor ("the node holding each connection"), replaces "reaped" with "removed and its departure announced", and says "the store's own key-expiry as a final backstop" in plain words. Numbers verified against `constants.ts`.

### 15. room L417 — deterministic convergence

- **Before (6):** "Metadata and config updates converge to the same result on every node, whatever the arrival order."
- **After (8):** "Metadata and config updates land in the same final state on every node, no matter what order they arrive in."
- **Why better:** "converge … whatever the arrival order" presumes distributed-systems literacy. "land in the same final state … no matter what order they arrive in" says the same thing in everyday words. (The bold header "Deterministic convergence" is kept as the technical label; the explaining sentence now carries the meaning.)

### 16. room L418 — `isolated: true` (was near-opaque)

- **Before (4):** "moves text to per-member keys — the Cloudflare Durable-Object publish-contention knob."
- **After (8):** "routes each member's `publish()` messages through their own pub/sub key instead of one key shared by the whole room. On platforms that map each key to its own coordinator (e.g. Cloudflare Durable Objects), that removes the contention of an entire room publishing through a single key; elsewhere it makes no difference."
- **Why better:** "the Cloudflare Durable-Object publish-contention knob" and the unexplained "text" were opaque even to advanced readers (criteria 1 & 2). The edit says what it does (`publish()` messages → per-member key), why it helps (removes contention where each key is its own coordinator), and that it's a no-op elsewhere. "text" is scoped accurately to `publish()` (binary is already per-member — verified in `server.ts`).

### 17. channel-config — hostile-input bound

- **Before (6):** "a client declaring a gigabyte message is rejected from the declared length — the body is never buffered — and its connection is terminated."
- **After (8):** "a client that declares a gigabyte message is rejected on that declared size — before its body is ever buffered — and its connection is terminated."
- **Why better:** "rejected from the declared length" was awkward and momentarily unclear (criterion 3). "rejected on that declared size — before its body is ever buffered" makes the sequence explicit: the declared size is what triggers rejection, and it happens before any buffering.

### 18. channel — receivers caveat

- **Before (6):** "It counts subscriptions at the transport hop, not end viewers."
- **After (8):** "It counts subscriptions at the transport hop — in-memory listeners, or peer instances subscribed over Redis/Cloudflare — not the end viewers those instances may each serve."
- **Why better:** "at the transport hop" was jargon and the subscriptions-vs-viewers gap was subtle (criterion 2). The edit glosses "transport hop" with the concrete cases (in-memory listeners / peer instances) and names *why* it differs from viewers — one subscribed instance may serve many.

---

## 10-wording pass — the binary-transport warning (item 8)

My first edit of this sentence landed at **7** ("media machinery real-time video needs at scale" — "media machinery" reads slightly oddly, criterion 3), so I explored alternatives. The constraint is a real tension: **keep the exact keywords** (congestion control / simulcast / jitter buffering — the engineer weighing production video is likely searching for exactly those) while **framing them so a non-expert still gets the takeaway**.

| # | Wording of the "but …" clause | Rating | Note |
|---|---|---|---|
| V1 | "…but it lacks the media machinery real-time video needs at scale: congestion control, simulcast, jitter buffering." | 7 | "media machinery" is an odd phrase (C3) |
| V2 | "It is not a media transport, though: there's no congestion control, simulcast, or jitter buffering." | 8 | crisp, but "media transport" is itself a little vague |
| V3 | "…a reliable, ordered pipe can't do what live video needs: adapt to congestion, send multiple qualities (simulcast), or smooth jitter." | 8 | most novice-friendly, but **loses the exact keywords** "congestion control"/"jitter buffering" |
| V4 | "…none of the machinery large-scale video relies on — congestion control, simulcast, jitter buffering — so many high-bitrate streams will overwhelm it." | 7 | adds a consequence that duplicates the next sentence |
| V5 | "…no congestion control, simulcast, or jitter buffering, the adaptations that keep large-scale live video smooth." | 8 | appositive gloss reads well |
| V6 | "Production-scale live video needs more than a reliable pipe — congestion control, simulcast, jitter buffering — and rooms provide none of it." | 8 | restructures away from the "ideal for X, but Y" parallel |
| V7 | "…not for production-scale video, which needs congestion control, simulcast, and jitter buffering that this transport doesn't provide." | 7 | overlaps the following "For production…" sentence |
| V8 | "…lacks the three things large-scale live video depends on: congestion control (backing off when the network is full), simulcast (multiple quality layers), and jitter buffering (smoothing arrival timing)." | 8 | fully glossed & keeps keywords, but long for a `<Warning>` |
| V9 | "…but not for video at scale: it has no congestion control, no simulcast, no jitter buffering." | 8 | punchy "no X, no Y, no Z" |
| V10 | "…but it's a plain reliable pipe, without the congestion control, simulcast, or jitter buffering that large-scale live video needs." | 8 | "plain reliable pipe" is a nice anchor |
| **V11 (chosen)** | "…but it lacks **what large-scale live video needs**: congestion control, simulcast, and jitter buffering." | **8** | keeps exact keywords, plain framing, shortest of the 8s |

**Chosen: V11** — it keeps the three exact terms (for the searching engineer) yet frames them as "what large-scale live video needs" (for everyone else), and it's the most concise of the top options, which matters inside a `<Warning>`. V3 and V8 score equally on clarity but either drop the keywords (V3) or run long (V8). This is the wording now in the doc.

---

## Not edited (and why)

- **Message-lanes table (`room` L34–38), rated 6.** Its problem is *placement and density* (it uses `me.`/`room.`/`member.` bindings before any example introduces them), not wording — the fix is structural (move it after the guides, or introduce the bindings first), not a sentence rewrite. Flagged in `DOCS-REVIEW.md`; left for a layout decision rather than reworded here.
- **Sentences rated 7.** These have only minor nits and sit outside the "low" band, so they were left as-is (e.g. "Sending to an unknown or departed participant rejects.", "`0` truthfully means nobody anywhere is subscribed right now"). Happy to take a pass at the 7s too if you want them tightened.
