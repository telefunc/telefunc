# Discord Clone — a `Room` stress test

A full Discord-style app built on [telefunc](https://telefunc.com)'s new `Room` primitive
([#436](https://github.com/telefunc/telefunc/pull/436)), written to find out where real app code
fights the API. The app is real — accounts, SQLite persistence, offline-capable DMs, voice
channels with camera and screen share — and every place the Room API forced complexity into app
code is called out inline and collected in [Findings](#findings-room-pr-436-pain-points).

The stress test runs in **rounds**, each following a base update of the Room PR and migrating the
app onto whatever it newly offers — so each finding below records what the adoption actually
deleted from app code.

- **Round 1** produced findings 1–13.
- **Round 2** migrated onto the first wave of adoptions (identity, join guards, named binary
  tracks, `snapshot()`/`onChange()`, `onActivity`, per-field `update`, leave causes,
  `getOrCreate`, typed metadata) and produced findings 14–16 — including a real bug in the new
  `snapshot()` cache that the migration flushed out (finding 15).
- **Round 3** adopted source-selective tracks (`receivers` on the publish ack).
- **Round 4** followed the base to `e810416`, which reshaped the surface again:
  guards split into `onBefore*`/`onAfter*` pre/post-commit hooks, the activity lane was **removed**,
  and `onDemand()`, `Room.get(…, { tail: true })`, `setAttributes()` and `publish({ coalesce })`
  landed. Migrating onto those closed four findings outright — the lossless-history fence
  (14), the two-clocks split (13), per-field metadata (5), and the encoder-pause probe hack (6) —
  and turned finding 10 into an instructive round-trip (adopted → removed upstream → re-derived a
  better way). A whole-app **smell audit** this round surfaced findings 17–22 (a coherent family:
  identity and cross-room aren't first-class, and change events carry no delta) plus a set of
  app-level bugs, fixed here.
- **Round 5** followed the base to `f0f221f`, where the author **folded two of this app's findings
  into `#436`**: the `snapshot()` cache-poisoning fix (finding 15) and the remaining view-type
  exports (finding 16). This branch takes the upstream fix + tests and adopts the exported types —
  both findings now close.
- **Round 6** followed the base to `3a619b9`, where the author began landing the audit findings as
  "Tranche-1": **`Room.setAttributes()`** (per-key room-meta merge, finding 21) and a **typed
  publish generic** `Room<Meta, PMeta, Pub>` (finding 20). Adopted both — `onSetTopic` is now a
  one-field merge, and the channel room carries `ChannelPublish` so subscribers drop their casts.
- **Round 7** followed the base to `27f1ceb` ("Tranche-2"), landing the identity/delta family:
  an **identity index** (`Room.send({ identity })`, index-resolved `removeParticipant({ identity })`
  — findings 17 send-half, 18) and **`snapshot({ by: 'identity' })`** + **`onParticipantUpdate`**
  (finding 19). Adopted — DM delivery is now two identity-addressed sends (no roster fan-out) and the
  member sidebar drops its hand-rolled dedupe.
- **Round 8** followed the base to `49700a9`: **`Room.getParticipants(id, { identity })`** (finding 17
  query-half — the DND check no longer loads the roster) and a telefunction **`Abort`'s message now
  on `err.message`** (finding 7 — `errorMessage()` drops the `abortValue` dig-out). Findings 7 and 17
  now fully close.
- **Round 9** followed the base to `3d5ad65`, which added a **server seat** (`join({ server: true })` →
  `room.server`): a full participant excluded from presence, "for a bot, a command sink". Adopted —
  RoomBot joins each channel as its server seat (publishes replies, listens for commands, but isn't
  counted in the channel's roster) while staying a *visible* guild member. (The round's other
  additions — `me.send()` returning its receipt, `Room.list<M>()`, and the identity-snapshot view
  exports — touch lanes this app doesn't use or already covers.) *(The seat was generalized/renamed
  in round 10 — see below.)*
- **Round 10** followed the base to `46ab8ae`, which **generalized the round-9 server seat into
  hidden participants** (`join({ hidden: true })` — any number, read via `getParticipants({ hidden:
  true })`; the one-per-room `{ server: true }` / `room.server` pair is gone) and added
  **`publish({ retain })` / `publishBinary({ retain })`** — MQTT-style server-retained replay to late
  subscribers. Adopted both: the bot's channel seat is now `{ hidden: true }` (`server/bot.ts`), and
  video keyframes publish with `{ retain: true }` so a viewer joining an *active* stream paints at
  once instead of waiting for the next periodic keyframe (`app/call.ts`) — `onDemand` still owns
  pause/resume, `retain` owns late-joiner seeding. `retain` is the publish-lane replay finding 22
  asked for; the `Room.send` twin (22b) is still open.
- **Round 11** followed the base to `e240b5b`, which added an **opt-in DM ack**: `me.send(to, data,
  { ack: true })` waits for the recipient's `listen` handler to run and resolves with its return
  (request/response), rejecting if they leave before handling. Nothing to adopt — this app closes the
  member-to-member `me.send()` lane by guard (finding 2), DMs are server-delivered, and `ack` is still
  live-only (both parties present), so it's request/response, not the offline-capable messaging
  finding 2 is about. Merged to stay current and verified: `me.listen()` (which carries the app's DM
  notices) is unaffected — its callback return type widened `void`→`unknown`, source-compatible.
- **Round 12** followed the base to `c6ea3ca`, which hardened **hidden participants** (now
  server-only — never streamed to clients, so no enumeration leak), refined the **ack receipt**
  (`{ response, seq, timestamp }` — a superset of plain send's receipt), fixed **Cloudflare `receivers`**
  threading, and clarified **authority identity** (identify by trusted identity, not user-supplied meta)
  + ack retry semantics. Nothing to adopt — the app doesn't use ack (finding 2 design), hidden members
  work transparently at the API level (no visibility leak for the bot), and Cloudflare's receivers
  count now flows through correctly. Merged, verified, merged to stay current.

**Features**

- Accounts (register/login, scrypt + httpOnly session cookie); the first account owns the server
- Text channels: live messages, history with pagination, topics, create/delete, unread badges
- Typing indicators (ephemeral), banned-word moderation, server-wide announcement banners
- Direct messages: conversation list, live delivery, works with **offline** recipients, Do-Not-Disturb
- Voice channels: live occupancy in the sidebar, microphone (Opus), camera (VP8), **screen share**,
  mute — all over Room named binary tracks via WebCodecs, no WebRTC
- Presence: online/idle/dnd status, multi-tab-aware member list, kick (with the kicker's name
  arriving on the leave event itself), crash-safe cleanup
- RoomBot: a server-side member (`!help`, `!ping`, `!members`, `!roll`, DM replies)

**Stack**: Vike + vike-react · telefunc (remote functions + rooms) · SQLite via `node:sqlite`
(Node's built-in driver — a native module would have to compile on every platform installing the
monorepo, which broke Windows CI; no ORM supports `node:sqlite` yet, so the data layer is
hand-written prepared statements in `database/queries.ts`) · Zustand · Tailwind v4 · Hono ·
WebCodecs.

## Running it

```bash
pnpm install && pnpm build   # repo root (workspace `telefunc` is linked)
cd test/discord-clone
pnpm dev                     # http://localhost:3000
```

Register a user (the first one becomes the server owner), then open a second browser profile —
or an incognito window — and register another. The database lives in `.data/` (`DISCORD_CLONE_DB`
overrides; the e2e suite uses `:memory:`). Node ≥ 22.5 (`node:sqlite`).

## Architecture

The **database is the durable truth** (users, sessions, channels, messages, DMs) and **rooms are
the live layer** (presence, delivery, events). Rooms are recreated from DB rows at boot
(`Room.getOrCreate`); nothing irreplaceable lives in them.

Every membership is granted **server-side, through a telefunction**, so it carries the trusted
app identity: `join(meta, { identity: user.id })` — metadata is display state, `identity` is who
you are (verified, not client-echoed).

| Discord concept | How it's built |
|---|---|
| Guild + online members | One guild room; joins are identity-stamped, members carry `{ name, color, status, admin }` display metadata. The member list groups participants by `identity` (two tabs = one row) |
| Text channel | One room per channel. **Viewing** = membership (grants publish), obtained via the `onOpenChannel` telefunction; only the *open* channel's text lane is subscribed — unread dots for the rest ride one guild-lane activity ping per message (finding 10) |
| Message history | `messages` table, written by the channel's `onAfterPublish` hook with the message's authoritative `seq`/`timestamp` (`author_id: from.identity`); the first read is fenced losslessly by `Room.get(…, { tail: true })` (finding 14), paginated by a `seq` cursor |
| Typing indicator | Ephemeral `publish({ kind: 'typing' })` — same lane as chat, skipped by the guard (never persisted, never acked as activity) |
| Moderation | `onBeforePublish` guard throws → rejection travels the ack back to the sender's promise; persistence is the separate `onAfterPublish` post-commit hook |
| DMs | **DB-first, server-delivered**: telefunction writes the row, then two `Room.send(room, { identity }, …)` calls push it to both users' live tabs — index-resolved server-side, no roster fan-out (finding 17). Works offline; DND enforced server-side. The member-to-member `me.send()` lane is deliberately closed by a guard (see finding 2) |
| Channel create/topic/delete | `Room.create` / per-key `Room.setAttributes({ topic })` / `Room.close`; the guild's announce lane doubles as the directory feed (finding 4) |
| Kick | One `Room.removeParticipant(roomId, { identity }, { reason })` per room — the leave lands as `{ type: 'removed', reason }` on the kicked client, no side-channel notice (finding 12) |
| Voice/video | One room per voice channel; capacity enforced by its `onBeforeJoin` guard. Mic/camera/screen are **named binary tracks** (`publishBinary(frame, { track, keyFrame, retain })` / `subscribeBinary(cb, { track })`), each paused by `onDemand` when unwatched; keyframes are **retained** so a late subscriber paints at once (finding 22); mute/camera/screen state merges one field at a time via `setAttributes` |
| Member sidebar / channel list | `room.snapshot()` + `room.onChange()` — the `useSyncExternalStore` contract, projected into Zustand |
| Bot | Same Room API, no browser. A **visible guild member**, but joins each text channel as a **hidden participant** (`join({ hidden: true })`) — a full participant (publishes replies, subscribes to commands) that's excluded from the channel's presence count/roster (`server/bot.ts`) |

Module map: `database/` (row types + DDL, `node:sqlite` bootstrap, all SQL in `queries.ts`) ·
`server/` (auth, sessions, room bootstrap, guards, bot) · `telefunc/` (the API: enter, channels,
DMs, admin) · `app/` (Zustand store = the Room→React adapter, `call.ts` = the WebCodecs media
engine, `ui/`) · `shared/` (types crossing the wire, including the typed
`Room<Meta, ParticipantMeta>` aliases).

## Testing

`.test-dev.test.ts` / `.test-preview.test.ts` drive **two real users** (separate browser
contexts) through everything above — including camera frames and screen share decoded across the
wire, using Chromium's fake media devices (flags in the repo-root `test-e2e.config.mjs`).

```bash
pnpm exec test-e2e test/discord-clone/.test-dev.test.ts   # repo root
```

## Findings: Room PR #436 pain points

Ordered by how much app complexity each one caused in round 1. "Receipt" = where this app pays
(or paid) for it. Findings the PR has since addressed are marked **✔ adopted upstream**, with
what migrating onto the adoption actually deleted.

### 1. Cross-room identity is entirely app glue — ✔ adopted upstream

Participant IDs are per-room **and** per-connection: the same human is a different participant in
the guild, in every channel, in voice — and gets new IDs on every reload. Round 1 stamped
`userId` into every membership's metadata and correlated by hand (and shipped the natural bug
once: passing a channel-room participant ID to a guild-room API → `Participant not found`).

- Adopted: server-side-only `join(meta, { identity })`, surfaced as `from.identity` /
  `participant.identity`, plus identity-addressed sweeps (`Room.removeParticipant(roomId,
  { identity }, …)`).
- Round 2 receipts: `userId` is gone from `MemberMeta` (`shared/types.ts`) — display state and
  identity are separate concerns now; the `onAfterPublish` hook persists `author_id: from.identity`
  (verified, not client-echoed — `server/guards.ts`); the kick sweep is one call per room
  (`telefunc/admin.telefunc.ts`); the member list dedupes tabs by `participant.identity`
  (`app/store.ts`). The trade: every join goes through a telefunction — the right security posture
  anyway, and its one-time friction with the history fence (finding 14) is now moot since `tail`
  closes that fence regardless of which lane the join took.

### 2. The private lane can't carry real DMs — partially adopted

`me.send()` addresses a *participant*, which exists only while its connection lives — offline
users are unreachable, and nothing is replayed. Discord-style DMs (offline delivery, history)
had to be rebuilt DB-first with `Room.send()` fan-out to each live participant
(`telefunc/dms.telefunc.ts`); after that, the only job left for the `onBeforeSend` guard is to
close the lane so clients can't bypass the server path (`server/guards.ts`).

- Adopted: deliveries are now **held until the recipient's first `listen()`** — the round-1 race
  (a DM fired reactively on a join event could arrive before the joiner attached its listener)
  is gone.
- Still open (by design): the lane is ephemeral peer-signaling, not messaging. The DB-first
  architecture stands; the docs should keep positioning the lane that way. *(Round 11 added opt-in
  `me.send(to, data, { ack: true })` — confirmed request/response delivery that resolves with the
  recipient's `listen` return. It sharpens the lane's purpose but stays live-only, both parties
  present, so it still can't carry offline DMs — the disposition holds.)*

### 3. Guards are per-instance — policy holes are one `Room.get()` away

A guard covers memberships granted through *that instance*, so policy + persistence must be
attached to **every** instance that hands out memberships. This app funnels all fetches through
`getGuardedGuild()`/`getGuardedChannel()` (`server/guards.ts`), but that's convention — any
direct `Room.get()` silently bypasses moderation *and* message persistence. `Room.guard()`
throwing on a second call also means helpers must own guarding exclusively.

- Suggestion: room-scoped guards (`Room.guard(roomId, ...)`), or guards as an option of
  `Room.get()`/`Room.create()` so the fetch and the policy can't be separated.

### 4. No directory events — partially adopted

Nothing tells a client (or the bot) that a room appeared. The guild's announce lane doubles as a
hand-rolled directory feed (`channel-created` in `telefunc/channels.telefunc.ts`, consumed in
`app/store.ts` and `server/bot.ts`). Deletion is fine — `onClose` *is* the signal.

- Adopted: `Room.list({ prefix })` — namespaced listing without fetching the world.
- Still open: created-events. The announce-lane directory pattern remains load-bearing here;
  worth blessing in the docs if it's the intended shape.

### 5. Every state write is a full replace — ✔ adopted upstream

- Adopted (round 2): `Room.update` patches per-field — updating a topic no longer re-supplies
  `size` (a finite cap used to silently reset to `Infinity`). Receipt: `onSetTopic` in
  `telefunc/channels.telefunc.ts` sends `{ meta }` alone.
- Adopted (round 4): `setAttributes(partial)` merges participant metadata by key — the whole-record
  read-modify-write is gone. Round-4 receipts: the six `setMeta({ ...meta, muted/camera/screen })`
  spreads in `app/call.ts` are now `setAttributes({ muted })` etc., and status in `app/store.ts`
  is `setAttributes({ status })`. Concurrent changes to *different* fields no longer clobber each
  other (the merge is per-key, not per-record).

### 6. One binary lane per member → hand-rolled multiplexing — ✔ adopted upstream

Mic, camera, and screen share are three concurrent streams. Round 1 multiplexed them onto the
member's single binary lane with an invented `[stream kind][flags]` frame envelope, per-frame
demux, and keyframe bookkeeping.

- Adopted: **named tracks** — `publishBinary(data, { track, keyFrame })`,
  `subscribeBinary(cb, { track })` with `info.keyFrame`, server-side per-(member, track) relay.
- Round 2 receipts: the envelope, demux switch, and flag bits are deleted from `app/call.ts`;
  what's left is pure WebCodecs. Subscribing per track also means a peer that never shares its
  screen costs nothing on the screen track.
- Round 3: tracks became selective **at the source** — the publish ack's `receivers` reports the
  track's live subscription count, so an unwatched encoder can pause.
- Round 4: `me.onDemand((track, count))` made "a viewer returned" a **first-class event**. Receipt:
  `demandGate` in `app/call.ts` pauses on an ack's `receivers === 0` and resumes on the `onDemand`
  signal — the round-3 keyframe-probe loop (drop-all-but-one-frame every couple of seconds, force
  a keyframe, read the probe's ack) is deleted. Alone in a voice channel, nothing is encoded or
  uploaded, and a returning viewer is noticed immediately instead of at the next probe.

### 7. Expected-rejection ergonomics differ by lane — ✔ adopted upstream

A guard `throw new Error('nope')` reaches the sender's promise verbatim through the publish ack
— great. The same throw in a telefunction was masked ("Internal Server Error") unless you used
`Abort`, whose client-side `Error.message` was generic — the real text hid in `err.abortValue`.

- Adopted: guard-rejected publishes are no longer *also* logged client-side as
  `[telefunc:channel-error]` — expected control flow stays out of the console (the e2e suite's
  `tolerateError` entry for it is gone).
- Round 4 clarified the guard shape: `onBefore*` (pre-commit, throw to reject) / `onAfter*`
  (post-commit, with the receipt). This app validates in `onBeforePublish`, persists in
  `onAfterPublish` (`server/guards.ts`).
- Adopted (round 8): a telefunction `Abort('…')` now **surfaces its own message on `err.message`**.
  Receipt: `errorMessage()` in `app/store.ts` drops the `err.abortValue` dig-out — expected
  failures (banned word, DND, capacity) read straight off `Error.message`, one path for guard-ack
  and telefunction rejections alike.

### 8. The channel-switch publish window

"Viewing = membership" means switching channels is a leave + join round-trip, and a message sent
during that window has no membership to publish through. Our first version dropped fast-typed
messages silently; the fix (chained switches + senders awaiting the in-flight join, see
`openChannel`/`sendMessage` in `app/store.ts`) is subtle enough to deserve a docs recipe. The
join is a telefunction (finding 1), so the window is a full HTTP round-trip — though round 4 at
least folded history *into* that one call (`onOpenChannel` returns membership + fenced history
together), so it's no longer join-then-separately-fetch.

### 9. Rooms are mutable emitters; UI frameworks want snapshots — ✔ adopted upstream

Round 1 hand-rolled the same adapter every React app would: `watchRoster()` + per-member
`onUpdate` bookkeeping + immutable projection + a store (~150 lines).

- Adopted: `room.snapshot()` (immutable, reference-stable until change) + `room.onChange()` —
  exactly the `useSyncExternalStore` contract.
- Round 2 receipts: the roster adapter is deleted; the member sidebar is one `onChange` →
  `snapshot().participants` projection (`wireGuild` in `app/store.ts`), and the channel list is
  `room.onChange(publishChannels)` (`wireChannel`). The migration also flushed out a real bug in
  the snapshot cache — finding 15.

### 10. Unread badges cost full subscription — ✔ resolved (adopted → removed → re-derived)

Discord-style unread indicators used to require subscribing to every channel's text lane — full
message traffic for channels nobody is reading. This finding took the most instructive path of
the whole stress test:

- Round 2 adopted `room.onActivity()` — a throttled, body-free per-channel signal. Only the open
  channel subscribed to messages; every other channel carried just the activity signal.
- Round 4 the base **removed the activity lane** (commit "remove the activity lane") — the docs
  dropped the unread-badges section entirely, taking the position that unread is the app's job.
  So this app re-derived it a *better* way: the channel's `onAfterPublish` hook pings one
  `channel-activity` event on the guild announce lane, which every client already listens to.
  Receipts: the per-channel `onActivity` subscription in `wireChannel` is gone; `markChannelActivity`
  in `app/store.ts` flips the dot from the guild lane; `server/guards.ts` fires the ping. Net cost
  dropped from one subscription *per channel* to **zero extra** — it rides a lane already open.
  Still deliberately not a count — Discord's plain-unread dot matches exactly.

### 11. No server-side join gate — ✔ adopted upstream

`size` was a hint and nothing enforced it: round 1's voice capacity check was client-side only.

- Adopted: an admission guard next to the publish/send guards (`onBeforeJoin` since round 4's
  pre/post-commit split).
- Receipts: voice rooms are guarded `onBeforeJoin: () => { if (channel.isFull) throw … }`
  (`server/guards.ts`); a full channel now rejects on the server, and the client just renders
  the rejection (`app/call.ts`).

### 12. Cross-lane ordering leaks into UX — ✔ adopted upstream

The round-1 kick flow sent a "you were kicked by X" notice on the private lane racing the
removal on the control lane; the client waited 500ms before concluding "disconnected" vs
"kicked".

- Adopted: every leave carries a cause — `onLeave((cause: LeaveCause) => …)` with
  `{ type: 'removed', reason }`, and `removeParticipant` accepts the reason.
- Round 2 receipts: the notice, the race, and the timer are deleted; the kicked screen renders
  straight from the leave cause (`wireGuild` in `app/store.ts`,
  `telefunc/admin.telefunc.ts`).

### 13. Smaller DX notes — mostly adopted

- ~~No `Room.getOrCreate()`~~ — ✔ adopted; boot seeding uses it (`server/rooms.ts`).
- ~~Metadata is `Record<string, unknown>` — every consumer casts~~ — ✔ adopted: `Room<Meta,
  ParticipantMeta>` type parameters flow through joins, guards, events, and snapshots; the
  app's `asMemberMeta` casts are gone (`shared/types.ts` declares `GuildRoom`/`ChannelRoom`
  aliases). `Sender` is exported now too.
- Still open: long-lived server-side state (the bot, the world latch, the SQLite handle) needs
  `globalThis` latches to survive dev-server module graphs and reloads — and Vite's *two* SSR
  module graphs will happily give you two `:memory:` databases (`database/db.ts`).
- ~~The `onPublish` guard has no timestamp/seq — history rows get `Date.now()` at persist time
  while live renders use `info.timestamp`; two clocks for one message.~~ — ✔ adopted (round 4):
  `onAfterPublish`'s receipt carries the message's central `seq` and `timestamp`, so
  `server/guards.ts` persists `at: info.timestamp` (the exact value the live lane rendered) and a
  `seq` column that history is now ordered and paginated by. One clock, one order.
- Still open: presence-death defaults are tuned for resilience, not chat UX: a closed tab holds
  its seat for `reconnectTimeout` (60s default) — this app lowers it to 10s (`+server.ts`).

## Findings surfaced by migrating

Migrating onto the adopted APIs surfaced three new items (14–16). Two are resolved; one is a real
upstream bug this app caught.

### 14. Server-side joins break the lossless-history fence — ✔ adopted upstream

The docs' original lossless-history recipe (subscribe → join → fetch: "the join ack proves the
subscription is active") assumed the join rides the room's own connection. But trusted `identity`
made **telefunctions the blessed join path** (finding 1), and a telefunction join travels the HTTP
lane, so its ack proved nothing about the WebSocket subscription. Round 2's fence was heuristic:
subscribe, then two round-trips (join + history fetch), and *hope* the subscription attached in
between. It never dropped a message in practice — but "wide margin" isn't "fence".

- Adopted (round 4): `Room.get(id, { tail: true })`. The telefunction fetches the room already
  relaying its live messages; the client holds them until its first `subscribe()`. Reading history
  *after* that fetch (in the same telefunction) cannot miss a message published meanwhile — the
  client renders the page, then subscribes to flush the held tail behind it, deduped by id. The
  ordering no longer depends on which lane the join took.
- Round-4 receipts: `onOpenChannel` (`telefunc/channels.telefunc.ts`) fetches with `{ tail: true }`,
  joins, and returns history in one call; `openChannel` (`app/store.ts`) renders that history and
  *then* subscribes. The heuristic "hope the race is won" comment is gone. This is the headline
  round-4 win — the one open finding the base built machinery specifically to close.

### 15. `snapshot()` cache poisoning on first roster load — ✔ found here, now fixed upstream

Real bug, caught by this app's first revival flow and worth the whole stress test: a
`room.onChange()` subscriber that synchronously calls `room.snapshot()` — **the documented
`useSyncExternalStore` pairing** — could permanently poison the snapshot cache during the first
roster reconcile. The order was: bump version → fire `onChange` (subscriber snapshots: caches an
*empty* roster under the new version) → then create the member entries *silently* (no second
bump) → every later `snapshot()` hits the poisoned cache. Symptom in this app: a revived guild
room whose member sidebar stayed empty forever while `getParticipants()` happily returned
everyone.

- The whole arc: this app's revival flow surfaced the bug (round 2); it was fixed and
  regression-tested on this branch and re-verified as still-needed against each base update
  (rounds 3–4); the Room author then **folded the fix into `#436` upstream** (base `f0f221f`,
  "fix(room): snapshot() cache poisoning …"). The upstream fix is the same shape — `reconcile()`
  bumps strictly *after* the mutations in both branches, `applyClosed()` bumps again after the
  clear — and slightly more thorough (it also bumps after the drift-reconcile diff). This branch
  now **takes upstream's `state.ts` and its equivalent regression tests** and drops the duplicates.
  A stress-test finding that went full circle: app → bug → fix → upstream.

### 16. The new surface's types weren't all exported — ✔ adopted upstream

- Through rounds 2–4 the public barrels grew to cover `Sender`, the guard types, and the
  `onAfter*` hooks + receipts — but `LeaveCause`, `ParticipantSnapshotView`, `RoomSnapshotView`,
  and `BinaryFrameInfo` were still missing, so the app couldn't name what `onLeave`, `snapshot()`,
  and `subscribeBinary` hand it.
- Adopted (round 5): the base (`f0f221f`) re-exports all four from both `telefunc` and
  `telefunc/client`. Receipts: `app/store.ts` drops the structural
  `ReturnType<GuildRoom['snapshot']>['participants'][number]` for the named
  `ParticipantSnapshotView<MemberMeta>`, and types the leave cause as `LeaveCause`;
  `telefunc/dms.telefunc.ts` names its roster param `RemoteParticipant<MemberMeta>[]` instead of
  `Awaited<ReturnType<…>>`. Nothing structural left to derive.

## Findings from a whole-app smell audit (17–22)

A systematic pass over the finished app (not just "what did adopting X delete?" but "what shape is
the app *still* forced into?") surfaced a coherent family of scaling/ergonomic pain points. The
throughline: **`identity` and cross-room operations aren't first-class, and change notifications
carry no delta**, so the app repeatedly pays `O(roster)`, `O(all-rooms)`, or `O(everything)` for
work that is logically `O(1)` or `O(one user)`.

### 17. `identity` is a second-class address — ✔ adopted upstream (send + query)

`Room` made `identity` the durable "who is this user" for `join`, `removeParticipant`, and reads —
but originally **send and query didn't speak it**, so acting on a user meant materializing the whole
roster and filtering in app code. The DM path paid `O(roster)` **KV reads** *per DM* (a DM-to-the-bot
three times); a 5,000-member guild shipped 5,000 records to deliver one message.

- Adopted (round 7, send): the **identity index** — `Room.send(id, { identity }, data)` fans out to
  every membership of an identity resolved in `O(memberships)`, not a roster scan; a signed-out
  recipient is a no-op. Receipt (`telefunc/dms.telefunc.ts`): DM delivery is two
  `Room.send(GUILD, { identity }, notice)` calls — the fan-out fetch, `filter`, and per-tab loop gone.
- Adopted (round 8, query): `Room.getParticipants(id, { identity })` — a server-side, `O(memberships)`
  presence read off the same index (`[]` for an absent identity). Receipt: the DND check reads only
  the target's tabs (`Room.getParticipants(GUILD, { identity: target.id })`), so `onSendDm` no longer
  loads — or even fetches — the guild roster. Send *and* query are now identity-addressed; the DM path
  is fully `O(one user)`.

### 18. No cross-room primitive — per-room sweep now O(k), cross-room enumeration still app-driven

`identity` spans rooms, but operations act on one room. Kicking (`telefunc/admin.telefunc.ts`)
enumerates **every channel** from the app's own DB and removes the identity from each.

- Adopted (round 7): each `removeParticipant({ identity })` is now index-resolved — `O(memberships
  of that identity in that room)` instead of a full per-room roster scan — so the sweep got much
  cheaper without an app change. (The kick already addressed by `{ identity }`, so it benefited
  transparently on the base upgrade.)
- Still open: the **cross-room enumeration** itself. There's no "remove this identity from all
  rooms" and no "which rooms is this identity in?" — `Room.list()` carries no membership — so the
  app still issues one call per channel. Fix: `removeParticipant({ identity }, { allRooms })` or an
  identity→rooms index.

### 19. `onChange` carries no delta and `snapshot()` is per-connection — grouping ✔ adopted, delta available

`onChange()` fired a bare "something changed", and `snapshot()` returned the whole per-connection
roster, so every consumer rebuilt its entire projection on every event — and the identity-grouping
the app wants (one row per user, not per connection) was recomputed by full scan each time.

- Adopted (round 7): `room.snapshot({ by: 'identity' })` returns the roster **grouped by user**
  (a user's tabs/connections collapsed), reference-stable like `snapshot()`. Receipt (`app/store.ts`):
  `pushMembers` drops the hand-rolled `Map` dedupe — it maps `snapshot({ by: 'identity' }).identities`
  straight to member rows. The server owns the grouping now (the client mirror of finding 17).
- Available, not yet fully adopted: `room.onParticipantUpdate((member, meta, prev))` — one delta
  subscription for every member's metadata change (vs. wiring `onUpdate` per handle from `onJoin`,
  which `app/call.ts` still does per peer). Consuming the *delta* to update incrementally (rather
  than `onChange` → rebuild-the-array) is coupled to the store's still-open selector/memo refactor,
  so this half is noted, not yet claimed.

### 20. `Room` isn't parameterized by its publish type — ✔ adopted upstream

`Room<Meta, ParticipantMeta>` typed the metadata but not what flowed over `publish`/`subscribe` —
`data` was `unknown`, so every consumer hand-cast (`data as ChannelPublish` etc.) on the
highest-traffic path, and a stale cast after a payload refactor was a silent runtime bug.

- Adopted (round 6): the base added an optional third generic, `Room<Meta, PMeta, Pub>`, threaded
  through `publish(data: Pub)` / `subscribe(RoomListener<P, Pub>)` / `getParticipant`. Receipts:
  `ChannelRoom = Room<ChannelMeta, MemberMeta, ChannelPublish>` (`shared/types.ts`), and the
  `as ChannelPublish` casts at the two subscribers — the client's open-channel lane (`app/store.ts`)
  and the bot's command lane (`server/bot.ts`) — are deleted; `data` arrives typed.
- Residual (minor): guard/hook callbacks (`onBeforePublish`/`onAfterPublish`) still receive
  `data: unknown` (not parameterized by `Pub`), so `server/guards.ts` keeps its cast; and the
  room-authored lanes (`announce`/`listen`, i.e. `GuildAnnouncement`/`SystemNotice`) aren't typed
  by `Pub` either. Threading `Pub` (or a separate announce type) into those would close the gap.

### 21. Room metadata has no per-key merge — ✔ adopted upstream

Participants got `setAttributes` (finding 5), but room meta didn't: `Room.update({ meta })` replaced
the whole object, so `onSetTopic` had to read current meta and respread `kind`/`name` just to change
`topic` — a read-modify-write with a round-trip in the middle, and concurrent edits to different
fields clobbered each other.

- Adopted (round 6): `Room.setAttributes(id, partialMeta)` — the admin, room-level mirror of the
  participant merge. Receipt: `onSetTopic` (`telefunc/channels.telefunc.ts`) is now
  `Room.setAttributes(roomId, { topic })` — one field written, no respread, no cross-field clobber.

### 22. Delivery-shape gaps: unscoped activity broadcast, and no reconnect-safe send

Two consequences of removed/absent primitives:
- **Unread is now a guild-wide broadcast.** With the activity lane gone (finding 10), the app pings
  `channel-activity` on the guild announce lane for *every* chat message — delivered to *every*
  member's client, just for dots. A busy channel turns the guild lane into an `O(members)`-per-message
  firehose (there's no server-side throttle). The re-derivation is correct but doesn't scale; the
  removed per-channel signal was the right shape. Fix: a scoped/throttled activity signal.
- **`Room.send` has no replay** *(22b — partly addressed in round 10)*. Channels got a lossless
  "history then live" fence (`Room.get({ tail })`, finding 14); the room-authored send lane had no
  equivalent — a DM delivered in the window after a recipient's participant is reaped but before their
  reconnect creates a new one is live-lost (it survives in the DB but only resurfaces on a full
  reload). Round 10 landed **`publish({ retain })` / `publishBinary({ retain })`** — MQTT-style
  server-retained replay to late subscribers, exactly this shape for the *publish* lane; the app
  adopted the binary half for video keyframes (a late viewer paints immediately, `app/call.ts`). But
  `retain` is one *latched* value, not a cursored backlog, and it rides `publish`, not the
  room-authored `Room.send` the DM path uses — so the reconnect-safe DM twin of `tail` is still open.

### App-level bugs the audit caught (fixed here, not Room-API issues)

The same pass found ordinary defects in the app itself — fixed in this round:

- **Boot latch memoized a rejected promise** (`server/rooms.ts`): one transient boot failure bricked
  every later `ensureLiveWorld()` for the process. Now cleared on failure so the next call retries.
- **`joinCall` had no switch fence** (`app/call.ts`): `session` is set only after `onJoinVoice`
  resolves, so rapid voice-channel switching (or leaving mid-join) orphaned the previous call — a
  leaked mic, `AudioContext`, and server-side voice slot. Now guarded by a join-intent token (the
  media twin of `openChannel`'s switch fence).
- **`openChannel` tail continuation wasn't fenced** (`app/store.ts`): after the awaited join, the
  history-record and unread-clear could land on a channel the user had already switched away from
  (wrongly clearing a genuinely-unread badge); and an `onOpenChannel` rejection bounced boot to the
  login screen / left a silently dead channel. Now re-checks the switch token and toasts the error.
- **Kick was non-atomic** (`telefunc/admin.telefunc.ts`): one channel throwing mid-sweep left a
  half-kicked user and skipped the announcement. Now best-effort per room, announce regardless.
- **`onSetTopic` had no authorization** (`telefunc/channels.telefunc.ts`): any logged-in user could
  rewrite any channel's topic. Now admin-gated, like delete/announce/kick.
- **`onCreateChannel` name race / ghost row**: a lost check-then-insert race threw a raw error
  instead of the friendly "already exists"; a failed `Room.create` left a DB row with no room. Now
  caught and compensated.
- **Stale screen-share `ended`**: the browser's "stop sharing" handler read the global session, so a
  late `ended` could toggle an unrelated later call. Now scoped to the capturing session.

Still-open app-level notes (not yet addressed, lower priority): the client re-renders broadly (the
root reads the whole store; no memoized selectors) and `recordMessage` re-sorts the whole thread per
message — both amplified by finding 19's lack of deltas; dedupe sets and message arrays aren't
windowed; `onListDmThreads` is a full-table scan capped at 500 rows (can drop older conversations);
expired sessions and the bot's `greeted` set are never pruned.

## Room API coverage

| API | Used | Where / why not |
|---|---|---|
| `Room.create` / `Room.get` (incl. `{ tail: true }`) / `Room.getOrCreate` / `Room.guard` / `Room.update` / `Room.setAttributes` / `Room.close` | ✔ | `server/rooms.ts`, `server/guards.ts`, `telefunc/channels.telefunc.ts` — `tail` fences history (finding 14); `setAttributes` merges the topic per-key (finding 21) |
| `Room.removeParticipant({ identity }, { reason })` / `Room.announce` / `Room.send(room, { identity }, …)` / `Room.getParticipants(id, { identity })` | ✔ | kick sweep (index-resolved per room, finding 18), banners + directory + activity feed, identity-addressed DM delivery + the DND presence read (finding 17) |
| `Room.join` (static) / `Room.list` | ✖ | memberships need identity + guards (server-side, per-instance); the app's channels table already knows the rooms (finding 4) |
| `room.join(meta, { identity })` (incl. `{ hidden: true }`) / `getParticipants` / `subscribe` / `onJoin` / `onLeave` / `onUpdate` / `onAnnounce` / `onClose` / `count` / `size` / `isFull` / `meta` | ✔ | throughout `telefunc/*`, `server/bot.ts`, `app/store.ts` — the bot's per-channel seat is a **hidden participant** (round 10), `server/bot.ts` |
| `getParticipants({ hidden: true })` | ✖ | the read-path for hidden participants; the bot holds the `LocalParticipant` that `join({ hidden: true })` returns, so it never re-reads its own seat (`server/bot.ts`) |
| `room.snapshot` / `snapshot({ by: 'identity' })` / `onChange` | ✔ | the Room→React adapter; the identity-grouped view is the member sidebar (finding 19) — `app/store.ts` |
| `room.onParticipantUpdate` | ✖ | available (finding 19 delta hook); `app/call.ts` still wires per-peer `member.onUpdate` — full adoption is tied to the store's selector/memo refactor |
| ~~`room.onActivity`~~ | — | removed upstream in round 4; unread is re-derived from `onAfterPublish` → guild announce (finding 10) |
| guards: `onBeforePublish` / `onAfterPublish` / `onBeforeSend` / `onBeforeJoin` | ✔ | validate + persist (split), closing the DM lane, voice capacity (`server/guards.ts`) |
| `room.getParticipant` / `onEmpty` / `onFull` / `isEmpty` / `isClosed` / `onAfterSend` / `onAfterJoin` | ✖ | roster filtering + live getters covered every need; the app persists in `onAfterPublish` only |
| `me.publish` / `publishBinary({ track, keyFrame, retain })` + ack `receivers` / `onDemand` / `listen` / `setAttributes` / `leave` / `onLeave` (with `LeaveCause`) / `selfDelivery: false` | ✔ | chat + typing, media tracks paused by `onDemand` when unwatched, **keyframes `retain`ed for late joiners** (round 10, finding 22), DM notices, per-field status & mute, channel switching, kick screen (`setMeta` superseded by `setAttributes` everywhere — finding 5) |
| `publish({ coalesce })` / `publish({ retain })` (text) | ✖ | `coalesce` is for lossy high-frequency streams (cursors) — this app's chat/typing are lossless/rate-limited already; text `retain` (last-message-only) has no home either — chat is DB-backed and replayed by `Room.get({ tail })` history, richer than one retained message (binary `retain` **is** used, above) |
| `me.send` / `me.send({ ack })` | ✖ | deliberately closed by guard — see finding 2; the round-11 `{ ack }` confirmed-delivery option rides this same lane and is still live-only, so it doesn't change the server-mediated DM design |
| `member.subscribeBinary(cb, { track })` / `onUpdate` / `onLeave` / `joinedAt` / `meta` / `identity` | ✔ | per-track media, live rosters, decoder lifecycle, tab dedupe |
| `member.subscribe` | ✖ | room-level `subscribe` + the verified `from` covered per-member needs |

## Future work

Message edit/delete, read-state persistence (server-side unread), attachments, role system
beyond owner/member, deafen, `@mention` highlighting, multiple guilds.
