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
- **Round 4** (this round) followed the base to `e810416`, which reshaped the surface again:
  guards split into `onBefore*`/`onAfter*` pre/post-commit hooks, the activity lane was **removed**,
  and `onDemand()`, `Room.get(…, { tail: true })`, `setAttributes()` and `publish({ coalesce })`
  landed. Migrating onto those closed four findings outright — the lossless-history fence
  (14), the two-clocks split (13), per-field metadata (5), and the encoder-pause probe hack (6) —
  and turned finding 10 into an instructive round-trip (adopted → removed upstream → re-derived a
  better way). The finding-15 snapshot fix is re-verified against `e810416` and still rides this
  PR for the author to fold in.

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
| DMs | **DB-first, server-delivered**: telefunction writes the row, then `Room.send()`s it to every live participant of both users (matched by `identity`). Works offline; DND enforced server-side. The member-to-member `me.send()` lane is deliberately closed by a guard (see finding 2) |
| Channel create/topic/delete | `Room.create` / per-field `Room.update({ meta })` / `Room.close`; the guild's announce lane doubles as the directory feed (finding 4) |
| Kick | One `Room.removeParticipant(roomId, { identity }, { reason })` per room — the leave lands as `{ type: 'removed', reason }` on the kicked client, no side-channel notice (finding 12) |
| Voice/video | One room per voice channel; capacity enforced by its `onBeforeJoin` guard. Mic/camera/screen are **named binary tracks** (`publishBinary(frame, { track, keyFrame })` / `subscribeBinary(cb, { track })`), each paused by `onDemand` when unwatched; mute/camera/screen state merges one field at a time via `setAttributes` |
| Member sidebar / channel list | `room.snapshot()` + `room.onChange()` — the `useSyncExternalStore` contract, projected into Zustand |
| Bot | A server-side member driven by the same Room API |

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
  architecture stands; the docs should keep positioning the lane that way.

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

### 7. Expected-rejection ergonomics differ by lane — partially adopted

A guard `throw new Error('nope')` reaches the sender's promise verbatim through the publish ack
— great. The same throw in a telefunction is masked ("Internal Server Error") unless you use
`Abort`, whose client-side `Error.message` is generic (the text hides in `err.abortValue`, see
`errorMessage()` in `app/store.ts`).

- Adopted: guard-rejected publishes are no longer *also* logged client-side as
  `[telefunc:channel-error]` — expected control flow stays out of the console (the e2e suite's
  `tolerateError` entry for it is gone).
- Round 4 clarified the guard shape: guards split into `onBefore*` (pre-commit, throw to reject)
  and `onAfter*` (post-commit, with the receipt). This app now validates in `onBeforePublish` and
  persists in `onAfterPublish` (`server/guards.ts`) — the two concerns that used to share one
  `onPublish` are cleanly separated, and persistence gets the authoritative order for free
  (finding 13).
- Still open: `Abort` message surfacing (`err.abortValue` vs `err.message`).

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

### 15. `snapshot()` cache poisoning on first roster load — found & fixed in this PR

Real bug, caught by this app's first revival flow and worth the whole stress test: a
`room.onChange()` subscriber that synchronously calls `room.snapshot()` — **the documented
`useSyncExternalStore` pairing** — could permanently poison the snapshot cache during the first
roster reconcile. The order was: bump version → fire `onChange` (subscriber snapshots: caches an
*empty* roster under the new version) → then create the member entries *silently* (no second
bump) → every later `snapshot()` hits the poisoned cache. Symptom in this app: a revived guild
room whose member sidebar stayed empty forever while `getParticipants()` happily returned
everyone.

- Fixed here (rides this PR for the Room author to fold in): `RoomState.reconcile()` now bumps
  strictly *after* the mutations in both branches, and `applyClosed()` bumps a second time after
  clearing members (leave callbacks run user code between the bumps — same hazard). Regression
  tests: `room.spec.ts` › "a subscriber that snapshots synchronously inside onChange cannot
  poison the cache" (roster load + close).
- Round-4 status: re-verified against base `e810416`. The base still bumps *before* the mutations
  in both `reconcile()` branches and `applyClosed()`, and ships no equivalent regression test — so
  the fix is genuinely still needed, and the merge preserves it (the two regression tests pass;
  the whole room suite is green at 101 tests). Still pending in #436 itself.

### 16. The new surface's types aren't all exported — partially adopted

The public barrels keep growing: `Sender`, the guard types (`SendGuard`/`PublishGuard`/`JoinGuard`),
and now round 4's post-commit hooks and receipts (`AfterPublishHook`/`AfterSendHook`/`AfterJoinHook`,
`RoomPublishReceipt`/`RoomSendReceipt`/`RoomJoinReceipt`) are all exported from `telefunc`. Still
missing: `LeaveCause`, `ParticipantSnapshotView`, `RoomSnapshotView`, `BinaryFrameInfo` — they
exist in the room module but aren't re-exported from the package entry, so an app still can't name
what `onLeave`, `snapshot()`, and `subscribeBinary` hand it. Receipt: `app/store.ts` still derives
the participant-snapshot type structurally
(`ReturnType<GuildRoom['snapshot']>['participants'][number]`), and the `onLeave` cause is typed
only by inference.

## Room API coverage

| API | Used | Where / why not |
|---|---|---|
| `Room.create` / `Room.get` (incl. `{ tail: true }`) / `Room.getOrCreate` / `Room.guard` / `Room.update` / `Room.close` | ✔ | `server/rooms.ts`, `server/guards.ts`, `telefunc/channels.telefunc.ts` — `tail` fences history (finding 14) |
| `Room.removeParticipant({ identity }, { reason })` / `Room.announce` / `Room.send` | ✔ | kick sweep, banners + directory + activity feed, DM delivery |
| `Room.join` (static) / `Room.list` | ✖ | memberships need identity + guards (server-side, per-instance); the app's channels table already knows the rooms (finding 4) |
| `room.join(meta, { identity })` / `getParticipants` / `subscribe` / `onJoin` / `onLeave` / `onUpdate` / `onAnnounce` / `onClose` / `count` / `size` / `isFull` / `meta` | ✔ | throughout `telefunc/*`, `server/bot.ts`, `app/store.ts` |
| `room.snapshot` / `onChange` | ✔ | the entire Room→React adapter (`app/store.ts`) |
| ~~`room.onActivity`~~ | — | removed upstream in round 4; unread is re-derived from `onAfterPublish` → guild announce (finding 10) |
| guards: `onBeforePublish` / `onAfterPublish` / `onBeforeSend` / `onBeforeJoin` | ✔ | validate + persist (split), closing the DM lane, voice capacity (`server/guards.ts`) |
| `room.getParticipant` / `onEmpty` / `onFull` / `isEmpty` / `isClosed` / `onAfterSend` / `onAfterJoin` | ✖ | roster filtering + live getters covered every need; the app persists in `onAfterPublish` only |
| `me.publish` / `publishBinary({ track, keyFrame })` + ack `receivers` / `onDemand` / `listen` / `setAttributes` / `leave` / `onLeave` (with `LeaveCause`) / `selfDelivery: false` | ✔ | chat + typing, media tracks paused by `onDemand` when unwatched, DM notices, per-field status & mute, channel switching, kick screen (`setMeta` superseded by `setAttributes` everywhere — finding 5) |
| `publish({ coalesce })` | ✖ | conflation is for lossy high-frequency streams (cursors); this app's chat and typing are both lossless/rate-limited already |
| `me.send` | ✖ | deliberately closed by guard — see finding 2 |
| `member.subscribeBinary(cb, { track })` / `onUpdate` / `onLeave` / `joinedAt` / `meta` / `identity` | ✔ | per-track media, live rosters, decoder lifecycle, tab dedupe |
| `member.subscribe` | ✖ | room-level `subscribe` + the verified `from` covered per-member needs |

## Future work

Message edit/delete, read-state persistence (server-side unread), attachments, role system
beyond owner/member, deafen, `@mention` highlighting, multiple guilds.
