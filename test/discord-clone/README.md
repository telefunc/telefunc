# Discord Clone — a `Room` stress test

A full Discord-style app built on [telefunc](https://telefunc.com)'s new `Room` primitive
([#436](https://github.com/telefunc/telefunc/pull/436)), written to find out where real app code
fights the API. The app is real — accounts, SQLite persistence, offline-capable DMs, voice
channels with camera and screen share — and every place the Room API forced complexity into app
code is called out inline and collected in [Findings](#findings-room-pr-436-pain-points).

**Features**

- Accounts (register/login, scrypt + httpOnly session cookie); the first account owns the server
- Text channels: live messages, history with pagination, topics, create/delete, unread badges
- Typing indicators (ephemeral), banned-word moderation, server-wide announcement banners
- Direct messages: conversation list, live delivery, works with **offline** recipients, Do-Not-Disturb
- Voice channels: live occupancy in the sidebar, microphone (Opus), camera (VP8), **screen share**,
  mute — all over Room per-member binary lanes via WebCodecs, no WebRTC
- Presence: online/idle/dnd status, multi-tab-aware member list, kick (with the reason delivered
  to the kicked user), crash-safe cleanup
- RoomBot: a server-side member (`!help`, `!ping`, `!members`, `!roll`, DM replies)

**Stack**: Vike + vike-react · telefunc (remote functions + rooms) · SQLite via better-sqlite3 +
Drizzle · Zustand · Tailwind v4 · Hono · WebCodecs.

## Running it

```bash
pnpm install && pnpm build   # repo root (workspace `telefunc` is linked)
cd test/discord-clone
pnpm dev                     # http://localhost:3000
```

Register a user (the first one becomes the server owner), then open a second browser profile —
or an incognito window — and register another. The database lives in `.data/` (`DISCORD_CLONE_DB`
overrides; the e2e suite uses `:memory:`).

## Architecture

The **database is the durable truth** (users, sessions, channels, messages, DMs) and **rooms are
the live layer** (presence, delivery, events). Rooms are recreated from DB rows at boot; nothing
irreplaceable lives in them.

| Discord concept | How it's built |
|---|---|
| Guild + online members | One guild room; members carry `{ userId, name, color, status, admin }` metadata |
| Text channel | One room per channel. **Subscribing** = receiving messages (all clients, for unread badges); **membership** = having the channel open (grants publish) |
| Message history | `messages` table, written by the channel's `onPublish` guard, replayed with the docs' subscribe → join → fetch recipe, paginated with a `beforeAt` cursor |
| Typing indicator | Ephemeral `publish({ kind: 'typing' })` — same lane as chat, skipped by the guard |
| Moderation | `onPublish` guard throws → rejection travels the ack back to the sender's promise |
| DMs | **DB-first, server-delivered**: telefunction writes the row, then `Room.send()`s it to every live participant of both users. Works offline; DND enforced server-side. The member-to-member `me.send()` lane is deliberately closed by a guard (see finding 2) |
| Channel create/topic/delete | `Room.create` / `Room.update` (read-modify-write) / `Room.close`; the guild's announce lane doubles as the directory feed (finding 4) |
| Kick | Notice via `Room.send` → `Room.removeParticipant` for **every** participant of that user in **every** room (finding 1) → `Room.announce` |
| Voice/video | One room per voice channel (`size`, `isolated`). Mic/camera/screen multiplexed on the member's one binary lane with a `[kind][flags]` prefix (finding 6); mute/camera/screen state rides `setMeta` |
| Bot | A server-side member driven by the same Room API |

Module map: `database/` (Drizzle schema + SQLite bootstrap) · `server/` (auth, sessions, room
bootstrap, guards, bot) · `telefunc/` (the API: enter, channels, DMs, admin) · `app/` (Zustand
store = the Room→React adapter, `call.ts` = the WebCodecs media engine, `ui/`) · `shared/`
(types crossing the wire).

## Testing

`.test-dev.test.ts` / `.test-preview.test.ts` drive **two real users** (separate browser
contexts) through everything above — including camera frames and screen share decoded across the
wire, using Chromium's fake media devices (flags in the repo-root `test-e2e.config.mjs`).

```bash
pnpm exec test-e2e test/discord-clone/.test-dev.test.ts   # repo root
```

## Findings: Room PR #436 pain points

Ordered by how much app complexity each one caused. "Receipt" = where this app pays for it.

### 1. Cross-room identity is entirely app glue

Participant IDs are per-room **and** per-connection: the same human is a different participant in
the guild, in every channel, in voice — and gets new IDs on every reload. Any concept of "the
same user" across rooms/connections is the app's problem.

- Receipts: every membership stamps `userId` into metadata; the member list dedupes participants
  by it (`app/store.ts` `wireGuild`); kicking sweeps every room comparing `meta.userId`
  (`telefunc/admin.telefunc.ts`); the call view needs `myParticipantId` because "which tile is
  me" can't use identity (two tabs = two tiles). An earlier iteration of this app shipped the
  natural bug: passing a channel-room participant ID to a guild-room API → `Participant not found`.
- Suggestion: opt-in first-class identity — `join(meta, { identity })` surfaced as
  `from.identity`, roster grouping helpers, `Room.removeParticipant` accepting an identity.
  Short of that, a prominent docs recipe ("stamp your user ID into meta; correlate by it").

### 2. The private lane can't carry real DMs

`me.send()` addresses a *participant*, which exists only while its connection lives — offline
users are unreachable, and nothing is replayed. Discord-style DMs (offline delivery, history)
had to be rebuilt DB-first with `Room.send()` fan-out to each live participant
(`telefunc/dms.telefunc.ts`); after that, the only job left for the `onSend` guard was to close
the lane so clients can't bypass the server path (`server/guards.ts`).

There's also a delivery race for *reactive* sends: a DM fired in response to a join event can
arrive before the joiner's `listen()` is attached (or even before the participant serializes) —
which is why the bot greets in #general instead of by DM (`server/bot.ts`).

- Suggestion: docs should position the lane as ephemeral peer-signaling (game moves, WebRTC
  negotiation) rather than messaging; consider buffering inbox deliveries until the first
  `listen()` attaches.

### 3. Guards are per-instance — policy holes are one `Room.get()` away

A guard covers memberships granted through *that instance*, so policy + persistence must be
attached to **every** instance that hands out memberships. This app funnels all fetches through
`getGuardedGuild()`/`getGuardedChannel()` (`server/guards.ts`), but that's convention — any
direct `Room.get()` silently bypasses moderation *and* message persistence. `Room.guard()`
throwing on a second call also means helpers must own guarding exclusively.

- Suggestion: room-scoped guards (`Room.guard(roomId, ...)`), or guards as an option of
  `Room.get()`/`Room.create()` so the fetch and the policy can't be separated.

### 4. No directory events, and `Room.list()` returns snapshots

Nothing tells a client (or the bot) that a room appeared. The guild's announce lane doubles as a
hand-rolled directory feed (`channel-created` in `telefunc/channels.telefunc.ts`, consumed in
`app/store.ts` and `server/bot.ts`). Deletion is fine — `onClose` *is* the signal. And handing
clients N live rooms costs `Room.list()` + N × `Room.get()` (mitigated: serializing a room is
O(1)). Notably, once an app has its own channels table, `Room.list()` has no job left — this app
never calls it.

- Suggestion: `Room.list({ prefix })` and/or opt-in created-events; otherwise bless the
  meta-room directory pattern in the docs.

### 5. Every state write is a full replace

`setMeta` and `Room.update` replace rather than patch. Every toggle is a spread —
`setMeta({ ...meta, muted })` (`app/call.ts`, three times) — and `Room.update` must re-supply
`size` or a finite cap silently resets to `Infinity` (`telefunc/channels.telefunc.ts`).
Concurrent updates to *different* fields clobber each other (LWW is per-record, not per-field).

- Suggestion: merge variants (`setMeta(patch, { merge: true })`), or at least loud docs.

### 6. One binary lane per member → hand-rolled multiplexing

Mic, camera, and screen share are three concurrent streams, but a member has one binary lane —
so the app invents `[stream kind][flags]` framing, demuxes per frame, tracks keyframes, and
resets decoders via metadata signals (`app/call.ts`). It works well (per-publisher lanes and
subscriber-selective delivery are exactly right), but every media app will rebuild this envelope.

- Suggestion: named per-member binary substreams (`member.subscribeBinary('camera', cb)`,
  `me.publishBinary(data, { stream: 'camera', keyframe })`) — or document the tag-byte pattern.

### 7. Expected-rejection ergonomics differ by lane

A guard `throw new Error('nope')` reaches the sender's promise verbatim through the publish ack
— great. The same throw in a telefunction is masked ("Internal Server Error") unless you use
`Abort`, whose client-side `Error.message` is generic (the text hides in `err.abortValue`, see
`errorMessage()` in `app/store.ts`). And every guard-rejected publish is *also* logged
client-side as `[telefunc:channel-error]` even when the app catches the rejection — expected
control flow reads like a bug in the console (and needs `tolerateError` in e2e).

- Suggestion: skip the automatic `console.error` on publish-ack rejections (the caller owns the
  promise), and align guard/Abort message surfacing.

### 8. The channel-switch publish window

"Viewing = membership" means switching channels is a leave + join round-trip, and a message sent
during that window has no membership to publish through. Our first version dropped fast-typed
messages silently; the fix (chained switches + senders awaiting the in-flight join, see
`openChannel`/`sendMessage` in `app/store.ts`) is subtle enough to deserve a docs recipe.

### 9. Rooms are mutable emitters; UI frameworks want snapshots

`member.meta` is replaced in place, `getParticipants()` is async, and events don't carry the new
roster — so every React app writes the same adapter: `watchRoster()` + snapshot projection +
a store (~150 lines here, `app/store.ts`). The docs' "Show who's here" example understates what
a real UI needs (per-member `onUpdate` hooks, dedupe, immutability).

- Suggestion: an official `useRoomRoster(room)` / `useRoomState(room)` hook package or recipe.

### 10. Unread badges cost full subscription

Discord-style per-channel unread counts require subscribing to every channel's text lane — you
pay full message traffic for channels nobody is reading (`wireChannel` in `app/store.ts`). Same
cost class as Discord's gateway, and lazy lanes make it *possible* to opt out — but there's no
cheap middle ground.

- Suggestion: an opt-in lightweight activity signal on the control lane (per-room message
  counter), so badge-only consumers don't consume bodies.

### 11. No server-side join gate

`size` is a hint and there is no join guard, so capacity (or bans) can't be enforced for
client-side joins on a room the client already holds — this app's voice capacity check is
client-side only (`app/call.ts`). Enforced joins require routing through a telefunction, which
conflicts with handing out long-lived room objects.

- Suggestion: an optional `onJoin` guard next to `onPublish`/`onSend`.

### 12. Cross-lane ordering leaks into UX

The kick notice (private lane) races the removal event (control lane); the client waits 500ms
before concluding "disconnected" rather than "kicked" (`wireGuild` in `app/store.ts`).

- Suggestion: let removal carry a reason — `Room.removeParticipant(id, pid, { reason })` —
  surfaced on the leave event itself.

### 13. Smaller DX notes

- No `Room.getOrCreate()` — every boot path is `create` + swallow-the-exists-error
  (`server/rooms.ts`).
- Long-lived server-side state (the bot, the world latch, the SQLite handle) needs `globalThis`
  latches to survive dev-server module graphs and reloads — and Vite's *two* SSR module graphs
  will happily give you two `:memory:` databases (`database/db.ts`). After a bot-code edit, the
  old closures keep running until a server restart.
- Metadata is `Record<string, unknown>` — every consumer casts (`asMemberMeta`); `Room` could
  take meta type parameters. The `Sender` type isn't exported.
- The `onPublish` guard has no timestamp/seq: history rows get `Date.now()` at persist time while
  live renders use `info.timestamp` — two clocks for one message (harmless here, but the guard
  seeing the publish info would align them).
- Presence-death defaults are tuned for resilience, not chat UX: a closed tab holds its seat for
  `reconnectTimeout` (60s default) — this app lowers it to 10s (`+server.ts`).

## Room API coverage

| API | Used | Where / why not |
|---|---|---|
| `Room.create` / `Room.get` / `Room.guard` / `Room.update` / `Room.close` | ✔ | `server/rooms.ts`, `server/guards.ts`, `telefunc/channels.telefunc.ts` |
| `Room.removeParticipant` / `Room.announce` / `Room.send` | ✔ | kick sweep, banners + directory feed, DM delivery + kick notices |
| `Room.join` (static) | ✖ | every real membership needs identity stamping through a *guarded* instance — the shorthand skips both |
| `Room.list` | ✖ | the app's channels table already knows the rooms (finding 4) |
| `room.join` / `getParticipants` / `subscribe` / `onJoin` / `onLeave` / `onUpdate` / `onAnnounce` / `onClose` / `count` / `size` / `isFull` / `meta` | ✔ | throughout `app/store.ts`, `server/bot.ts`, `telefunc/*` |
| `room.getParticipant` / `room.subscribeBinary` / `onEmpty` / `onFull` / `isEmpty` / `isClosed` | ✖ | roster filtering + member-level binary + live getters covered every need |
| `me.publish` / `publishBinary` / `listen` / `setMeta` / `leave` / `onLeave` / `selfDelivery: false` | ✔ | chat + typing, media, DM/kick notices, status & mute, channel switching, kick screen |
| `me.send` | ✖ | deliberately closed by guard — see finding 2 |
| `member.subscribeBinary` / `onUpdate` / `onLeave` / `joinedAt` / `meta` | ✔ | per-publisher media, live rosters, decoder lifecycle |
| `member.subscribe` | ✖ | room-level `subscribe` + the verified `from` covered per-member needs |

## Future work

Message edit/delete, read-state persistence (server-side unread), attachments, role system
beyond owner/member, deafen, `@mention` highlighting, multiple guilds.
