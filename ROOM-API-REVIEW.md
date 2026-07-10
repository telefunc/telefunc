# Room API review — bloat audit, questioned decisions, LiveKit comparison

Findings requested in review of PR #436 (`Room` — multi-party rooms, implementing #250). Everything here was researched against:

- **livekit/client-sdk-js** `2.20.1`, cloned at commit `3da1eb3` (2026-07-08) — ~32,000 lines of client source. All `file:line` references below are into that tree.
- **Issue #250** — the original spec, used as the baseline for the bloat audit.
- **PR #436** at head `0eaaad60` — LOC and API-surface measurements taken directly from the branch.
- **Ably rewind** docs — prior art for the history-then-live question.

This document contains findings and recommendations only — it changes no code. Where it recommends changes to PR #436, those are listed in §7 for decision.

> **Disposition (executed).** Every recommendation here was acted on in PR #436, with one deliberate override: the direction was to ship *everything now*, with no phasing. So the items this doc scoped as "v1.1" or recommended deferring were built into #436 too — `setAttributes`, `coalesce`, single-call `tail` history, and, notably, **`onDemand`**. §2.1/§5.1 recommended keeping `receivers` and deferring event-driven demand; instead `onDemand` was built for real as the correct multi-node design sketched in §2.1 — control-lane want-gossip, no adapter-interface change — and `receivers` was kept alongside it. The `Room.guard`→getter fold (§2.3) was the one item explicitly excluded. This doc remains the point-in-time analysis; #436 is the executed result.

---

## TL;DR

1. **The bloat critique is partially confirmed.** The code itself is lean per feature and the tests are proportional. The bloat is *feature accretion*: the surface roughly doubled beyond issue #250's spec, and the growth came almost entirely from AI self-review cycles — 41 of 42 work items added; exactly 1 removed anything. Three features have no user request, no issue mandate, and no prior art in LiveKit: the **activity lane**, **member-selective text wants**, and **per-track upstream narrowing**. Recommended v1 cut list in §1.3.
2. **Two real architecture findings** (not just docs): the `receivers`-polling encoder-pause recipe is strictly worse than LiveKit's event-driven dynacast and should be replaced by a demand event (§2.1); and history-then-live **can** be a single telefunction call using machinery the codebase already has — the docs' "why not one call" argument is wrong (§2.2).
3. **Several questioned designs are validated by LiveKit doing the same or worse**: in-envelope verified senders (LiveKit hands you `undefined` instead), leave causes (LiveKit's `DisconnectReason`), DM ephemerality + database-for-durable (LiveKit data is even more ephemeral), the DM pre-listen hold (LiveKit silently drops). These need docs trims, not code changes.
4. **The docs are the biggest real problem.** The 610-line `/room` page explains implementation and justifies design ("why this is airtight", "nothing to race", stray `m` variables) instead of guiding. The gotcha-dense feel the review flagged is mostly *rationale prose*, not actual API gotchas. Rewrite plan in §6 targets ≤ 350 lines with zero internals in the guides.
5. **Honest scope caveat the docs currently hide**: telefunc binary tracks ride reliable, ordered transports (WS/SSE over TCP). Real multi-party video needs congestion control, unreliable delivery, simulcast, and jitter buffering — LiveKit exists because of exactly that stack. Telefunc's tracks are excellent for binary *data* substreams and small-scale/prototype AV on good networks; the docs should say so instead of leading with mic/camera/screen (§2.9).
6. **Verdict on "which is better"**: they solve different layers. In the overlap zone — data rooms: presence, chat, signals, moderation — telefunc's design wins on DX and infrastructure weight (zero extra servers, guards next to `getContext()`, returnable objects). For media, LiveKit wins outright and telefunc shouldn't pretend otherwise. Scored table in §4.

---

## 1. The bloat audit ("tokenmaxing" check)

**Question asked:** is the implementation bloated because of AI-agent only-ever-adding-code behavior?

### 1.1 How the surface grew

Issue #250 specified: rooms + presence + metadata + room-wide/per-member pub-sub + admin statics (`update`/`close`/`removeParticipant`) + `size` hint + `isolated` mode + KV adapter methods. Explicitly **not** in the issue: private messages, guards, identity, announce, leave causes, tracks, activity signal, snapshot/onChange, typed generics, `getOrCreate`, selective delivery machinery.

The delta came from successive review→implement cycles. The work-item history is the cleanest evidence:

- 42 tracked tasks; **41 added** code/features/docs, **1 removed** anything (a late DM-hold simplification).
- The big expansions (C1–C10, then tasks 27–38) were driven by *AI-generated reviews of the AI's own code* (the "problem-variability review" and "Discord-clone field test"), each review round proposing additions, each addition then needing tests, docs, and convergence handling — the classic accretion loop the "tokenmaxing" concern describes.

That loop produced high-quality additions — but nothing in the loop ever pushed *back* on scope. The one force that removed code was a human correction.

### 1.2 The numbers

PR #436: +7,982 / −52 across 47 files.

| Bucket | Lines |
|---|---|
| Room implementation (7 files) | 3,764 |
| Shared-infra changes (adapter KV, `messageLimit`, mux) | ~390 |
| Unit tests (94 room tests + limit/redis/CF specs) | ~2,330 |
| E2E tests | ~590 |
| Docs | ~660 |

Wire protocol: **20 message tags** (issue #250 sketched ~10). Browser-shipped room code: ~2,100 lines. For scale: LiveKit's client is ~32,000 lines (different problem — it carries a full WebRTC media stack; its E2EE dir alone is 5,205 lines).

Per-feature footprint (grep-hit signal across the 7 room files; rough proxy for entanglement, not exact LOC):

| Feature | Hits | In issue #250? | Prior art (LiveKit) |
|---|---|---|---|
| Binary tracks (+ per-track wants) | 168 | ✗ | ✓ tracks are core there |
| Private messages + inbox hold | 131 | ✗ | ✓ `destinationIdentities` |
| Identity | 91 | ✗ | ✓ identity is core there |
| Snapshot/onChange | 76 | ✗ | ~ (components-react solves it) |
| Lazy/streamed roster | 73 | ✗ (issue was eager `init`) | ✗ (LiveKit roster is eager) |
| LWW convergence (hybrid clock, read-back) | 69 | ~ (implied by "works distributed") | ~ (server-owned state) |
| Activity lane | 60 | ✗ | ✗ none |
| Guards | 43 | ✗ (issue: auth in telefunctions) | ~ (token grants) |
| Liveness (heartbeat/reap/TTL) | 39 | ✗ | ✓ (server-owned) |
| Member-selective text wants | 22 | ✗ | ✗ none |
| `keyFrame` bit | 15 | ✗ | ✓ (RTP-level) |

### 1.3 Feature-by-feature verdicts

**Keep — validated by prior art or load-bearing:**

- **Private messages** (LiveKit: `destinationIdentities` on every data publish — same lane, less ergonomic). Core chat need.
- **Identity** (LiveKit: `identity` is *the* primary participant key; per-connection `sid` is secondary — telefunc's participant-id/identity split is the same shape).
- **Guards** — telefunc-idiomatic replacement for LiveKit's token grants; colocates policy with `getContext()`. Needs the ergonomics fix in §2.3, not removal.
- **Leave causes** (LiveKit: `DisconnectReason` enum with a dozen values; telefunc's 4 causes + carried `reason` is a cleaner cut of the same need).
- **Announce / `Room.send`** (LiveKit server SDK publishes room-authored data the same way).
- **Liveness layers, LWW convergence, `messageLimit`** — correctness/security, not features.
- **snapshot/onChange** — small, and it's the entire React story.
- **Typed generics** — type-only, zero runtime.
- **Sugar** (`getOrCreate`, `Room.join` shorthand, `list({ prefix })`) — trivial.
- **DM pre-listen hold** — keep: LiveKit *silently drops* data with no listener attached (`Room.ts:2092` emits into the void) and buffers only while disconnected (`IncomingDataStreamManager.ts:32-47`). The hold prevents a real footgun at ~30 lines.

**Trim — keep the mechanism, cut the speculative half:**

- **Binary tracks**: keep named tracks + `keyFrame` (modest framing; LiveKit validates the concept). **Reconsider per-(member,track) *upstream* key narrowing** — the machinery that stops a track's upstream pull when the last local watcher leaves. It's the most entangled piece of the largest feature, built for a 500-camera scale nobody has measured, and it's why `track` shows 168 hits. Per-member upstream keys with client-side track filtering would keep the API identical and delete the hardest code. Judgement call; flagged, not mandated.

**Cut from v1 (re-add on demand):**

- **Activity lane** (`onActivity`, `sub-activity`, `activity` tag, per-node throttling): no user request, no issue mandate, no LiveKit equivalent, and the unread-badge use case is fully served by the database the docs already tell users to keep (count unread there). ~1 wire lane + throttle machinery + docs section. Cheapest meaningful surface reduction.
- **Member-selective text wants** (`sub-text`, relay filter, isolated-mode narrowing): built for "follow 2 members of a 10k-member room" — a workload that doesn't exist yet. Note LiveKit offers *no* receiver-side data selectivity at all. Room-level lazy text (the standard subscription gate) stays — that one is cheap and real. Re-add per-member wants when someone has the 10k room.

### 1.4 Verdict

Not tokenmaxing in the code-padding sense — functions are tight, there's no dead code, tests scale with features. But **yes, feature-maxing through self-review loops is visible and real**: roughly a third of the wire protocol and ~25–30% of the implementation serves features with no external pull. The cut list above removes ~2 wire tags + the activity/sub-text machinery immediately, and flags the per-track upstream narrowing as the big optional simplification inside tracks. Equally important: **stop the loop** — future review cycles on this API should carry an explicit "what can be removed" axis, and additions should require a named user/workload pull.

---

## 2. The questioned decisions, researched

### 2.1 `receivers`-polling for encoder pause — real finding, replace the pattern

**The docs recipe** (pause when a publish ack reports `receivers === 0`, then probe with a keyframe every 3s to detect returning viewers) is a **polling workaround for a missing event**, and the review was right to flag it.

**What LiveKit does (dynacast):** the SFU tracks per-layer subscriber demand and *pushes* `SubscribedQualityUpdate` to the publisher over the signaling socket (`SignalClient.ts:843` → `LocalParticipant.handleSubscribedQualityUpdate`, `LocalParticipant.ts:2025`); the SDK reacts by flipping `encodings[].active` on the RTP sender (`LocalVideoTrack.ts:711-743`) — the browser encoder genuinely stops producing paused layers. The publisher never polls and never publishes speculative probes. Keyframes for returning subscribers are requested at the transport level (SFU sends RTCP PLI; the SDK has *no* keyframe API at all) — so the probe loop in telefunc's docs has no analog in LiveKit at any layer.

**Why telefunc ended up polling:** `receivers` comes for free (in-memory count, Redis `PUBLISH` return). A *push* signal needs global demand transitions (0↔>0 per (member, track)) across nodes, which needs either adapter support or want-gossip on the control lane. That's real work — but the probe loop just moves the cost into every user's application code and onto the wire as garbage frames.

**Recommendation:**
- **Now (PR #436):** keep `receivers` on the ack (it's free and truthful); **delete the probe-loop recipe from the docs**. Document only: "pause when `receivers === 0`; resume-on-demand arrives in a future release."
- **v1.1:** `me.onDemand(track, (count) => ...)` — server-side, each node already knows its local want-set per (member, track) because stubs declare it; nodes announce local 0↔>0 transitions on the control lane (they already publish control events); the owner's node aggregates. The same event carries "keyframe requested" when a new subscriber attaches — which is exactly LiveKit's PLI flow, lifted to the data plane.

### 2.2 History-then-live in one call — real finding, the docs' "why not" is wrong

The docs currently require: subscribe → join (ack as fence) → fetch history → dedupe by ID, and assert a single call "always re-opens the gap." **That assertion is false given machinery the code already has.**

`RoomStubChannel` already buffers relayed publishes from the moment a room is serialized until the client's response stream attaches (the pre-peer buffer — byte-capped, FIFO-evicting, replay-on-reconnect). The only missing piece is an opt-in to start relaying text *at serialization time* instead of waiting for the client's subscription declaration; plus the client-side mirror of the DM inbox hold (hold text frames until the first `subscribe()`), which already exists for DMs.

**Prior art:** Ably `rewind` — attach with `rewind=N` and the server delivers history strictly before live messages, seam handled server-side, no client choreography. (Ably can do it fully server-side because Ably *stores* messages; telefunc keeps storage in the user's DB, so telefunc's version fences the *live tail* instead and lets the DB provide the past.)

**Proposed shape (naming open):**

```ts
// server — ONE telefunction
export async function onJoinChat(roomId: string) {
  const room = await Room.get(roomId, { tail: true }) // stub relays text from NOW, buffered until the client attaches
  const history = await db.messages.latest(roomId, 50) // read strictly after the tail started
  return { room, history }
}

// client — no ordering ritual
const { room, history } = await onJoinChat('lobby')
for (const m of history) show(m)
room.subscribe(show) // drains the held tail; the seen-Set dedupe handles the overlap
```

Correctness: the `onPublish` guard persists before a message is sequenced, so anything published before the tail started is in the DB read; anything after is in the tail; boundary messages are in both → overlap-only, same dedupe, **zero client ordering ritual and one round-trip**.

Honest caveats to design around: the tail buffer is byte-capped (a publish storm during the response round-trip can evict — eviction should surface as an explicit gap event so apps can refetch instead of silently missing); and it must stay opt-in so presence-only grants don't pay for text.

**Recommendation:** adopt for v1.1 (or pre-merge if the shape lands cleanly) and delete the docs' fence ritual + "why this is airtight" essay entirely. This single change removes the scariest section of the docs.

### 2.3 `Room.guard` — "instance" is real ambiguity; the guards are more consistent than they look

Three sub-questions were raised:

**"What does *instance* mean?"** The room object returned by that particular `Room.get()` call. A guard covers memberships granted *through that object* — including client-side `join()`s on the copy a telefunction returned. The concept is sound (policy rides the grant); the *word* is the problem, and so is the two-step dance (`get` then `guard`) with its "throws if called twice" edge.

**"Why does `onPublish` get `from` but `onJoin` doesn't?"** It does — the guards are uniform: the first argument is always the acting member (`onJoin(member)`, `onPublish(from, data)`, `onSend(from, to, data)`; `member` *is* the join's `from`). What created the confusion is the docs example, which ignores the `member` argument and reaches for closure state (`db.isBanned(user.id)`) — making the parameter look decorative.

**"Isn't `from` always the context user anyway?"** No, and this is load-bearing: guards run at *message time*, potentially hours after the telefunction returned — `getContext()` is gone; one instance can grant several memberships (multi-join through a returned room); and metadata/identity are live values at call time. `from` is the only correct source.

**Recommendation:** fold guards into the getter and delete the standalone attach + the "instance" prose:

```ts
const room = await Room.get(roomId, {
  onJoin: (member) => { if (banned.has(member.identity)) throw new Error('banned') },
  onPublish: (from, data) => { if (isSpam(data)) throw new Error('blocked') },
})
```

Same semantics, one step, nothing to explain, "called twice" impossible, and the docs example gets fixed to use `member.identity` so the uniformity is visible. (Breaking change is fine — Stream is beta.) `Room.getOrCreate`/`Room.join` take the same options.

### 2.4 Sender snapshot-vs-live duality — validated; LiveKit does strictly worse

The review asked whether always-carrying identity in the envelope (with a snapshot `Sender` when no live object exists) is the right architecture. LiveKit's answer to the same race: `DataReceived` hands the app `participant?: RemoteParticipant` — **`undefined`** whenever the sender isn't in the local map (`Room.ts:2045`), and the sender's identity string is *dropped* at that point rather than surfaced. No buffering, no reconciliation (`RTCEngine` data and signaling are separate transports with no cross-ordering, same as telefunc's lanes).

So the duality isn't an architectural mistake being papered over — it's the honest answer to a race every dual-transport system has, and telefunc's version is the strong one: `from` is never `undefined`, never spoofable, and never wrong. **Keep the architecture; cut the 40-line explanation in the docs to one Note** ("`from` is always present and verified; it's the live participant object when your view knows the sender, a plain `{ id, meta, identity }` otherwise").

### 2.5 `onActivity` vs `room.storage` — different questions; cut the first, defer the second

- `room.storage` (per-key shared state, LWW per key, fine-grained subscriptions — Liveblocks/PartyKit territory) **does not replace** `onActivity`: writing a storage key per publish recreates the hot-key problem the activity lane's throttling exists to avoid.
- But the badge use case doesn't justify a dedicated lane either (§1.3): unread state needs *counts per user*, which only the database (already holding messages for history) can answer; "when to show the dot" can come from the message stream you're subscribed to, or from the DB.
- **Recommendation:** cut `onActivity` from v1. Evaluate `room.storage` separately as the possible next primitive — it has real pull (game state, shared settings; the gap analysis is right that `meta` full-replace is config, not state), it subsumes several "nice-to-haves" (pinned messages, typing indicators as ephemeral keys), and the KV + LWW + per-member-record machinery it needs already exists internally. Until then, document the `publish`-based pattern (works today, fine at low frequency).

### 2.6 Switching-rooms recipe — docs bug

The promise-juggling (`joining?.then((old) => old.leave())`) is library-internals thinking leaked into user guidance. The 90% recipe is three lines: `await old.leave(); const me = await room.join({ name }); …` — and a one-line note that a send on a departed membership rejects explicitly. The interleaved-typing edge case belongs in a collapsible aside, if anywhere. Docs fix only; no API change needed.

### 2.7 DM durability note + pre-listen hold — validated, over-documented

"Is `Room.send` really the recommended mechanism [for durable DMs]?" — The note actually says the opposite (durable DMs belong in your database), which matches LiveKit exactly (data packets are fire-and-forget; lossy is even the *default* there — `DataPublishOptions.reliable?` absent ⇒ lossy channel with `maxRetransmits: 0` that drops on backpressure, `RTCEngine.ts:906,1660`). The division of labor is right; the paragraph explaining it is 4× too long. Trim to one sentence; move the hold's bound/semantics to the reference table where they already appear.

### 2.8 Leave causes — validated, over-documented

LiveKit ships a 17-value `DisconnectReason` enum (`CLIENT_INITIATED`, `PARTICIPANT_REMOVED`, `ROOM_DELETED`, `SERVER_SHUTDOWN`, `DUPLICATE_IDENTITY`, …) — the same taxonomy, coarser payload: its `LeaveRequest` carries **no string field**, so a kicked LiveKit client learns *that* it was removed, never *why*. Telefunc's 4 causes + `reason` riding the removal is a genuinely better cut of the same need. The feature stays; the "nothing to race, no second channel" justification prose goes.

### 2.9 Binary tracks positioned as video — the honesty gap

Telefunc tracks run over WS/SSE — TCP: reliable, ordered, head-of-line blocking, no bandwidth estimation, no selective retransmit, no simulcast/SVC, no jitter buffer. LiveKit is 32k lines *because* real multi-party video needs that stack (its data channels are capped at 64KB per message and even *those* ride SCTP with tunable reliability).

This doesn't make tracks wrong — named binary substreams with per-member selectivity are a strong data-plane feature (canvas deltas, audio snippets, file transfer, low-fps previews, ML frames). But the docs lead with mic/camera/screen and a 500-camera example, which sets users up for TCP-video disappointment at any real scale or on any lossy network.

**Recommendation:** reposition the docs section as "Binary streams & tracks" with an explicit scope note ("for production multi-party AV, use a WebRTC SFU — this lane is for binary data and small-scale/controlled-network AV"), keep the API as is.

---

## 3. The gap list, evaluated

From the review's gap analysis, with LiveKit evidence:

| Gap | Verdict | Evidence / reasoning |
|---|---|---|
| **E2EE** | Defer; document the pattern | LiveKit E2EE is 5,205 lines: a dedicated worker + key provider encrypting media frames via insertable streams, with *opt-in* data-packet encryption in the newer API (`DataCryptor`, gated by `options.encryption`) — and the app must bundle its own worker. For telefunc's data plane, apps can encrypt payloads before `publish`/`send` today (presence/meta stay visible — LiveKit likewise never encrypts signaling). Note the real tension: content guards (`onPublish` spam checks) are incompatible with E2EE payloads by construction — a future E2EE mode changes what guards can see. Reserve nothing now beyond that note. |
| **Ephemeral / high-frequency lane** (cursors, typing) | v1.1 candidate, small | LiveKit's answer is the *lossy* channel (default!) — drop-on-backpressure, unordered. Telefunc can't do unreliable transport over WS/SSE, but the equivalent semantics are **latest-wins coalescing**: `publish(data, { coalesce: key })` where relay hops keep only the newest payload per key under backpressure. Today: throttled `publish` is fine and should be documented (the docs already say cursors belong on `publish`, not `setMeta` — correct). |
| **`room.storage`** | Defer as its own primitive | See §2.5. Real pull, real scope — don't bolt onto v1. |
| **History helpers** | Yes — §2.2 *is* this | The `{ tail: true }` + DB pattern is the batteries-included answer that keeps storage yours. |
| **Typing indicators** | Pattern, not API | One doc snippet on the publish lane (throttle + expire client-side). LiveKit has no first-class support either. |
| **Device management** | N/A | LiveKit's device APIs manage cameras/mics — media concern. Telefunc's multi-tab identity already covers the identity side. |
| **Federation** | Out of scope | No prior art in any comparable library at this layer. The key schema is already versioned-by-shape (`__r` tags); a protocol version field can be added compatibly when ever needed. Don't reserve API now. |
| **Search** | No | Database concern by the same division of labor as history. |
| **Reactions / threading** | No | Message-schema patterns on `publish`; maybe one docs example. |
| **Moderation tooling** (audit log, reports) | Pattern, not API | `onPublish`/`onSend` guards are exactly the audit hook (they see every message with verified sender). One docs line. |
| **Metrics / observability** | Defer | `count`, `Room.list()` exist; anything deeper (per-room throughput, join rates) should wait for a real operator need. |

The gap analysis's own framing is correct: these are next layers, not design flaws — with the one exception that **the ephemeral-lane gap slightly weakens the "cursors on publish()" guidance** (no coalescing under backpressure yet), worth the small v1.1 item.

---

## 4. LiveKit client vs telefunc Room — scored comparison

Scores are /5 for how well each solves the problem *for its intended workload*, with the caveat that LiveKit is 8 years of production WebRTC and telefunc Room is a beta PR — maturity is scored as its own row rather than silently weighting every row.

| Problem | LiveKit | Telefunc Room | Better | Why |
|---|---|---|---|---|
| Presence & roster | 4 | 4 | tie | LK: eager, full roster always pushed, battle-tested, keyed by identity with version-guarded updates; TF: lazy + crash-safe (TTL/heartbeat/reap) with O(1) serialization — stronger design, unproven in production. Observers: TF holders-without-joining are first-class; LK needs the server-side `hidden` permission. |
| Identity | 5 | 4 | LK | Same model (app identity + per-connection id; LK's roster is literally keyed by identity). LK adds token-bound identity and an explicit duplicate policy — a second connection with the same identity *displaces* the first (`DisconnectReason.DUPLICATE_IDENTITY`); TF instead allows multi-tab membership and offers the sweep kick (`removeParticipant({ identity })`) — the better default for chat, worth documenting as a deliberate difference. |
| Verified sender on messages | 2 | 5 | **TF** | LK: `participant` may be `undefined` and the identity string is dropped on lookup miss (`Room.ts:2045`); TF: `from` always present, always verified, snapshot fallback. |
| Room-wide messaging | 3 | 4 | TF | LK: 64KB cap, no acks to sender, no server-side content hook; TF: acked, guarded, 16MiB, but no lossy mode. |
| Private messages | 3 | 4 | TF | LK `destinationIdentities`: fire-and-forget, drops without listener; TF: acked, guarded, pre-listen hold, explicit unknown-target rejection. |
| Large payloads / streams | 4 | 2 | LK | LK text/byte streams: 15KB chunks, topics, backpressure, progress (`OutgoingDataStreamManager.ts`); TF: single messages under `messageLimit` only. Lift candidate. |
| Participant↔participant RPC | 4 | 1 | LK | LK: `performRpc` with timeout + typed error codes; TF: none (server RPC is telefunctions, but member-to-member request/response must be hand-rolled on DMs). |
| Media (video/audio) | 5 | 1 | LK | Not TF's layer: no congestion control, no simulcast/SVC, no jitter handling over TCP transports. §2.9. |
| Publisher demand signal | 5 | 2 | LK | Dynacast: server-pushed layer demand, automatic pause; TF: ack-polling + probe recipe. §2.1 — the main lift. |
| Selective delivery (data) | 2 | 5 | **TF** | LK has *no* receiver-side data selectivity (topics filter client-side; everything reaches every subscriber); TF: per-member and per-(member,track) wants enforced source-side… which is also why §1.3 flags half of it as premature. |
| Moderation & authorization | 4 | 4 | tie | Different philosophies: LK capability tokens + live permission updates + server API; TF guards colocated with `getContext()`, content-inspecting, spoof-proof `from`. TF guards can gate on message *content* (LK can't — and its client doesn't even self-enforce a revoked `canPublish`, that's server-side); LK permissions can change mid-session (TF's can't). |
| Kick / leave semantics | 4 | 5 | TF | Same taxonomy; TF carries the human-readable `reason` with the removal, LK delivers only an enum. |
| History / replay | 1 | 2→4 | — | Neither stores messages (correct for both). Ably-style seam: LK has nothing; TF has the fence ritual today, and the §2.2 single-call design makes it a 4. |
| Reconnection | 5 | 3 | LK | LK: two-tier resume/restart with a `SyncState` message that reconciles subscriptions, published tracks, and reliable-channel cursors, replaying unacked data (`resendReliableMessagesForResume`), 10 attempts with jittered backoff; TF: rides channel reconnect + roster resync — sound, but young. |
| UI-framework integration | 4 | 4 | tie | LK: components-react ecosystem; TF: `useSyncExternalStore(room.onChange, room.snapshot)` is the whole adapter — elegant, no ecosystem. |
| Server-side story | 3 | 5 | **TF** | TF rooms are plain server objects next to your RPC layer, returnable to clients; LK needs its server SDK + SFU deployment for everything. |
| Infrastructure weight | 2 | 5 | **TF** | TF: your existing server, optional Redis/CF; LK: run an SFU (or pay for cloud). |
| E2EE | 4 | 1 | LK | §3. |
| Scale ceiling (their workload) | 5 | 3 | LK | SFU designed for large media rooms; TF's pub/sub fan-out is fine for data rooms, DO-contention addressed by `isolated`, but unproven past that. |
| Maturity | 5 | 1 | LK | Years in production vs. a beta PR. |

**Reading of the table:** telefunc Room should own the *data room* — presence, chat, DMs, moderation, signals, collaborative state — where it beats LiveKit on nearly every row that matters and by an order of magnitude on infrastructure weight. It should *not* contest media. The two sane positionings even compose: telefunc rooms for presence/chat/state + LiveKit (or any SFU) for AV in the same app.

---

## 5. What to lift from LiveKit

Concrete, in priority order (design lifts, not code copying — livekit-client is Apache-2.0 if code ever moves):

1. **Dynacast → demand events** (`SubscribedQualityUpdate` flow): replace `receivers` polling with server-pushed per-(member,track) demand transitions + keyframe-request. §2.1. *The* architectural lift.
2. **`attributes` alongside `metadata`** (`participant.setAttributes`, `LocalParticipant.ts:375-383`: updates *only the keys present*, deletes by empty string, change events deliver the delta): the cheap, proven answer to "`setMeta` full-replace is too heavy for one changed field" — and a stepping stone that might make `room.storage` unnecessary for the common cases. Small: per-member records already exist.
3. **Chunked streams** (`sendText`/`streamBytes`: 15KB chunks, topic handlers, backpressure, progress callbacks — `OutgoingDataStreamManager.ts:27`): the pattern for payloads above `messageLimit`, and the future carrier for file transfer. v1.1+.
4. **Lossy-lane semantics → `coalesce` publish option**: LiveKit's default channel drops under backpressure by design (`bufferStatusLowBehavior: 'drop'`, `RTCEngine.ts:1660`); telefunc's reliable-only lane needs latest-wins coalescing to serve the same cursor/ephemeral workloads. §3.
5. **RPC pattern** (`performRpc`: 15s default timeout, ack-vs-response phases, typed `RpcError` codes incl. `RECIPIENT_DISCONNECTED`): if member-to-member request/response ever gets API'd, this is the shape — it rides the existing DM lane naturally.
6. **Reliable-lane resume replay** (`reliableDataSequence` + `DataPacketBuffer` + `resendReliableMessagesForResume`, `RTCEngine.ts:1684`; on resume a `SyncState` message also reconciles subscription deltas and data-channel receive cursors): worth auditing telefunc's reconnect story against — does a publish acked-then-disconnected always survive? (The pre-peer buffer answers the server→client half; the client→server half deserves a test.)
7. **Event-taxonomy audit**: LiveKit's `RoomEvent`/`ParticipantEvent` list is a good completeness checklist — the one gap it surfaces that matters: telefunc rooms expose no reconnect lifecycle (LiveKit has `Reconnecting`/`SignalReconnecting`/`Reconnected` room events; channel-level state exists in telefunc but room-level exposure would let UIs gray out the member list during a blip).

Explicitly **not** worth lifting: eager roster push (telefunc's lazy roster is the better default for data rooms), token-grant authorization (guards + `getContext()` is the more idiomatic fit), the media stack (out of scope), per-quality layers (meaningless without simulcast).

---

## 6. Docs rewrite plan

The core critique — *"the docs read like a commit description; gotchas feel like documented mistakes"* — is confirmed, and it's mostly a prose genre problem: the page keeps *justifying the design to a reviewer* instead of *guiding a user*. 610 lines → target ≤ 350.

Principles (each maps to a flagged passage):

1. **No rationale-first prose.** Delete every "why this is airtight", "nothing to race, no second channel", "so cross-lane ordering is a non-event" passage. Where a constraint matters to the user, state the *rule*, one line, no proof. (The proofs live in code comments / this review.)
2. **No implementation vocabulary in guides.** "Lanes", "relay", "stub", "upstream", "KV", "hybrid clock", "single-flight" — all internal. Users see: messages, private messages, announcements, tracks.
3. **No undefined symbols.** `room.getParticipant(m.id) === m` → either a full two-line example or plain words ("a returned participant *is* the same object the room hands you — compare with `===`").
4. **Examples use their own arguments.** The guard example must use `member.identity`, not closure `user.id` (§2.3's confusion was manufactured by the example).
5. **Recipes must be the 90% case.** Switching rooms: 3 lines (§2.6). History: becomes trivial if §2.2 lands; until then the ordering rule gets *stated*, not proven.
6. **Scope honesty over aspiration.** Video section → "Binary streams & tracks" with the §2.9 note.

Per-section actions:

| Section (current) | Action |
|---|---|
| Intro + model + quick start | Keep; already good. Trim the "two properties hold everywhere" block to two plain sentences. |
| Show who's here | Keep (best section on the page). |
| Chat | Keep; fold the lurker-cost note into one line. |
| Identity | Keep; trim the "trust lives where…" theory to one sentence. |
| Private messages | Keep example; compress the durability/hold paragraph to one sentence each (§2.7). |
| Moderate | Rewrite around guard-options-on-get (§2.3); delete "instance" prose + the helper-per-room-type advice (obsolete once guards ride the getter); kick example stays, its justification goes. |
| System notices | Keep (short, task-shaped). |
| Video & tracks | Rename + reposition (§2.9); delete the 500-camera fan-out essay and the probe-loop recipe (§2.1); keep the two code samples. |
| Load history | Replace with the §2.2 single-call pattern when it lands; until then: state the order + dedupe in 6 lines, delete the "airtight" proof and the "why not one call" paragraph (it's wrong). |
| Unread badges | Delete section (feature cut, §1.3) — or if `onActivity` survives, 4 lines max. |
| Switching rooms | Replace with the 3-line version. |
| Capacity | Keep. |
| Reference tables | Keep tables; delete the paragraph restating each table row in prose (the "I'm not even reading that" block — the tables already say it). |
| Production | Collapse to ~10 lines: "works multi-node with Redis/Cloudflare; presence survives crashes (30s heartbeat + TTL); per-lane costs in one table." Everything else (convergence discipline, three-layer liveness narrative, suppression) → "How it works" or code comments. |
| How it works | Keep as the single place internals are allowed; absorb Production's mechanics. |

---

## 7. Recommended action plan

For decision — none of this is applied yet:

**Before merging #436** (shrinks surface + fixes the review's docs critique):
1. Docs rewrite per §6 (no API dependency; the largest single improvement).
2. Cut `onActivity` + `sub-text` member-selective wants (§1.3) — 2 wire tags, the docs sections, and the maintenance surface.
3. Guards ride the getter (§2.3) — small breaking change, beta-appropriate, deletes a concept.
4. Delete the keyframe-probe recipe; keep `receivers` (§2.1).
5. Reposition tracks as binary streams with the scope note (§2.9). Optionally: drop per-track *upstream* narrowing to per-member (§1.3, judgement call).

**v1.1 (with named demand):**
6. Single-call history: `Room.get(id, { tail: true })` + client hold-until-first-subscribe (§2.2).
7. Demand events `me.onDemand(track, cb)` replacing polling guidance (§2.1/§5.1).
8. `attributes`-style partial metadata updates (§5.2).
9. `coalesce` publish option for ephemeral signals (§5.4).

**Explicitly deferred with a one-line docs note:** E2EE (pattern: encrypt payloads; note the guard tension), `room.storage`, chunked streams, member RPC, federation.

**Process:** future self-review cycles on this API run with a removal axis ("what would we cut?") and additions require a named workload — the accretion loop in §1.1 shouldn't repeat.
