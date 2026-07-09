# Problem-variability review — PR #436 (`Room`)

**Under review:** `feat: add Room — multi-party rooms with presence and member management`
(branch `claude/telefunc-issue-250-la26xh`, ~4,600 lines added over `main`).

## What this measures

This is **not** a bug review. It rates each high-level problem the PR solves on a single
axis — *how much design freedom the problem has, and how confidently the chosen solution can
be called optimal*:

| Score | Meaning |
|---|---|
| **10** | The solution is essentially **forced / canonical**. There is one obviously-right way to solve it, so the code can't be meaningfully improved. **Low problem variability.** |
| **0** | It is **highly unclear whether a better solution exists**. The design space is wide, several defensible approaches compete, and the choice is a genuine bet. **High problem variability.** |

A high score is *not* praise for code quality and a low score is *not* criticism — a 3 can be
excellent code for an intrinsically open problem. The score answers: *"if a smart person
re-derived this from scratch, how likely are they to land somewhere very different?"*

The useful reading is the inverse: **the lowest-scoring problems are where the real design
bets live** — the parts most worth scrutinising, stress-testing, and expecting to revisit. The
highest-scoring ones are settled and can be trusted.

---

## Summary (sorted by score)

| # | Problem | Score |
|---|---|:---:|
| 11 | Binary framing (fixed 16-byte member-ID prefix) | **7** |
| 3 | Shared event-sourced state machine (`RoomState`, server **and** client) | **7** |
| 6 | Room-wide messaging (room-level + per-member off one stream) | **7** |
| 8 | Room-authored messages (`announce` / `Room.send`, unforgeable sender) | **7** |
| 13 | Wire serialization seam (fresh stub per response) | **7** |
| 14 | Client↔server stub request/ack protocol | **7** |
| 15 | Impersonation prevention (validate against per-stub membership) | **7** |
| 18 | Pluggable KV contract + Redis + Cloudflare backends | **7** |
| 1 | Public API & dual-environment type surface | **6** |
| 2 | Room lifecycle CRUD (`create`/`get`/`list`/`update`/`close`) | **6** |
| 7 | Private messaging with verified senders (per-member inbox keys) | **6** |
| 16 | Lazy subscription reconciliation (`_syncSubs`) | **6** |
| 19 | Relay backpressure (byte-capped FIFO buffering) | **6** |
| 5 | Crash-safe presence (heartbeat + TTL reaping) | **4** |
| 9 | Authorization model (single send-guard on the room grant) | **4** |
| 10 | Member-selective binary delivery | **4** |
| 17 | `isolated` mode (per-member data keys) | **4** |
| 4 | Multi-node eventual consistency (local-apply + echo + idempotent dedup) | **3** |
| 12 | `selfDelivery` echo suppression (cross-wire registry) | **3** |

**Mean ≈ 5.6.** A cluster of 6–7s (settled, well-constrained execution) sitting on top of six
genuine design bets (3–4) concentrated in the distributed-systems core: consistency, crash
presence, the authz surface, binary selectivity, isolated mode, and the self-delivery link.

---

## A. API & type surface

### 1. Public API & dual-environment type design — **6**
*One `Room` / `LocalParticipant` / `RemoteParticipant` type, identical on server and client and
returnable from a telefunction as-is; `Room.*` statics as the entry point; admin operations kept
off the instance so a room handed to a client carries no privileged methods.*

Genuinely open problem, but heavily constrained by **codebase consistency**: mirroring the
existing `Broadcast.*` statics and the `Date`-style value+type merge is close to forced once
those precedents exist, and keeping admin on the statics is the clean way to make the instance
safe to hand out. Still, defensible alternatives were left on the table — the issue's original
callable `room()` shorthand, a `.client` variant type, capability-guarded admin methods on the
instance. Well-reasoned, not inevitable.

### 2. Room lifecycle CRUD — **6**
*`create` (throws if exists) / `get` (throws if missing) / `list` / `update` / `close`, backed by
a config record in KV.*

The create/get error semantics are canonical. Two things keep it off the top: `update()` is a
**full replace** (omitted options reset to defaults) rather than a merge — a real, reversible
UX choice — and `list()` is an `O(rooms × members)` sweep (a config read *and* a member scan per
room, `server.ts:140`). Fine at the scale the docs scope it to, but not the only or obviously-best
shape.

### 13. Wire serialization seam — **7**
*A `ServerRoom` is deliberately **not** a channel; serializing one attaches a fresh
`RoomStubChannel` per response (`response/room.ts`), so the same instance can be returned from any
number of telefunctions while `room.id` stays the room ID (wire channel IDs must be globally
unique).*

Composition-over-inheritance cleanly resolves the "channel id must be unique but room id is the
room id" tension, and it drops into the existing replacer/reviver architecture with near-zero
disruption (one new `protected _listen()` on `ServerChannel`). The surrounding serializer
framework constrains the space tightly; this is about as forced as a good fit gets.

### 14. Client↔server stub request/ack protocol — **7**
*Tagged-union requests (`req-join`/`req-leave`/`req-set-meta`/`req-dm`/`sub-binary`) with typed
acks, plus forward-compatible "ignore unknown `__r`" handling.*

Standard RPC-over-channel once the channel primitive exists. The tagged-union + ignore-unknown
shape is the idiomatic, low-risk choice; little would change on a re-derivation.

---

## B. Presence & distributed state

### 3. Shared event-sourced state machine — **7**
*`RoomState` (in `shared.ts`) is the single local view of a room — membership, metadata, every
user callback — and **the same class runs on server and client**; only the event *source* differs
(adapter subscription vs relayed wire frames). Application is idempotent so a snapshot seed and a
concurrent event stream can overlap without double-firing.*

Sharing one machine to *guarantee* identical semantics is close to the obviously-right move (DRY
+ correctness), and event-sourced presence with idempotent apply is a well-trodden pattern. It
loses a couple of points only because the broader mechanism it plugs into (#4) is itself open.

### 4. Multi-node eventual consistency — **3**  ⚠ design bet
*The node causing a change applies it locally, then publishes it; every other node — and the
origin's own echo — re-applies it. `join`/`leave`/`closed` are idempotent by nature; `p-meta` and
`update` carry an 8-char random `eid` (`makeEid`, `shared.ts:206`) so the origin recognises and
absorbs its own echo. `membershipVersion` guards async KV reconciles from clobbering a fresher
event.*

This is the widest design space in the PR and the correctness rests on subtle ordering arguments
(e.g. `applyData`'s "unknown sender is noise, not a race" reasoning). Many alternative
foundations exist — server-authoritative sequencing, version vectors, monotonic per-key seqs,
CRDTs — and it is genuinely unclear the local-apply-plus-random-eid-dedup scheme is the best of
them. The `eid` in particular is an ad-hoc dedup token (random, non-unique-by-design). It works;
whether it's optimal is exactly the question this axis asks, and the answer is "unclear."

### 5. Crash-safe presence (heartbeat + TTL reaping) — **4**  ⚠ design bet
*Graceful exits propagate instantly as events. Against hard crashes, the owning node refreshes a
`seenAt` timestamp on each of its members every 30 s (`ROOM_HEARTBEAT_INTERVAL_MS`); records
older than 2 min (`ROOM_MEMBER_TTL_MS`) are reaped, and the leave announced, whenever
`readMembers` runs.*

A classic liveness pattern, but full of judgement calls with real alternatives. Reaping happens
**on reads and heartbeats** — so a room with no live owner and no reader can retain ghost members
until something touches it. The timing constants are hand-sized for eventually-consistent KV. And
polling generic KV is only one option: Redis-native key TTL / ephemeral presence keys, or
connection-lease models, would move the same problem somewhere quite different. Solid, clearly
not the only defensible point.

### 16. Lazy subscription reconciliation (`_syncSubs`) — **6**
*After every change that could matter (listeners, members, stubs, close), recompute the full set
of pub/sub keys this instance needs and diff it against the live subscriptions
(`server.ts:629`).*

Declarative "compute desired, reconcile to it" is a robustly-correct way to manage subscription
churn — far less bug-prone than scattered imperative subscribe/unsubscribe, and a re-derivation
that cared about correctness would likely land here too. Held below the top only by the full
recompute-on-every-change granularity (a deliberate simplicity/efficiency trade, not a forced
one).

---

## C. Messaging

### 6. Room-wide messaging — **7**
*A member's `publish()` reaches both room-level subscribers (with a `from` identity) and that
member's per-member subscribers, off the same stream.*

Canonical pub/sub with a clean room-level/per-member split from one source. Reasonably forced by
the model.

### 7. Private messaging with verified senders — **6**
*`send()` delivers to exactly one participant over that participant's **own inbox pub/sub key**
(`roomDmKey`), which only the target's owning node subscribes to — privacy by construction, never
touching the room stream. `from` is the verified sender: the live `RemoteParticipant` when the
holder observes the room, else an `{id, meta}` snapshot the sender's node stamped into the
envelope.*

Transport-level privacy is a strong choice — a DM physically never reaches another connection.
But real alternatives exist (route DMs on the room key with server-side filtering; a single
addressed DM key), each trading the per-member inbox subscription cost against relay complexity.
The verified-sender live-vs-snapshot upgrade path is intricate. Good design, not inevitable.

### 8. Room-authored messages — **7**
*The room itself can speak: `Room.announce` (to everyone, arrives only on `onAnnounce()`, **never**
on `subscribe()`) and `Room.send` (privately, arriving on `listen()` with `from: null`). The
`null`/empty-`from` sender is unforgeable — clients' messages are always validated as
participant-authored.*

Once you decide the room can author messages, the design is nearly forced: a distinct envelope
tag, and the strict invariant that `subscribe()` streams stay participant-authored. The
empty-string-means-server-authored wire encoding is a small wart but unambiguous.

### 11. Binary framing — **7**
*Binary relay frames are `[16-byte member UUID][payload]` — a fixed-size prefix, so no length
field is needed and any node can unframe statelessly (`frameWithMemberId`).*

About as constrained as it gets: member IDs are UUIDs (16 bytes), a fixed prefix is the minimal
self-describing framing, and statelessness means no cross-node index table to synchronise. The
one reason it isn't an 8–9: a compact per-room integer index would save 14 bytes/frame on
high-rate video — a real bandwidth argument, traded away for simplicity and statelessness.

### 12. `selfDelivery` echo suppression — **3**  ⚠ design bet
*`join(meta, { selfDelivery: false })` stops your own published frames coming back. Because a
telefunction typically returns `{ room, participant }` as **two independent wire values**, the
participant and its sibling `ClientRoom` are separate revived objects; they're linked only by a
**module-global registry** (`globalObject.suppressed`, a `roomId → Set<memberId>` map,
`client.ts:397`) so the room knows to drop that member's echo.*

The feature is simple; the cross-wire link is the least-settled construct in the PR. Global
mutable state keyed by room ID is a smell, and clear alternatives exist — carry suppression in
the room snapshot, or have the participant resolve its sibling room through shared context rather
than a side registry. Works, but a re-derivation could easily avoid the global map entirely.

---

## D. Security & authorization

### 9. Authorization model — **4**  ⚠ design bet
*"One guard concept — the room grant": `Room.get(id, { onSend })` installs a guard that runs
before every private `send()` from a membership granted through that instance (server- or
client-side joins alike), closing over the telefunction's `getContext()`. Admin ops live only on
the statics; room IDs are documented as capabilities.*

The description notes this survived "an over-flexibility audit that collapsed guards to the single
room-grant concept" — the collapse is a good instinct, but it lands at one point in a wide space
and the trade-offs are visible: **only DMs are guarded**, room-wide `publish()` is not
(`_publishData` runs no guard); there is no join guard or per-message content guard; and
authorization of *who may enter* is pushed entirely onto app code via capability room IDs. All
reasonable, none obviously optimal — authz surface area is intrinsically high-variability.

### 15. Impersonation prevention — **7**
*Every client publish and DM is validated against the members that client actually joined through
**that specific stub** (`_assertStubMember`, `server.ts:618`); a client cannot act as a member it
didn't join.*

Given per-connection stubs as the trust boundary, "validate against this stub's membership" is the
forced-correct invariant. A re-derivation that took security seriously would land in the same
place.

---

## E. Delivery optimisation

### 10. Member-selective binary delivery — **4**  ⚠ design bet
*Clients declare which members' binary streams they want (`sub-binary`, full-replace); the server
relays only those, and in `isolated` mode doesn't even subscribe **upstream** to unwanted members.
Declarations go synchronously (like `subscribe()`) so same-connection FIFO guarantees a publish
right after subscribing gets its own frame back. Text is never wire-filtered (it carries control
events and is cheap).*

Sophisticated and well-motivated by bandwidth, but the design space is wide and the complexity
cost is real. The simplest alternative — deliver all binary, filter on the client — is far less
code and defensible for many workloads; separate per-member channels are another shape entirely.
Choosing wire-level selectivity is a deliberate bet on high-fan-out media, not a forced move.

### 17. `isolated` mode — **4**  ⚠ design bet
*An opt-in room mode giving each member their own upstream data key, removing publish contention
on platforms that map each key to its own coordinator (Cloudflare Durable Objects). Fixed at
creation; invisible to clients.*

A genuine, platform-specific optimisation — but its existence is a judgement call. It threads a
branch through publishing, subscription sync, and framing, and whether a per-member-key mode
belongs in the core (vs being handled inside the Cloudflare adapter, or not at all) is exactly the
kind of thing reasonable designers disagree on. Pragmatic escape hatch, wide surrounding space.

### 19. Relay backpressure — **6**
*Server→client relay rides the existing channel pre-peer buffers and replay buffers, which are
**byte-capped with FIFO eviction** (binary can never evict control text); the Cloudflare cold-path
adds its own hard-capped `CloudflareBucketPublishBuffer`.*

Byte-capped FIFO with the invariant that control events can't be evicted is a sound, fairly
standard answer to "bound server memory for slow clients." Most of it is **inherited** from the
channel layer rather than newly designed here, which both de-risks it and means the room code
didn't really get to choose — hence mid-scale rather than low.

---

## F. Backends

### 18. Pluggable KV contract + Redis + Cloudflare — **7**
*`Room` needs cross-node state, so the broadcast adapter/transport gains four **optional** KV
methods — `get`/`set`/`delete`/`keys(prefix)` — with a clear usage error when a transport lacks
them. Redis backs them with `GET`/`SET`/`DEL` plus cluster-aware `SCAN` (walks every master,
glob-escaped MATCH); Cloudflare backs them with Workers KV under a `tfkv:` prefix (cursor
pagination).*

The four-method contract is minimal and canonical, and the two implementations are largely forced
by their platforms' primitives (SCAN cursors, KV list cursors, cluster hash-slot rules). The one
soft spot — `keys(prefix)` is an inherent full-enumeration — is a property of generic KV, not of
this design. Little would change on a re-derivation.

---

## Overall reading

Execution is consistent and well-constrained across most of the surface — the 6–7 band is broad,
reflecting a feature that leans hard on existing telefunc precedents (Broadcast statics, the
serializer seam, the channel buffers) and inherits their settledness.

The signal to act on is the **six design bets at 3–4**, all in the distributed-systems core:

- **#4 (multi-node consistency, 3)** and **#12 (self-delivery registry, 3)** are the two most
  open — the first because its foundation (local-apply + random-eid dedup) is one of many
  plausible schemes resting on subtle ordering arguments, the second because a module-global
  cross-wire map is a solvable-differently smell.
- **#5, #9, #10, #17 (4)** are deliberate bets — crash-detection timing over generic KV, a
  single DM-only guard, wire-level binary selectivity, and a per-member-key mode — each defensible
  but each clearly one choice among several.

If this PR is revisited, expect the churn there, not in the messaging or serialization plumbing.
