# `@telefunc/room` architecture

A map for reading the code. The user docs are `docs/pages/room/+Page.mdx`; this file says how the parts fit and what
the private nouns mean.

## The path of a call

```
user code ──► Room (ServerRoom, the user's facade)              client: ClientRoom ◄── its stub's channel
                ▼                                                                       ▲
              handle (RoomHandle, one per Room.get())                                   │
                │  guards, tail, stubs, participants, listeners                         │
                ▼                                                                       │
              replica (RoomReplica, one per room per process) ── model (RoomModel) ── views (RoomState)
                │  lease, heartbeat, demand, authority sync
                ▼
              lane plan (LanePlan) ── which lanes this process subscribes, from its holders' wants
                │
                ▼
              plane (RoomPlane) ── checks the driver's orderings, shares lane subscriptions
                │
                ▼
              driver (RoomDriver: memory, Redis, or Cloudflare) ── the room's authority:
                   head, cells, lanes, retained frames, directory
```

A write goes down the same path: the handle's `RoomWrites` runs a store function (`server/store/`), which runs an
operation or a cell exchange on the driver, then commits an event on a lane. Every process with the lane subscribed,
this one too, applies the event to its model, and its views tell their listeners.

## Glossary

**Room's state at its authority** (the driver's; `backend/contract.ts`):
- **authority**: where a room's state lives and is ordered: the memory driver's maps, Redis, or the room's Durable
  Object on Cloudflare (`cloudflare/RoomAuthority.ts`).
- **head**: a room's root record: which incarnation is current, its state (`open`, `closing`, `closed`), its config,
  and when it lapses. Only compare-exchanged.
- **incarnation** (`inc`): one life of a room, from create to close. A room recreated under the same id is a new
  incarnation, and everything below is scoped to one.
- **cell**: a versioned key-value entry of an incarnation: a member's record, its meta keys, the hold counters, a
  lease, a view. Read in pages, written by compare-exchange or by an operation.
- **operation**: a whole write (join, leave, a member's meta write) as one pure plan whose writes the driver applies
  atomically (`backend/operation.ts`, `server/store/roomOperations.ts`). Memory runs it inline, and Cloudflare in one
  transaction in the authority. Redis plans it in the process, a round trip per step of cells it reads, then applies
  it in one Lua script, which checks that the cells read are unchanged. If one changed, the operation is planned again
  from the start (`RedisRoomDriver.runOperation()`).
- **lane**: an ordered stream of an incarnation's commits, each with a seq: one semantic (messages), one control
  (membership, meta, close), one demand, one inbox per member (DMs), one binary per (member, track).
- **retained**: a lane's last frame kept for late subscribers (`publish(…, { retain: true })`).
- **directory**: the listing `Room.list()` pages through, and the due-check index that closes lapsed rooms.
- **fence** (Redis): a token a commit carries so the committer knows when its own subscriptions saw it.

**A room in one process** (`server/`):
- **handle**: one `Room.get()`'s `RoomHandle`: its guards, tail, stubs, participants and listeners. The user gets
  `ServerRoom`, a facade over it; Room's own code gets the handle (`handleOf(room)`).
- **replica**: the one `RoomReplica` per room per process that every handle shares: the model, the lane plan, the
  lease, the heartbeat and the demand. It keeps the handles it serves, and what they are to the room (observed,
  listened, a tail held, the members owned), in the methods each change of a handle goes through; the lane plan reads
  those roles as data.
- **model** / **view**: a room's members, meta and closure as one side knows them (`RoomModel`), and one listener set
  over it (`RoomState`).
- **stub**: the server end of a client's room: what the client wants, and the members it joined (`RoomStub`).
- **holder**: anything lanes replay into: a stub, or the handle's own listeners (`LaneHolder`).
- **wants**: the lanes a holder asks for: text, and binary per member and track (`WantsIndex`).
- **tail**: the recent messages a handle holds between its serialization and its client's first subscribe
  (`TailHold`).
- **admission**: a join under way through one handle: its guard, then its inbox, then its record and join event
  (`Admission`).
- **lease**: a process's claim on the members and views it hosts. Its heartbeat renews it; a lapsed lease's members
  are reaped by the next process that finds it (`store/lease.ts`).
- **hold**: what keeps a room open: its holders and members are counted, and once nothing holds it, it closes after
  its timeout (`lifecycle/hold.ts`).
- **roster**: the members as the authority lists them, read to correct a view that drifted (`store/roster.ts`,
  `RosterStream`).
- **demand**: whether any process wants a member's track, exchanged on the demand lane so a publisher knows
  (`RoomDemand`, `DemandExchange`, `GossipOutbox`).
- **horizon**: `ROOM_HORIZON_MS`, how long Room waits for a lane to be ready, a delivery handoff or a lost lane's read.
- **plane**: what Room's server code calls instead of the driver: its writes' orderings checked, its subscriptions
  shared (`backend/RoomPlane.ts`). The **kit** is the lane keys and page sizes a driver is built with.
- **mirror** / **rewind**: a client's copy of a participant's meta (`metaMirrors`); a failover that resets meta seqs
  (`MetaRewinds`).

## Implementing a backend

A backend serves Room's plane with a `RoomDriver` (`backend/contract.ts`, whose JSDoc defines each name below;
`MemoryRoomDriver.ts` is the shortest driver to read). Each of these writes is one atomic step at the authority: what it
checks and what it applies hold together, with no other call in between. A step whose check fails applies nothing,
and returns the refusal its result type names (`conflict`, `stale-incarnation` or `stale-cell`).

- `compareExchangeHead()` writes the head and counts `counts` in its `currentInc`, if the stored head is what
  `expected` names.
- `compareExchangeCells()` applies the mutations and the counts, if the head's open incarnation is `inc` and each
  expected cell is at its version (`null`: still absent).
- `runOperation()` applies the writes its plan recorded (`OperationTx`) and takes the seq of its control commit, if
  it has one, while the head's open incarnation is `inc`. A driver that plans outside the step, as Redis does, also
  checks that each cell the plan read, and each dropped member's lanes, are as the plan read them, and plans again if
  one changed (`RedisRoomDriver.runOperation()`).
- `commitLane()` takes the lane's next seq, keeps the retained frame, and hands the frame to the lane's subscriptions
  in seq order, if the head's open incarnation is `inc`, or, with `closingLease`, the commit is a control commit and
  the head is closing under that lease. Each of `requiredCellKeys` must exist too.
- `directoryPut()` writes one room's entry, with its tag and when it is due. `directoryDelete()` deletes it if it is
  still listed under `incTag`.

The other calls are each atomic only on their own. A cell read, and each page of `readCellPages()`, is taken at once,
but nothing holds between two of them. `subscribe()` delivers each lane's commits in seq order from when it confirms;
a frame retained before that is read with `readRetained()`. `dropIncarnation()` returns quietly for an incarnation
already dropped, because a close, a create and the directory's due check can each drop one that was left over. An
inbox or control commit's `delivery`, its handoff to the subscriptions, settles after the step, outside it.

**Redis's operation packet.** `OPERATION_LUA` (`redis/src/commands.ts`) takes a number of KEYS and ARGV that depends on
the operation. `operation.invoke()` in the same file lays them out, and the script walks them in the same order with a
running index. The `OperationPacket` type writes down that layout: its fields say how many KEYS each part takes and
which ARGV holds its bytes, in this order: the fixed KEYS, two per dropped member, three per retained binary frame, one
per track lane set, one per expiring cell, then the lane commit's. Nothing checks it at runtime, and a mismatch reads,
writes or deletes the wrong key without an error, so a change to the order on one side changes the other in the same
commit. The lane commit's own KEYS and ARGV, which `commitLane()` and an operation share, can't drift: `COMMIT_KEYS`
and `COMMIT_ARGS` name each slot once, and both its script and `commitInvocation()` are built from them.

## The lane plan

`LanePlan` subscribes the fewest lanes that carry what this process's holders want, and no frame they don't. Text is
one semantic lane. A member's binary tracks come:
- on a lane per (member, track), for the tracks a holder here names of that member alone;
- on the member's every-track lane, while a holder here wants all of its tracks, or names more than
  `ROOM_MEMBER_NAMED_LANES_MAX` of them;
- on a track's every-member lane, for a track a holder here names of every member. Once those tracks outnumber both
  `ROOM_MEMBER_NAMED_LANES_MAX` and the members, they come on the members' every-track lanes instead, until the members
  are twice as many as the tracks.

The plan is three parts, each using only the one before it:
- **The choice** (`shapeOf()`, `servesByAll()` and `nextByMember()` in `LanePlan.ts`): which lane shape serves each
  (member, track), from the holders' wants, the membership and the two rules above.
- **The switch** (`BinaryLanes`): the binary subscriptions of each shape. A subscription delivers only what is published
  after it confirms, so a pair whose shape changes keeps its old lane until the new one delivers, and a fan-in lane also
  stays while one of the other kind is establishing (`Establishing`), so a switch loses no frame. Each switch returns
  what then delivers the pair, member or track it switched. During the overlap a frame comes on both lanes with one
  order, and the holder keeps it once (`ReplayGate.admitBinary()`, per (member, track)).
- **The pass** (`LanePlan`): the changes queue, and one pass at a time plans them, in `ROOM_PLAN_SLICE_MS` slices, then
  tells the demand from the choice and from what each switch returned. `BinaryLanes` never calls the plan: each round
  starts by taking what the lanes' readiness and switches replanned (`takeReplans()`).

**Why not one every-track lane per wanted member, filtered here.** Measured on the memory driver, 50 members each
publishing 10 frames of 256 bytes on each of 4 tracks (2000 frames), with one handle here listening (the probe counts
the driver's subscriptions and the binary frames it delivers to this process):

| the handle listens to | lanes opened | frames delivered here | one every-track lane per member |
|---|---|---|---|
| one track of every member | 1 every-member lane | 500 | 50 lanes, 2000 frames |
| one track of 5 members | 5 (member, track) lanes | 50 | 5 lanes, 200 frames |
| every track of 5 members | 5 every-track lanes | 200 | 5 lanes, 200 frames |

The simpler plan delivers every track of each member it covers, 4 times the frames here at 4 equal tracks, and more
when the unwanted tracks are video. On Redis each frame is a Pub/Sub message to this process, and on Cloudflare a
delivery to its session's Durable Object. It also opens 50 subscriptions where one does.

**The thresholds, measured.** `probes/lane-plan.mjs` (memory driver; the members joined on another server's lease, so
only the view's lanes are here; `node packages/room/probes/lane-plan.mjs` after a build) counts the binary
subscriptions opened and closed:

| what changes | where the plan switches | subscribes and unsubscribes |
|---|---|---|
| tracks named of every member, 10 members, rising to 60, then falling | every-track lanes at 17 tracks, back at 16 | 26, 26 |
| the same, 20 members | at 21, back at 16 | 40, 36 |
| the same, 40 members | at 41, back at 20 | 80, 60 |
| 20 members, the tracks hovering between 20 and 21, ten times | once, at the first 21 | 40, then 0 per cycle |
| one member's named tracks hovering between 16 and 17, ten times | each time: 16 named lanes, or its every-track lane | 34 per cycle |

The `byMember` band holds: at 40 members the plan switches at 41 tracks and back only at 20, and a want set hovering
inside the band switches nothing. `ROOM_MEMBER_NAMED_LANES_MAX` has no band: a member whose named tracks hover at the
limit switches its lanes on every change.

The same probe times a want change's pass until the lanes it opened are ready. The times below are one dev machine's,
and depend on it; the probe prints yours. One track of every member plans only that track's every-member lane: 0.2 ms
at 1,000, 10,000 and 100,000 members. Every track of every member plans each member: about 10 ms at 1,000 members and
90 ms at 10,000, in slices, the longest the event loop waited being 12–15 ms against the 10 ms `ROOM_PLAN_SLICE_MS`.
