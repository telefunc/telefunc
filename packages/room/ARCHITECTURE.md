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
- **operation**: a whole write (join, leave, a member's meta write) as one pure plan that the driver runs atomically
  where the state lives: inline on memory, one Lua script on Redis, one transaction in the authority on Cloudflare
  (`backend/operation.ts`, `server/store/roomOperations.ts`).
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

## The lane plan

`LanePlan` subscribes the fewest lanes that carry what this process's holders want, and no frame they don't. Text is
one semantic lane. A member's binary tracks come:
- on a lane per (member, track), for the tracks a holder here names of that member alone;
- on the member's every-track lane, while a holder here wants all of its tracks, or names more than
  `ROOM_MEMBER_NAMED_LANES_MAX` of them;
- on a track's every-member lane, for a track a holder here names of every member. Once those tracks outnumber both
  `ROOM_MEMBER_NAMED_LANES_MAX` and the members, they come on the members' every-track lanes instead, until the members
  are twice as many as the tracks.

A switch keeps the lanes it leaves until the lanes it takes deliver (`Establishing`), so a frame is neither lost nor
delivered twice.

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
