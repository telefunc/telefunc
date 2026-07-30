export { Room }

import React, { useEffect, useState } from 'react'
import {
  onCreateRoom,
  onGetRoom,
  onGetRoomTail,
  onGetOrCreateRoom,
  onGetTypedRoom,
  onGetGuardedRoom,
  onGetAuditRoom,
  onGetAudit,
  onJoinAsServer,
  onJoinRoomAsServerSelf,
  onGetRoomWithMember,
  onWatchRoom,
  onGetWatched,
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
} from './Room.telefunc'
import { roomScenario } from './Room.scenarios'

/** Render every poll so the e2e autoRetry sees fresh data on each iteration (see Publish.tsx).
 *  `render` may be async — some scenarios read server-side state (e.g. an audit log) each tick. */
async function pollUntil(render: () => { done: boolean } | Promise<{ done: boolean }>) {
  for (let poll = 0; poll < 50; poll++) {
    if ((await render()).done) break
    await new Promise((r) => setTimeout(r, 200))
  }
}

let gcParticipant: { publish(data: unknown): Promise<unknown> } | null = null
let gcRoomRef: WeakRef<object> | null = null

async function retainOnlyJoinedParticipant(roomId: string): Promise<void> {
  const room = await onGetRoom(roomId)
  gcRoomRef = new WeakRef(room)
  gcParticipant = await room.join({ meta: { name: 'GC survivor' } })
}

function Room() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="room-result">{result}</pre>

      <h2>Presence & Chat</h2>

      <button
        id={roomScenario('chat').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-chat:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)

          const events: string[] = []
          lobby.onJoin((m) => events.push(`join:${m.meta.name}`))
          lobby.onLeave((m) => events.push(`leave:${m.meta.name}`))
          const received: Array<{ text: unknown; from: unknown }> = []
          lobby.subscribe((data, _info, from) => {
            received.push({ text: (data as { text: string }).text, from: from.meta.name })
          })

          const snapBefore = lobby.snapshot()
          let changes = 0
          lobby.onChange(() => changes++)

          const me = await lobby.join({ meta: { name: 'Alice' } })
          const countAfterJoin = lobby.count
          const updates: unknown[] = []
          ;(await lobby.getParticipant(me.id))!.onUpdate((meta, prev) => updates.push([meta.score, prev.score]))

          const ack = await me.publish({ text: 'hello' })
          await me.setMeta({ name: 'Alice', score: 42 })
          await me.leave()

          await pollUntil(() => {
            const state = {
              events,
              received,
              updates,
              countAfterJoin,
              count: lobby.count,
              ack: { key: ack.key, seq: ack.seq },
              snapshotChanged: lobby.snapshot() !== snapBefore,
              snapshotStable: lobby.snapshot() === lobby.snapshot(),
              changes,
            }
            setResult(JSON.stringify(state))
            return {
              done: events.length >= 2 && received.length >= 1 && updates.length >= 1 && changes >= 1,
            }
          })
        }}
      >
        Join, publish, setMeta, leave
      </button>

      <h2>Retained Replay</h2>

      <button
        id={roomScenario('retain').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-retain:${crypto.randomUUID()}`
          await onCreateRoom(roomId)

          // A publisher pins a retained message; a subscriber that arrives *after* the publish must
          // still receive it (MQTT-style). The retained slot is read only once the subscription is
          // live at the backend — the readiness handoff — so an async transport (Redis) can't drop
          // it in the gap between subscribing and the read. Keep the publisher's view referenced so
          // its membership (and thus the owned retained slot) survives until the late subscriber reads.
          const pubView = await onGetRoom(roomId)
          const author = await pubView.join({ meta: { name: 'Author' } })
          await author.publish({ text: 'pinned' }, { retain: true })

          const late = await onGetRoom(roomId)
          const received: string[] = []
          late.subscribe((data) => received.push((data as { text: string }).text))

          await pollUntil(() => {
            setResult(JSON.stringify({ received }))
            return { done: received.length >= 1 }
          })
        }}
      >
        Retained replay to a late subscriber
      </button>

      <h2>Server-Joined Participant</h2>

      <button
        id={roomScenario('participant').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-participant:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const observer = await onGetRoom(roomId)
          const received: Array<{ text: unknown; from: unknown }> = []
          observer.subscribe((data, _info, from) => {
            received.push({ text: (data as { text: string }).text, from: from.meta.name })
          })

          // Joined server-side — arrives as a standalone participant with its own stub channel.
          const me = await onJoinAsServer(roomId, 'Bob')
          await me.publish({ text: 'from-bob' })
          await me.setMeta({ name: 'Bobby' })
          // Live handle (meta stays fresh); retry — the join event may still be in flight.
          let remoteMe = await observer.getParticipant(me.id)
          for (let i = 0; i < 50 && !remoteMe; i++) {
            await new Promise((r) => setTimeout(r, 200))
            remoteMe = await observer.getParticipant(me.id)
          }

          // Direct message: a room-joined participant whispers to the standalone one.
          // Privacy: it must reach Bob's inbox and never the room stream.
          const ally = await observer.join({ meta: { name: 'Ally' } })
          const dms: Array<{ data: unknown; fromAlly: boolean }> = []
          me.listen((data, from) => dms.push({ data, fromAlly: from?.id === ally.id }))
          await ally.send(me.id, 'psst')

          await pollUntil(() => {
            const state = {
              received,
              count: observer.count,
              remoteMetaName: remoteMe?.meta.name ?? null,
              localMetaName: me.meta.name ?? null,
              localIdentity: me.identity,
              remoteIdentity: remoteMe?.identity ?? null,
              dms,
            }
            setResult(JSON.stringify(state))
            return {
              done:
                received.length >= 1 &&
                state.remoteMetaName === 'Bobby' &&
                state.localMetaName === 'Bobby' &&
                dms.length >= 1,
            }
          })
        }}
      >
        Server-side join + publish
      </button>

      <h2>Binary</h2>

      <button
        id={roomScenario('binary').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-binary:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const videoRoom = await onGetRoom(roomId)
          const me = await videoRoom.join({ meta: { name: 'Cam' } })

          const frames: Array<{
            size: number
            firstByte: number
            fromSelf: boolean
            track: string | null
            meta: Record<string, unknown> | null
          }> = []
          videoRoom.subscribeBinary((data, info, from) => {
            frames.push({
              size: data.byteLength,
              firstByte: data[0]!,
              fromSelf: from.id === me.id,
              track: info.track,
              meta: info.meta,
            })
          })
          const cameraOnly: number[] = []
          videoRoom.subscribeBinary((data) => cameraOnly.push(data[0]!), { track: 'camera' })
          const defaultOnly: number[] = []
          videoRoom.subscribeBinary((data) => defaultOnly.push(data[0]!), { track: null })

          // selfDelivery defaults to true — our own frames come back to us.
          for (let i = 0; i < 3; i++) {
            await me.publishBinary(new Uint8Array(64).fill(i + 1))
          }
          // Named track + per-frame meta — mic/camera/screen multiplex over one member lane;
          // the ack's `receivers` counts the track's live subscriptions (the pause-at-0 signal).
          const camAck = await me.publishBinary(new Uint8Array(32).fill(9), { track: 'camera', meta: { key: true } })

          await pollUntil(() => {
            setResult(JSON.stringify({ frames, cameraOnly, defaultOnly, camReceivers: camAck.receivers }))
            return {
              done:
                frames.length >= 4 &&
                cameraOnly.length >= 1 &&
                defaultOnly.length >= 3 &&
                typeof camAck.receivers === 'number' &&
                camAck.receivers >= 1,
            }
          })
        }}
      >
        Publish 3 binary frames
      </button>

      <h2>Guards</h2>

      <button
        id={roomScenario('guard').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-guard:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const room = await onGetGuardedRoom(roomId)

          const received: unknown[] = []
          room.subscribe((data) => received.push(data))
          // Admission is guarded too — the rejection reaches the joiner's promise over the wire.
          const joinError = await room.join({ meta: { name: 'Banned' } }).then(
            () => null,
            (err: Error) => err.message,
          )
          const me = await room.join({ meta: { name: 'Mallory' } })
          const peer = await room.join({ meta: { name: 'Peer' } })
          const inbox: unknown[] = []
          peer.listen((data) => inbox.push(data))

          // Guard rejections travel back over the wire to the caller's promise.
          const publishError = await me.publish('forbidden').then(
            () => null,
            (err: Error) => err.message,
          )
          const sendError = await me.send(peer.id, 'forbidden').then(
            () => null,
            (err: Error) => err.message,
          )
          await me.publish('fine')
          await me.send(peer.id, 'psst')

          await pollUntil(() => {
            setResult(JSON.stringify({ joinError, publishError, sendError, received, inbox }))
            return { done: received.length >= 1 && inbox.length >= 1 }
          })
        }}
      >
        Guarded publish & send
      </button>

      <button
        id={roomScenario('shield').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-shield:${crypto.randomUUID()}`
          // A room declared with a message type — `Room.create<…, ChatMsg>` inside the telefunction.
          const room = await onGetTypedRoom(roomId)
          const me = await room.join({ meta: { name: 'A' } })
          const received: string[] = []
          room.subscribe((data) => received.push(data.text))

          // A well-typed payload sails through the shield auto-generated from the declared type.
          const okAck = await me.publish({ kind: 'chat', text: 'hi' }).then(
            () => true,
            () => false,
          )
          // A malformed payload — cast past the compile-time type, as an untyped or hostile client could
          // send — is rejected at the server ingress by that same auto-generated shield, before any handler.
          const badError = await me.publish({ kind: 'chat' } as unknown as { kind: 'chat'; text: string }).then(
            () => null,
            (err: Error) => err.name,
          )

          await pollUntil(() => {
            setResult(JSON.stringify({ okAck, badError, received }))
            return { done: received.length >= 1 }
          })
        }}
      >
        Shielded publish
      </button>

      <h2>Returnable member view</h2>

      <button
        id={roomScenario('member').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-member:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)
          const me = await lobby.join({ meta: { name: 'Viewed' } })

          // A telefunction returns { room, member } — ref-identity binds the view to the room.
          const out = await onGetRoomWithMember(roomId, me.id)
          const viaRoom = await out.room.getParticipant(me.id)
          const state = {
            hasMember: out.member !== null,
            memberName: out.member?.meta.name ?? null,
            sameObject: out.member !== null && viaRoom === out.member,
          }
          setResult(JSON.stringify(state))
        }}
      >
        Return room + member view
      </button>

      <h2>Admin (announce, system send, kick & close)</h2>

      <button
        id={roomScenario('admin').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-admin:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)
          const me = await lobby.join({ meta: { name: 'Eve' } })

          // Room-authored messages: a broadcast to everyone, and a whisper (fromId === '').
          const announcements: unknown[] = []
          lobby.onAnnounce((data) => announcements.push(data))
          const system: Array<{ data: unknown; fromRoom: boolean }> = []
          me.listen((data, from) => system.push({ data, fromRoom: from === null }))

          let kicked = false
          let kickCause: unknown = null
          let closed = false
          me.onLeave((cause) => {
            kicked = true
            kickCause = cause
          })
          lobby.onClose(() => (closed = true))

          await onAnnounce(roomId, 'maintenance')
          await onSystemSend(roomId, me.id, 'welcome')
          await pollUntil(() => {
            setResult(JSON.stringify({ announcements, system }))
            return { done: announcements.length >= 1 && system.length >= 1 }
          })

          await onKick(roomId, me.id)
          await onCloseRoom(roomId)
          await pollUntil(() => {
            const state = {
              announcements,
              system,
              kicked,
              kickCause,
              closed,
              isClosed: lobby.isClosed,
              count: lobby.count,
            }
            setResult(JSON.stringify(state))
            return { done: kicked && closed && lobby.isClosed }
          })
        }}
      >
        Announce, system send, kick, close
      </button>

      <h2>Coalesce (conflation)</h2>

      <button
        id={roomScenario('conflate').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-conflate:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)
          const received: number[] = []
          lobby.subscribe((data) => received.push((data as { n: number }).n))
          const me = await lobby.join({ meta: { name: 'Cursor' } })

          // A synchronous burst under one key: the first send goes, 2..5 collapse into a single
          // pending, so only the first and the latest reach the room — deterministically [1, 5].
          const acks = await Promise.all([1, 2, 3, 4, 5].map((n) => me.publish({ n }, { coalesce: 'cursor' })))

          await pollUntil(() => {
            setResult(JSON.stringify({ received, acked: acks.length, allSeqs: acks.every((a) => a.seq > 0) }))
            return { done: received.includes(5) && received.length >= 2 }
          })
        }}
      >
        Coalesced burst
      </button>

      <h2>setAttributes (partial merge)</h2>

      <button
        id={roomScenario('attributes').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-attr:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)
          const me = await lobby.join({ meta: { name: 'Zoe', score: 1 } })
          let remote = await lobby.getParticipant(me.id)
          for (let i = 0; i < 50 && !remote; i++) {
            await new Promise((r) => setTimeout(r, 200))
            remote = await lobby.getParticipant(me.id)
          }

          await me.setAttributes({ score: 2 }) // merge — name is untouched
          await me.setAttributes({ title: 'lead' }) // add a key
          await me.setAttributes({ score: undefined }) // a key set to undefined is removed

          await pollUntil(() => {
            const meta = (remote?.meta ?? {}) as { name?: string; score?: number; title?: string }
            const localMeta = me.meta as { name?: string; score?: number; title?: string }
            setResult(
              JSON.stringify({
                name: meta.name ?? null,
                title: meta.title ?? null,
                hasScore: 'score' in meta,
                localName: localMeta.name ?? null,
                localHasScore: 'score' in localMeta,
              }),
            )
            return { done: meta.name === 'Zoe' && meta.title === 'lead' && !('score' in meta) }
          })
        }}
      >
        Merge & delete attributes
      </button>

      <h2>onDemand (track demand)</h2>

      <button
        id={roomScenario('demand').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-demand:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const pubRoom = await onGetRoom(roomId)
          const pub = await pubRoom.join({ meta: { name: 'Pub' } })
          const cam: boolean[] = []
          pub.onDemand((track, wanted) => {
            if (track === 'camera') cam.push(wanted)
          })
          // Announce the camera track so demand is attributable to (Pub, camera).
          await pub.publishBinary(new Uint8Array(8).fill(1), { track: 'camera', meta: { key: true } })

          // A separate observer wants the track — demand rises; releasing it — demand falls.
          const viewer = await onGetRoom(roomId)
          const unsub = viewer.subscribeBinary(() => {}, { track: 'camera' })
          await pollUntil(() => {
            setResult(JSON.stringify({ cam }))
            return { done: cam.includes(true) }
          })
          unsub()
          await pollUntil(() => {
            setResult(JSON.stringify({ cam }))
            return { done: cam.includes(true) && cam[cam.length - 1] === false }
          })
        }}
      >
        Track demand up & down
      </button>

      <h2>Tail (single-call history)</h2>

      <button
        id={roomScenario('tail').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-tail:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const owner = await onGetRoom(roomId)
          const me = await owner.join({ meta: { name: 'Src' } })

          // Tail handle: relay starts at serialize time, buffered on the client until subscribe().
          const tailed = await onGetRoomTail(roomId)
          // Published AFTER the tail handle exists but BEFORE we subscribe — must not be dropped.
          await me.publish({ t: 'between' })

          const received: string[] = []
          tailed.subscribe((data) => received.push((data as { t: string }).t))

          await pollUntil(() => {
            setResult(JSON.stringify({ received }))
            return { done: received.includes('between') }
          })
        }}
      >
        Tail holds pre-subscribe messages
      </button>

      <h2>After-hooks (persistence receipts)</h2>

      <button
        id={roomScenario('hooks').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-hooks:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const room = await onGetAuditRoom(roomId)
          const a = await room.join({ meta: { name: 'A' } })
          const b = await room.join({ meta: { name: 'B' } })
          await a.publish('hello')
          await a.send(b.id, 'dm')

          await pollUntil(async () => {
            const auditLog = (await onGetAudit(roomId)) as Array<{
              kind: string
              name?: string
              to?: string
              seq?: number
              joinedAt?: number
              data?: unknown
            }>
            const joinA = auditLog.find((e) => e.kind === 'join' && e.name === 'A')
            const publish = auditLog.find((e) => e.kind === 'publish')
            const send = auditLog.find((e) => e.kind === 'send')
            setResult(
              JSON.stringify({
                joins: auditLog
                  .filter((e) => e.kind === 'join')
                  .map((e) => e.name)
                  .sort(),
                joinHasTs: typeof joinA?.joinedAt === 'number',
                publish: publish ? { name: publish.name, data: publish.data, seqOk: (publish.seq ?? 0) > 0 } : null,
                send: send ? { name: send.name, to: send.to, seqOk: (send.seq ?? 0) > 0 } : null,
              }),
            )
            return { done: !!joinA && !!publish && !!send }
          })
        }}
      >
        After-join/publish/send receipts
      </button>

      <h2>Member-selective receive</h2>

      <button
        id={roomScenario('memberSub').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-membersub:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const room = await onGetRoom(roomId)
          const x = await room.join({ meta: { name: 'X' } })
          const y = await room.join({ meta: { name: 'Y' } })

          const observer = await onGetRoom(roomId)
          let remoteX = await observer.getParticipant(x.id)
          for (let i = 0; i < 50 && !remoteX; i++) {
            await new Promise((r) => setTimeout(r, 200))
            remoteX = await observer.getParticipant(x.id)
          }
          const xText: string[] = []
          const xBin: number[] = []
          remoteX!.subscribe((data) => xText.push(data as string))
          remoteX!.subscribeBinary((data) => xBin.push(data[0]!), { track: null })
          // Room-level control: proves Y's traffic really was delivered, so xText's absence means something.
          const all: string[] = []
          observer.subscribe((data) => all.push(data as string))

          await x.publish('x1')
          await y.publish('y1')
          await x.publishBinary(new Uint8Array([7]))
          await y.publishBinary(new Uint8Array([8]))

          await pollUntil(() => {
            setResult(JSON.stringify({ xText, xBin, all: [...all].sort() }))
            return { done: all.includes('x1') && all.includes('y1') && xBin.includes(7) }
          })
        }}
      >
        Follow one member
      </button>

      <h2>DM pre-listen hold</h2>

      <button
        id={roomScenario('dmHold').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-dmhold:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const observer = await onGetRoom(roomId)
          const bob = await onJoinAsServer(roomId, 'Bob')
          const ally = await observer.join({ meta: { name: 'Ally' } })

          // Sent BEFORE Bob listens — held in his inbox, flushed the moment he attaches.
          await ally.send(bob.id, 'early')
          const held: string[] = []
          bob.listen((data) => held.push(data as string))

          await pollUntil(() => {
            setResult(JSON.stringify({ held }))
            return { done: held.includes('early') }
          })
        }}
      >
        Send before listen
      </button>

      <h2>selfDelivery: false</h2>

      <button
        id={roomScenario('self').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-self:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          // The "others receive it" observer is a server-side subscriber — a genuinely different
          // client with its own stub. Suppression is per-stub at the source, so it isn't affected.
          await onWatchRoom(roomId)
          const room = await onGetRoom(roomId)
          const me = await room.join({ meta: { name: 'Solo' }, selfDelivery: false })
          const mine: string[] = []
          room.subscribe((data) => mine.push(data as string))

          await me.publish('hi')

          await pollUntil(async () => {
            const theirs = (await onGetWatched(roomId)) as string[]
            setResult(JSON.stringify({ mine, theirs, selfDelivery: me.selfDelivery }))
            // `theirs` arriving proves the publish propagated — so `mine` staying empty is meaningful.
            return { done: theirs.includes('hi') }
          })
        }}
      >
        Own frames suppressed
      </button>

      <h2>selfDelivery: false (server co-return + client join on one stub)</h2>

      <button
        id={roomScenario('selfServer').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-self-server:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          await onWatchRoom(roomId) // a different (server-side) client — receives everything

          // `me`: server-side join with selfDelivery:false, co-returned with its room. Its own echo
          // must be dropped at the source for this client's `room` view (never crosses the wire).
          const { room, me } = await onJoinRoomAsServerSelf(roomId, 'Solo')
          const mine: string[] = []
          room.subscribe((data) => mine.push(data as string))
          await me.publish('from-me')

          // `notMe`: a client-side join (selfDelivery on) on the SAME room stub — its publishes DO
          // come back to `mine`. Proves both source gates coexist on one subscription, keyed by sender.
          const notMe = await room.join({ meta: { name: 'NotMe' } })
          await notMe.publish('from-notme')

          await pollUntil(async () => {
            const theirs = (await onGetWatched(roomId)) as string[]
            setResult(JSON.stringify({ mine, theirs, selfDelivery: me.selfDelivery }))
            // Gate on the client seeing notMe's frame (published last) and the watcher seeing me's.
            return { done: mine.includes('from-notme') && theirs.includes('from-me') }
          })
        }}
      >
        Co-return suppressed, client join delivered
      </button>

      <h2>Reconfigure, list, getOrCreate</h2>

      <button
        id={roomScenario('reconfig').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const base = `e2e-reconfig:${crypto.randomUUID()}`
          const roomId = `${base}:a`
          await onCreateRoom(roomId)
          const room = await onGetRoom(roomId)
          await room.join({ meta: { name: 'R' } })
          const updates: string[] = []
          room.onUpdate((meta) => updates.push((meta as { topic?: string }).topic ?? ''))

          await onUpdateRoom(roomId, { topic: 'updated' })
          await onCreateRoom(`${base}:b`) // a second room under the same prefix, for list()
          const same = await onGetOrCreateRoom(roomId) // idempotent — returns the existing room
          const listed = await onListRooms(base)

          await pollUntil(() => {
            setResult(
              JSON.stringify({
                updates,
                topic: (room.meta as { topic?: string }).topic ?? null,
                listed,
                sameId: same.id === roomId,
                sameCount: same.count,
              }),
            )
            return { done: updates.includes('updated') }
          })
        }}
      >
        Update, list, getOrCreate
      </button>

      <h2>Kick by identity, onEmpty</h2>

      <button
        id={roomScenario('identity').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-identity:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const observer = await onGetRoom(roomId)
          let empty = false
          observer.onEmpty(() => {
            empty = true
          })

          // Server-side join stamps identity 'user:Multi'.
          const multi = await onJoinAsServer(roomId, 'Multi')
          let cause: { type: string; reason?: unknown } | null = null
          multi.onLeave((c) => {
            cause = c
          })
          await pollUntil(() => ({ done: observer.count >= 1 }))

          await onKickByIdentity(roomId, 'user:Multi')
          await pollUntil(() => {
            setResult(JSON.stringify({ cause, count: observer.count, empty }))
            return { done: cause !== null && observer.count === 0 }
          })
        }}
      >
        Remove by identity
      </button>

      <h2>Participant keeps its Room alive</h2>

      <button
        id={roomScenario('gcParticipant').selector.slice(1)}
        onClick={async () => {
          setResult('')
          const roomId = `e2e-gc-participant:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          await retainOnlyJoinedParticipant(roomId)
          setResult(JSON.stringify({ phase: 'ready' }))
        }}
      >
        Join and retain only the participant
      </button>
      <button
        id="test-room-gc-participant-publish"
        onClick={async () => {
          const roomAlive = gcRoomRef?.deref() !== undefined
          const published = await gcParticipant!.publish('after-gc').then(
            () => true,
            () => false,
          )
          setResult(JSON.stringify({ phase: 'published', roomAlive, published }))
        }}
      >
        Publish from retained participant
      </button>
    </div>
  )
}
