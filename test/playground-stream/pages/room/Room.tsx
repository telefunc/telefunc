export { Room }

import React, { useEffect, useState, useSyncExternalStore } from 'react'
import { keepReachable } from '../keepReachable'
import {
  onCreateRoom,
  onGetRoom,
  onGetRoomTail,
  onGetOrCreateRoom,
  onCreateTypedRoom,
  onOpenGuardedRoom,
  onOpenAuditedRoom,
  onGetAudit,
  onJoinAsServer,
  onJoinRoomAsServerSelf,
  onGetRoomWithMember,
  onWatchRoom,
  onGetWatched,
  onWatchRoomTrack,
  onGetWatchedTrack,
  onJoinHiddenAndHand,
  onHiddenSetMeta,
  onHiddenLeave,
  onHiddenAfterPlayers,
  onGetHiddenAfterPlayers,
  onCloseRoomWithMember,
  onBackgroundErrors,
  onRoomWhileAway,
  onRoomOverflow,
  onGetRoomOverflow,
  onRoomBurst,
  onGetWatchedCount,
  onRoomRelease,
  onFailBackend,
  onEndBackendSubscriptions,
  onListParticipantNames,
  onCreateRoomClosingAfter,
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
} from './Room.telefunc'

type PollResult = { done: boolean; result?: unknown }

async function getParticipantWhenJoined(room: Awaited<ReturnType<typeof onGetRoom>>, id: string) {
  let participant = await room.getParticipant(id)
  for (let i = 0; i < 50 && !participant; i++) {
    await new Promise((r) => setTimeout(r, 200))
    participant = await room.getParticipant(id)
  }
  if (!participant) throw new Error(`Participant did not join within the test horizon: ${id}`)
  return participant
}

async function createRoomId(label: string): Promise<string> {
  const roomId = `e2e-${label}:${crypto.randomUUID()}`
  await onCreateRoom(roomId)
  return roomId
}

async function createRoom(label: string) {
  const roomId = await createRoomId(label)
  return [roomId, await onGetRoom(roomId)] as const
}

type RoomView = Awaited<ReturnType<typeof onGetRoom>>

/** The documented React adapter: the room's methods passed as they are. */
function LiveCount({ room }: { room: RoomView }) {
  const snapshot = useSyncExternalStore(room.onChange, room.snapshot)
  return <pre id="room-live-count">{snapshot.count}</pre>
}

function Room() {
  const [result, setResult] = useState<string>('')
  const [liveRoom, setLiveRoom] = useState<RoomView | null>(null)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  /** Render every poll so e2e autoRetry sees fresh data; some reads are asynchronous. */
  const pollUntil = async (render: () => PollResult | Promise<PollResult>) => {
    for (let poll = 0; poll < 50; poll++) {
      const { done, result } = await render()
      if (result !== undefined) setResult(JSON.stringify(result))
      if (done) break
      await new Promise((resolve) => setTimeout(resolve, 200))
    }
  }

  const scenario = (id: string, heading: string | null, label: string, run: () => Promise<void>) => (
    <>
      {heading && <h2>{heading}</h2>}
      <button
        id={`test-room-${id}`}
        onClick={async () => {
          setResult('')
          await run()
        }}
      >
        {label}
      </button>
    </>
  )

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="room-result">{result}</pre>
      {liveRoom && <LiveCount room={liveRoom} />}

      {scenario('chat', 'Presence & Chat', 'Join, publish, setMeta, leave', async () => {
        const [roomId, lobby] = await createRoom('chat')

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
          return {
            result: state,
            done: events.length >= 2 && received.length >= 1 && updates.length >= 1 && changes >= 1,
          }
        })
      })}

      {scenario('retain', 'Retained Replay', 'Retained replay to a late subscriber', async () => {
        const roomId = await createRoomId('retain')

        const pubView = await onGetRoom(roomId)
        // A GC pass would close `pubView`, and Author leaving with it deletes the message it retained.
        const releasePubView = keepReachable(pubView)
        const author = await pubView.join({ meta: { name: 'Author' } })
        await author.publish({ text: 'pinned' }, { retain: true })

        const late = await onGetRoom(roomId)
        const received: string[] = []
        late.subscribe((data) => received.push((data as { text: string }).text))

        await pollUntil(() => ({ result: { received }, done: received.length >= 1 }))
        releasePubView()
      })}

      {scenario('participant', 'Server-Joined Participant', 'Server-side join + publish', async () => {
        const [roomId, observer] = await createRoom('participant')
        const received: Array<{ text: unknown; from: unknown }> = []
        observer.subscribe((data, _info, from) => {
          received.push({ text: (data as { text: string }).text, from: from.meta.name })
        })

        const me = await onJoinAsServer(roomId, 'Bob')
        await me.publish({ text: 'from-bob' })
        await me.setMeta({ name: 'Bobby' })
        // Live handle (meta stays fresh); retry (the join event may still be in flight).
        const remoteMe = await getParticipantWhenJoined(observer, me.id)

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
          return {
            result: state,
            done:
              received.length >= 1 &&
              state.remoteMetaName === 'Bobby' &&
              state.localMetaName === 'Bobby' &&
              dms.length >= 1,
          }
        })
      })}

      {scenario('binary', 'Binary', 'Publish 3 binary frames', async () => {
        const [roomId, videoRoom] = await createRoom('binary')
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

        for (let i = 0; i < 3; i++) {
          await me.publishBinary(new Uint8Array(64).fill(i + 1))
        }
        // Named track + per-frame meta: mic/camera/screen multiplex over one member lane;
        // the ack's `receivers` counts the track's live subscriptions (the pause-at-0 signal).
        const camAck = await me.publishBinary(new Uint8Array(32).fill(9), { track: 'camera', meta: { key: true } })

        await pollUntil(() => {
          return {
            result: { frames, cameraOnly, defaultOnly, camReceivers: camAck.receivers },
            done:
              frames.length >= 4 &&
              cameraOnly.length >= 1 &&
              defaultOnly.length >= 3 &&
              typeof camAck.receivers === 'number' &&
              camAck.receivers >= 1,
          }
        })
      })}

      {scenario('guard', 'Guards', 'Guarded publish & send', async () => {
        const roomId = await createRoomId('guard')
        const room = await onOpenGuardedRoom(roomId)

        const received: unknown[] = []
        room.subscribe((data) => received.push(data))
        const joinError = await room.join({ meta: { name: 'Banned' } }).then(
          () => null,
          (err: Error) => err.message,
        )
        const me = await room.join({ meta: { name: 'Mallory' } })
        const peer = await room.join({ meta: { name: 'Peer' } })
        const inbox: unknown[] = []
        peer.listen((data) => inbox.push(data))

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

        await pollUntil(() => ({
          result: { joinError, publishError, sendError, received, inbox },
          done: received.length >= 1 && inbox.length >= 1,
        }))
      })}

      {scenario('shield', null, 'Shielded publish', async () => {
        const roomId = `e2e-shield:${crypto.randomUUID()}`
        const room = await onCreateTypedRoom(roomId)
        const me = await room.join({ meta: { name: 'A' } })
        const received: string[] = []
        room.subscribe((data) => received.push(data.text))

        const okAck = await me.publish({ kind: 'chat', text: 'hi' }).then(
          () => true,
          () => false,
        )
        // Cast past the type to prove the generated shield rejects hostile ingress.
        const badError = await me.publish({ kind: 'chat' } as unknown as { kind: 'chat'; text: string }).then(
          () => null,
          (err: Error) => err.name,
        )

        await pollUntil(() => ({ result: { okAck, badError, received }, done: received.length >= 1 }))
      })}

      {scenario('member', 'Returnable member view', 'Return room + member view', async () => {
        const [roomId, lobby] = await createRoom('member')
        const me = await lobby.join({ meta: { name: 'Viewed' } })

        const out = await onGetRoomWithMember(roomId, me.id)
        const viaRoom = await out.room.getParticipant(me.id)
        const state = {
          hasMember: out.member !== null,
          memberName: out.member?.meta.name ?? null,
          sameObject: out.member !== null && viaRoom === out.member,
        }
        setResult(JSON.stringify(state))
      })}

      {scenario(
        'admin',
        'Admin (announce, system send, kick & close)',
        'Announce, system send, kick, close',
        async () => {
          const [roomId, lobby] = await createRoom('admin')
          const me = await lobby.join({ meta: { name: 'Eve' } })

          const announcements: unknown[] = []
          lobby.onAnnounce((data) => announcements.push(data))
          const system: Array<{ data: unknown; fromRoom: boolean }> = []
          me.listen((data, from) => system.push({ data, fromRoom: from === null }))
          await me.setAttributes({}) // same-channel ack orders the declarations before the HTTP-triggered broadcasts

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
          await pollUntil(() => ({
            result: { announcements, system },
            done: announcements.length >= 1 && system.length >= 1,
          }))

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
            return { result: state, done: kicked && closed && lobby.isClosed }
          })
        },
      )}

      {scenario('conflate', 'Coalesce (conflation)', 'Coalesced burst', async () => {
        const [roomId, lobby] = await createRoom('conflate')
        const received: number[] = []
        lobby.subscribe((data) => received.push((data as { n: number }).n))
        const me = await lobby.join({ meta: { name: 'Cursor' } })

        const acks = await Promise.all([1, 2, 3, 4, 5].map((n) => me.publish({ n }, { coalesce: 'cursor' })))

        await pollUntil(() => ({
          result: { received, ackSeqs: acks.map((ack) => ack.seq) },
          done: received.includes(5) && received.length >= 2,
        }))
      })}

      {scenario('attributes', 'setAttributes (partial merge)', 'Merge & delete attributes', async () => {
        const [roomId, lobby] = await createRoom('attr')
        const me = await lobby.join({ meta: { name: 'Zoe', score: 1 } })
        const remote = await getParticipantWhenJoined(lobby, me.id)

        await me.setAttributes({ score: 2 }) // merge: name is untouched
        await me.setAttributes({ title: 'lead' }) // add a key
        await me.setAttributes({ score: undefined }) // a key set to undefined is removed

        await pollUntil(() => {
          const meta = (remote?.meta ?? {}) as { name?: string; score?: number; title?: string }
          const localMeta = me.meta as { name?: string; score?: number; title?: string }
          return {
            result: {
              name: meta.name ?? null,
              title: meta.title ?? null,
              hasScore: 'score' in meta,
              localName: localMeta.name ?? null,
              localHasScore: 'score' in localMeta,
            },
            done: meta.name === 'Zoe' && meta.title === 'lead' && !('score' in meta),
          }
        })
      })}

      {scenario('demand', 'onDemand (track demand)', 'Track demand up & down', async () => {
        const [roomId, pubRoom] = await createRoom('demand')
        // A GC pass would close `pubRoom`, and Pub leaving with it ends the demand this waits on.
        const releasePubRoom = keepReachable(pubRoom)
        const pub = await pubRoom.join({ meta: { name: 'Pub' } })
        const cam: boolean[] = []
        pub.onDemand((track, wanted) => {
          if (track === 'camera') cam.push(wanted)
        })
        await pub.publishBinary(new Uint8Array(8).fill(1), { track: 'camera', meta: { key: true } })

        const viewer = await onGetRoom(roomId)
        const unsub = viewer.subscribeBinary(() => {}, { track: 'camera' })
        await pollUntil(() => ({ result: { cam }, done: cam.includes(true) }))
        unsub()
        await pollUntil(() => ({
          result: { cam },
          done: cam.includes(true) && cam[cam.length - 1] === false,
        }))
        releasePubRoom()
      })}

      {scenario('tail', 'Tail (single-call history)', 'Tail holds pre-subscribe messages', async () => {
        const [roomId, owner] = await createRoom('tail')
        const me = await owner.join({ meta: { name: 'Src' } })

        const tailed = await onGetRoomTail(roomId)
        await me.publish({ t: 'between' })

        const received: string[] = []
        tailed.subscribe((data) => received.push((data as { t: string }).t))

        await pollUntil(() => ({ result: { received }, done: received.includes('between') }))
      })}

      {scenario('hooks', 'After-hooks (persistence receipts)', 'After-join/publish/send receipts', async () => {
        const roomId = await createRoomId('hooks')
        const room = await onOpenAuditedRoom(roomId)
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
          return {
            result: {
              joins: auditLog
                .filter((e) => e.kind === 'join')
                .map((e) => e.name)
                .sort(),
              joinHasTs: typeof joinA?.joinedAt === 'number',
              publish: publish ? { name: publish.name, data: publish.data, seqOk: (publish.seq ?? 0) > 0 } : null,
              send: send ? { name: send.name, to: send.to, seqOk: (send.seq ?? 0) > 0 } : null,
            },
            done: !!joinA && !!publish && !!send,
          }
        })
      })}

      {scenario('member-sub', 'Member-selective receive', 'Follow one member', async () => {
        const [roomId, room] = await createRoom('membersub')
        // A GC pass would close `room`, and X leaving with it drops the observer's subscription to X's frames.
        const releaseRoom = keepReachable(room)
        const x = await room.join({ meta: { name: 'X' } })
        const y = await room.join({ meta: { name: 'Y' } })

        const observer = await onGetRoom(roomId)
        const remoteX = await getParticipantWhenJoined(observer, x.id)
        const xText: string[] = []
        const xBin: number[] = []
        remoteX.subscribe((data) => xText.push(data as string))
        remoteX.subscribeBinary((data) => xBin.push(data[0]!), { track: null })
        // Room-level control: proves Y's traffic really was delivered, so xText's absence means something.
        const all: string[] = []
        observer.subscribe((data) => all.push(data as string))

        await x.publish('x1')
        await y.publish('y1')
        await x.publishBinary(new Uint8Array([7]))
        await y.publishBinary(new Uint8Array([8]))

        await pollUntil(() => ({
          result: { xText, xBin, all: [...all].sort() },
          done: all.includes('x1') && all.includes('y1') && xBin.includes(7),
        }))
        releaseRoom()
      })}

      {scenario('dm-hold', 'DM pre-listen hold', 'Send before listen', async () => {
        const [roomId, observer] = await createRoom('dmhold')
        const bob = await onJoinAsServer(roomId, 'Bob')
        const ally = await observer.join({ meta: { name: 'Ally' } })

        await ally.send(bob.id, 'early')
        const held: string[] = []
        bob.listen((data) => held.push(data as string))

        await pollUntil(() => ({ result: { held }, done: held.includes('early') }))
      })}

      {scenario('self', 'selfDelivery: false', 'Own frames suppressed', async () => {
        const roomId = await createRoomId('self')
        await onWatchRoom(roomId)
        const room = await onGetRoom(roomId)
        const me = await room.join({ meta: { name: 'Solo' }, selfDelivery: false })
        const mine: string[] = []
        room.subscribe((data) => mine.push(data as string))

        await me.publish('hi')

        await pollUntil(async () => {
          const theirs = (await onGetWatched(roomId)) as string[]
          // `theirs` arriving proves the publish propagated, so `mine` staying empty is meaningful.
          return { result: { mine, theirs, selfDelivery: me.selfDelivery }, done: theirs.includes('hi') }
        })
      })}

      {scenario(
        'self-server',
        'selfDelivery: false (server co-return + client join on one stub)',
        'Co-return suppressed, client join delivered',
        async () => {
          const roomId = await createRoomId('self-server')
          await onWatchRoom(roomId) // a different (server-side) client, which receives everything

          const { room, me } = await onJoinRoomAsServerSelf(roomId, 'Solo')
          const mine: string[] = []
          room.subscribe((data) => mine.push(data as string))
          await me.publish('from-me')

          const notMe = await room.join({ meta: { name: 'NotMe' } })
          await notMe.publish('from-notme')

          await pollUntil(async () => {
            const theirs = (await onGetWatched(roomId)) as string[]
            // Gate on the client seeing notMe's frame (published last) and the watcher seeing me's.
            return {
              result: { mine, theirs, selfDelivery: me.selfDelivery },
              done: mine.includes('from-notme') && theirs.includes('from-me'),
            }
          })
        },
      )}

      {scenario('reconfig', 'Reconfigure, list, getOrCreate', 'Update, list, getOrCreate', async () => {
        const base = `e2e-reconfig:${crypto.randomUUID()}`
        const roomId = `${base}:a`
        await onCreateRoom(roomId)
        const room = await onGetRoom(roomId)
        await room.join({ meta: { name: 'R' } })
        const updates: string[] = []
        room.onUpdate((meta) => updates.push((meta as { topic?: string }).topic ?? ''))

        await onUpdateRoom(roomId, { topic: 'updated' })
        const secondRoomId = `${base}:b`
        await onCreateRoom(secondRoomId) // a second room under the same prefix, for list()
        const same = await onGetOrCreateRoom(roomId) // idempotent: returns the existing room
        const listed = await onListRooms(base)

        await pollUntil(() => {
          return {
            result: {
              updates,
              topic: (room.meta as { topic?: string }).topic ?? null,
              expectedIds: [roomId, secondRoomId],
              listed,
              sameId: same.id === roomId,
              sameCount: same.count,
            },
            done: updates.includes('updated'),
          }
        })
      })}

      {scenario('identity', 'Kick by identity, onEmpty', 'Remove by identity', async () => {
        const [roomId, observer] = await createRoom('identity')
        let empty = false
        observer.onEmpty(() => {
          empty = true
        })

        const multi = await onJoinAsServer(roomId, 'Multi')
        let cause: { type: string; reason?: unknown } | null = null
        multi.onLeave((c) => {
          cause = c
        })
        await pollUntil(() => ({ done: observer.count >= 1 }))

        await onKickByIdentity(roomId, 'user:Multi')
        await pollUntil(() => ({
          result: { cause, count: observer.count, empty },
          done: cause !== null && observer.count === 0,
        }))
      })}

      {scenario('listeners', 'Listener registrations', 'Same callback twice, unlisten each', async () => {
        const [, room] = await createRoom('listeners')
        const calls: string[] = []
        const a = () => void calls.push('a')
        const offFirst = room.onJoin(a)
        room.onJoin(() => void calls.push('b'))
        const offSecond = room.onJoin(a)
        const joinAndWait = async (expected: number) => {
          await room.join()
          await pollUntil(() => ({ done: calls.length >= expected }))
        }
        await joinAndWait(3)
        offFirst()
        await joinAndWait(5)
        offSecond()
        await joinAndWait(6)
        setResult(JSON.stringify({ calls }))
      })}

      {scenario(
        'retain-late',
        'Retained replay, once and in order',
        'Late subscribers get what is retained',
        async () => {
          const [roomId, pubView] = await createRoom('retain-late')
          // A GC pass would close `pubView`, and its members leaving with it delete what they retained.
          const releasePubView = keepReachable(pubView)
          const a = await pubView.join({ meta: { name: 'A' } })
          const b = await pubView.join({ meta: { name: 'B' } })
          await a.publishBinary(new Uint8Array([1]), { track: 'cam', retain: true })
          await b.publishBinary(new Uint8Array([2]), { track: 'cam', retain: true })
          await b.publishBinary(new Uint8Array([3]), { track: 'mic', retain: true })
          await a.publish('pinned', { retain: true })
          await b.leave()

          await onWatchRoomTrack(roomId, 'cam')
          const late = await onGetRoom(roomId)
          const text: string[] = []
          const frames: Array<[string | null, number, unknown]> = []
          late.subscribe((data) => text.push(data as string))
          late.subscribeBinary((data, info, from) => frames.push([info.track, data[0]!, from.meta.name]))
          await a.publish('live')
          await a.publishBinary(new Uint8Array([4]), { track: 'cam' })
          await pollUntil(async () => {
            const server = await onGetWatchedTrack(roomId)
            return { done: text.includes('live') && frames.length >= 2 && server.frames.includes(4) }
          })

          const later: string[] = []
          late.subscribe((data) => later.push(data as string))
          await a.publish('live-2')
          await pollUntil(async () => {
            const server = await onGetWatchedTrack(roomId)
            return {
              result: { text, frames, later, server },
              done: later.includes('live-2') && server.text.includes('live-2'),
            }
          })
          releasePubView()
        },
      )}

      {scenario('hidden', 'Hidden participants', 'Hidden member handed to one page', async () => {
        const [roomId, view] = await createRoom('hidden')
        const events: string[] = []
        view.onJoin((m) => events.push(`join:${m.meta.name}`))
        view.onLeave((m) => events.push(`leave:${m.meta.name}`))
        view.onParticipantUpdate((m) => events.push(`update:${m.meta.name}`))
        let empty = false
        view.onEmpty(() => (empty = true))
        const player = await view.join({ meta: { name: 'P' } })

        const handed = await onJoinHiddenAndHand(roomId)
        const handedEvents: string[] = []
        handed.room.onJoin((m) => handedEvents.push(`join:${m.meta.name}`))
        handed.room.onLeave((m) => handedEvents.push(`leave:${m.meta.name}`))
        const memberEvents: string[] = []
        handed.member!.onUpdate((meta) => memberEvents.push(`update:${meta.name}`))
        handed.member!.onLeave((cause) => memberEvents.push(`leave:${cause.type}`))
        const counts = [view.count, handed.room.count]

        await onHiddenSetMeta(roomId, 'Bot2')
        await pollUntil(() => ({ done: memberEvents.includes('update:Bot2') }))
        await player.leave()
        await pollUntil(() => ({ done: empty }))
        await onHiddenLeave(roomId)
        await pollUntil(() => ({ done: memberEvents.includes('leave:left') }))
        // Delivered after the hidden member's events, so they had their chance to reach `view`.
        await view.join({ meta: { name: 'Q' } })
        await pollUntil(() => ({ done: events.includes('join:Q') && handedEvents.includes('join:Q') }))

        const aloneId = `e2e-hidden-alone:${crypto.randomUUID()}`
        await onHiddenAfterPlayers(aloneId)
        await pollUntil(async () => {
          const alone = await onGetHiddenAfterPlayers(aloneId)
          return {
            result: { events, handedEvents, memberEvents, counts, empty, alone },
            done: alone.closed && alone.cause !== null,
          }
        })
      })}

      {scenario('proxy', 'Room methods as React reads them', 'Stable and detached methods', async () => {
        const [roomId, room] = await createRoom('proxy')
        const sameMethods = room.onChange === room.onChange && room.snapshot === room.snapshot
        const { join, snapshot } = room
        await join({ meta: { name: 'Detached' } })
        await pollUntil(() => ({ done: snapshot().count === 1 }))
        setLiveRoom(room)
        await onJoinAsServer(roomId, 'Bob')

        const closed = await onCloseRoomWithMember(`e2e-proxy-closed:${crypto.randomUUID()}`)
        const causes: unknown[] = []
        closed.member!.onLeave((cause) => causes.push(cause))
        setResult(
          JSON.stringify({
            sameMethods,
            detachedCount: snapshot().count,
            causes,
            closedIsClosed: closed.room.isClosed,
          }),
        )
      })}

      {scenario('errors', 'Background errors', 'Failing listeners and un-awaited writes', async () => {
        const [, room] = await createRoom('errors')
        room.subscribe(async () => {
          throw new Error('room-listener-bug: async subscribe')
        })
        const me = await room.join({ meta: { name: 'Me' } })
        await me.publish('hello')
        await me.leave()
        me.publish('after-leave')
        me.publish('after-leave', { coalesce: 'cursor' })
        me.publishBinary(new Uint8Array([1]))
        me.send(me.id, 'after-leave')
        const awaited = await me.publish('after-leave').then(
          () => null,
          (err: Error) => err.message,
        )
        const server = await onBackgroundErrors(`e2e-errors-server:${crypto.randomUUID()}`)
        setResult(JSON.stringify({ awaited, server }))
      })}

      {scenario('tracks', 'Named tracks and demand', 'Tracks reach who asked', async () => {
        const [roomId, pubView] = await createRoom('tracks')
        // A GC pass would close `pubView`, and its members leaving with it end the demand this reads.
        const releasePubView = keepReachable(pubView)
        const pub = await pubView.join({ meta: { name: 'Pub' }, selfDelivery: false })
        const other = await pubView.join({ meta: { name: 'Other' } })
        const demand: Array<[string | null, boolean]> = []
        pub.onDemand((track, wanted) => demand.push([track, wanted]))
        const otherDemand: Array<[string | null, boolean]> = []
        other.onDemand((track, wanted) => otherDemand.push([track, wanted]))
        // Pub's own side: its frames never come back to this view, so it wants nothing of Pub.
        pubView.subscribeBinary(() => {}, { track: 'own' })

        const tracks = Array.from({ length: 1_000 }, (_, i) => `t${i}`)
        const viewer = await onGetRoom(roomId)
        const remote = await getParticipantWhenJoined(viewer, pub.id)
        const frames: Record<string, number[]> = {}
        const offs = tracks.map((track) =>
          remote.subscribeBinary((data, info) => (frames[info.track!] ??= []).push(data[0]!), { track }),
        )
        const defaultTrack: number[] = []
        viewer.subscribeBinary((data) => defaultTrack.push(data[0]!), { track: null })
        await pollUntil(() => ({
          done: demand.some(([track]) => track === 't999') && demand.some(([t]) => t === null),
        }))

        await Promise.all([
          ...tracks.flatMap((track) => [1, 2].map((n) => pub.publishBinary(new Uint8Array([n]), { track }))),
          pub.publishBinary(new Uint8Array([7])),
        ])
        await pollUntil(() => ({
          done: tracks.every((track) => frames[track]?.length === 2) && defaultTrack.length > 0,
        }))
        offs[0]!()
        await pollUntil(() => ({ done: demand.some(([track, wanted]) => track === 't0' && !wanted) }))

        const many = await onGetRoom(roomId)
        for (let i = 0; i < 4_096; i++) many.subscribeBinary(() => {}, { track: `x${i}` })
        const tooMany = (() => {
          try {
            many.subscribeBinary(() => {}, { track: 'past' })
            return null
          } catch (err) {
            return (err as Error).message
          }
        })()
        setResult(
          JSON.stringify({
            inOrder: tracks.filter((track) => frames[track]!.join() === '1,2').length,
            defaultTrack,
            demand: demand.filter(([track]) => track === null || track === 'own' || track === 't0' || track === 't999'),
            wanted: new Set(demand.filter(([, wanted]) => wanted).map(([track]) => track)).size,
            otherOwn: otherDemand.filter(([track]) => track === 'own'),
            tooMany,
          }),
        )
        releasePubView()
      })}

      {scenario('away', 'Back from offline', 'What the room did while away', async () => {
        const [roomId, view] = await createRoom('away')
        const messages: string[] = []
        const leaves: string[] = []
        let closed = false
        view.subscribe((data) => messages.push(data as string))
        view.onLeave((member, cause) => leaves.push(`${member.meta.name}:${cause.type}`))
        view.onClose(() => (closed = true))
        const me = await view.join({ meta: { name: 'Me' } })
        let myCause: unknown = null
        me.onLeave((cause) => (myCause = cause))
        await onRoomWhileAway(roomId, me.id)
        await pollUntil(() => ({ done: view.count === 3 }))
        setResult(JSON.stringify({ roomId }))
        for (let poll = 0; poll < 300 && !(closed && myCause); poll++) await new Promise((r) => setTimeout(r, 100))
        setResult(
          JSON.stringify({
            roomId,
            messages,
            leaves: leaves.sort(),
            topic: (view.meta as { topic?: string }).topic ?? null,
            myCause,
            closed,
          }),
        )
      })}

      {scenario('overflow', 'Offline past bufferLimit', 'Let go with ChannelOverflowError', async () => {
        const [roomId, view] = await createRoom('overflow')
        const seen: string[] = []
        view.subscribe((data) => seen.push((data as string).trim()))
        let closed = false
        view.onClose(() => (closed = true))
        const me = await view.join({ meta: { name: 'Me' } })
        let myCause: unknown = null
        me.onLeave((cause) => (myCause = cause))
        await onRoomOverflow(roomId)
        await pollUntil(() => ({ done: seen.includes('before') }))
        setResult(JSON.stringify({ roomId }))
        for (let poll = 0; poll < 300 && !(closed && myCause); poll++) await new Promise((r) => setTimeout(r, 100))
        const leaves = await onGetRoomOverflow(roomId)
        setResult(JSON.stringify({ roomId, seen, myCause, closed, isClosed: view.isClosed, leaves }))
      })}

      {scenario('slow-link', 'Slow link', '640 KB each way', async () => {
        const [roomId, view] = await createRoom('slow-link')
        await onWatchRoom(roomId)
        const received: number[] = []
        view.subscribe((data) => received.push(Number.parseInt(data as string)))
        const me = await view.join({ meta: { name: 'Me' }, selfDelivery: false })
        await onRoomBurst(roomId, 40, 16_000)
        await pollUntil(() => ({ done: received.length >= 40 }))
        await Promise.all(Array.from({ length: 40 }, (_, n) => me.publish(String(n).padEnd(16_000))))
        // The server's view hears the speaker's 40 and the page's 40.
        let watched = 0
        for (let poll = 0; poll < 100 && watched < 80; poll++) {
          watched = await onGetWatchedCount(roomId)
          if (watched < 80) await new Promise((r) => setTimeout(r, 200))
        }
        setResult(JSON.stringify({ received, watched }))
      })}

      {scenario('release', 'What the server lets go', 'Unheld view, left member, closed room', async () => {
        const roomId = `e2e-release:${crypto.randomUUID()}`
        await onRoomRelease(roomId)
        setResult(JSON.stringify({ roomId }))
      })}

      {scenario('tail-order', 'Tail: order and cap', 'Held text flushed in order', async () => {
        const [roomId, owner] = await createRoom('tail-order')
        const me = await owner.join({ meta: { name: 'Src' } })
        const tailed = await onGetRoomTail(roomId)
        const announcements: string[] = []
        tailed.onAnnounce((data) => announcements.push(data as string))
        await me.publish('a')
        await onAnnounce(roomId, 'ann')
        await pollUntil(() => ({ done: announcements.length > 0 }))
        await me.publish('b')
        const received: string[] = []
        tailed.subscribe((data) => received.push(data as string))
        await pollUntil(() => ({ done: received.includes('b') }))
        const flushed = [...received]

        const capped = await onGetRoomTail(roomId)
        await Promise.all(Array.from({ length: 300 }, (_, n) => me.publish(`n${n}`)))
        const kept: string[] = []
        capped.subscribe((data) => kept.push(data as string))
        await pollUntil(() => ({ done: kept.includes('n299') }))
        setResult(JSON.stringify({ received: flushed, announcements, kept }))
      })}

      {scenario('tail-lease', null, 'Subscribe to a tail after its lease', async () => {
        const [roomId, owner] = await createRoom('tail-lease')
        const me = await owner.join({ meta: { name: 'Src' } })
        const tailed = await onGetRoomTail(roomId)
        await me.publish('held')
        setResult(JSON.stringify({ waiting: true }))
        await new Promise((resolve) => setTimeout(resolve, 61_000))
        const received: string[] = []
        tailed.subscribe((data) => received.push(data as string))
        await me.setAttributes({}) // same-channel ack orders the subscription before the live publish
        await me.publish('live')
        await pollUntil(() => ({ result: { received }, done: received.includes('live') }))
      })}

      {scenario('hiccups', 'Store hiccups', 'A store write fails once', async () => {
        const settle = (promise: Promise<unknown>) =>
          promise.then(
            () => 'ok',
            (err: Error) => err.message,
          )

        const [leaveRoomId, leaveView] = await createRoom('hiccup-leave')
        const leaves: string[] = []
        leaveView.onLeave((member, cause) => leaves.push(`${member.meta.name}:${cause.type}`))
        const me = await leaveView.join({ meta: { name: 'Me' } })
        // The leave's write of the member record.
        await onFailBackend({ op: 'compareExchangeCells', roomId: leaveRoomId, key: 'm:' })
        const firstLeave = await settle(me.leave())
        const publishAfter = await settle(me.publish('still-here'))
        const secondLeave = await settle(me.leave())
        await pollUntil(() => ({ done: leaveView.count === 0 }))

        const [joinRoomId, joinView] = await createRoom('hiccup-join')
        // The join's announcement, after its member record committed.
        await onFailBackend({ op: 'commitLane', roomId: joinRoomId, key: 'm:' })
        const firstJoin = await settle(joinView.join({ meta: { name: 'First' } }))
        await joinView.join({ meta: { name: 'Second' } })
        await pollUntil(() => ({ done: joinView.count === 1 }))
        const listed = await onListParticipantNames(joinRoomId)

        const [laneRoomId, laneView] = await createRoom('hiccup-lane')
        const got: string[] = []
        laneView.subscribe((data) => got.push(data as string))
        const speaker = await laneView.join({ meta: { name: 'Speaker' } })
        await speaker.publish('before')
        await pollUntil(() => ({ done: got.includes('before') }))
        const ended = await onEndBackendSubscriptions(laneRoomId)
        await speaker.publish('after')
        await pollUntil(() => ({ done: got.includes('after') }))

        setResult(
          JSON.stringify({
            leave: { firstLeave, publishAfter, secondLeave, leaves, count: leaveView.count },
            join: { firstJoin, count: joinView.count, listed },
            lane: { ended: ended > 0, got },
          }),
        )
      })}

      {scenario('hiccups-heartbeat', null, 'A failed write healed by the heartbeat', async () => {
        const viewRoomId = `e2e-hiccup-view:${crypto.randomUUID()}`
        // Past the 30 s heartbeat, which restores what the store failed to keep.
        await onCreateRoomClosingAfter(viewRoomId, 35_000)
        // The write of this page's view, which holds the room.
        await onFailBackend({ op: 'compareExchangeCells', roomId: viewRoomId, key: 'v:' })
        const view = await onGetRoom(viewRoomId)

        const [joinRoomId, joinView] = await createRoom('hiccup-join')
        await onFailBackend({ op: 'commitLane', roomId: joinRoomId, key: 'm:' })
        const firstJoin = await joinView.join({ meta: { name: 'First' } }).then(
          () => 'ok',
          (err: Error) => err.message,
        )
        await joinView.join({ meta: { name: 'Second' } })
        setResult(JSON.stringify({ waiting: true }))

        await new Promise((resolve) => setTimeout(resolve, 40_000))
        setResult(
          JSON.stringify({
            viewRoom: { listed: await onListRooms(viewRoomId), isClosed: view.isClosed },
            joinRoom: { firstJoin, listed: await onListParticipantNames(joinRoomId), count: joinView.count },
          }),
        )
      })}
    </div>
  )
}
