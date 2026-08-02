export { Room }

import React, { useEffect, useState } from 'react'
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
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
} from './Room.telefunc'
import { roomScenario, type RoomScenarioId } from './Room.scenarios'

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

function Room() {
  const [result, setResult] = useState<string>('')
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

  const scenario = (id: RoomScenarioId, heading: string | null, label: string, run: () => Promise<void>) => (
    <>
      {heading && <h2>{heading}</h2>}
      <button
        id={roomScenario(id).selector.slice(1)}
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

        // Read retained state only after subscription readiness; keep its membership-owning publisher referenced.
        const pubView = await onGetRoom(roomId)
        const author = await pubView.join({ meta: { name: 'Author' } })
        await author.publish({ text: 'pinned' }, { retain: true })

        const late = await onGetRoom(roomId)
        const received: string[] = []
        late.subscribe((data) => received.push((data as { text: string }).text))

        await pollUntil(() => ({ result: { received }, done: received.length >= 1 }))
      })}

      {scenario('participant', 'Server-Joined Participant', 'Server-side join + publish', async () => {
        const [roomId, observer] = await createRoom('participant')
        const received: Array<{ text: unknown; from: unknown }> = []
        observer.subscribe((data, _info, from) => {
          received.push({ text: (data as { text: string }).text, from: from.meta.name })
        })

        // Joined server-side — arrives as a standalone participant with its own stub channel.
        const me = await onJoinAsServer(roomId, 'Bob')
        await me.publish({ text: 'from-bob' })
        await me.setMeta({ name: 'Bobby' })
        // Live handle (meta stays fresh); retry — the join event may still be in flight.
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

        // selfDelivery defaults to true — our own frames come back to us.
        for (let i = 0; i < 3; i++) {
          await me.publishBinary(new Uint8Array(64).fill(i + 1))
        }
        // Named track + per-frame meta — mic/camera/screen multiplex over one member lane;
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

        // One key emits only the first and latest values: [1, 5].
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

        await me.setAttributes({ score: 2 }) // merge — name is untouched
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
      })}

      {scenario('tail', 'Tail (single-call history)', 'Tail holds pre-subscribe messages', async () => {
        const [roomId, owner] = await createRoom('tail')
        const me = await owner.join({ meta: { name: 'Src' } })

        const tailed = await onGetRoomTail(roomId)
        // Publish after tail creation but before subscribe; it must be replayed.
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
          // `theirs` arriving proves the publish propagated — so `mine` staying empty is meaningful.
          return { result: { mine, theirs, selfDelivery: me.selfDelivery }, done: theirs.includes('hi') }
        })
      })}

      {scenario(
        'self-server',
        'selfDelivery: false (server co-return + client join on one stub)',
        'Co-return suppressed, client join delivered',
        async () => {
          const roomId = await createRoomId('self-server')
          await onWatchRoom(roomId) // a different (server-side) client — receives everything

          // The co-returned server join suppresses its own publish on this room view.
          const { room, me } = await onJoinRoomAsServerSelf(roomId, 'Solo')
          const mine: string[] = []
          room.subscribe((data) => mine.push(data as string))
          await me.publish('from-me')

          // A client join on the same stub keeps default self-delivery.
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
        const same = await onGetOrCreateRoom(roomId) // idempotent — returns the existing room
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

    </div>
  )
}
