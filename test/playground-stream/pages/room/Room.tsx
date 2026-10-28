export { Room }

import React, { useEffect, useState, useSyncExternalStore } from 'react'
import { abort, close } from 'telefunc/client'
import { keepReachable } from '../keepReachable'
import {
  onCreateRoom,
  onGetRoom,
  onGetRoomTail,
  onGetOrCreateRoom,
  onGetOrCreateRoomTail,
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
  onHoldRetainedReads,
  onHeldRetainedReads,
  onReleaseRetainedReads,
  onWatchEveryFrame,
  onGetEveryFrame,
  onJoinHiddenAndHand,
  onHiddenSetMeta,
  onHiddenLeave,
  onHiddenAfterPlayers,
  onGetHiddenAfterPlayers,
  onCloseRoomWithMember,
  onBackgroundErrors,
  onRoomWhileAway,
  onRoomMetaWhileAway,
  onRoomLeaveWhileJoining,
  onRoomOverflow,
  onParticipantOverflow,
  onGetRoomOverflow,
  onRoomBurst,
  onGetWatchedCount,
  onRoomRelease,
  onFailBackend,
  onEndBackendSubscriptions,
  onListParticipantNames,
  onCreateRoomClosingAfter,
  onSeedRoom,
  onSeedChurn,
  onPing,
  onAnnounce,
  onSystemSend,
  onUpdateRoom,
  onListRooms,
  onKick,
  onKickByIdentity,
  onCloseRoom,
  onWatchRoomBinary,
  onGetWatchedBinary,
  onPublishBufferAsServer,
  onSendReusingObjects,
  onPublishRightAfterSubscribe,
  onOpenSlowGuardRoom,
  onConcurrentMetaWrites,
  onReplaceMetaInLoop,
  onStopReplacingMeta,
  onReadMetaAcrossWrite,
  onReadMetaAcrossDrop,
  onReadSplitMetaAcrossMerge,
  onOpenRecordingGuardRoom,
  onGetGuardCalls,
  onOpenSlowJoinRoom,
  onGetSlowJoins,
  onGetSlowJoinRoom,
  onCloseWhenEmpty,
  onCreateRoomWith,
  onCreateRoomHeld,
  onJoinAndLeaveAsServer,
  onIsOpen,
  onListAsListed,
  onListWhileClosing,
  onRecreate,
  onJoinAnswering,
  onSaveReply,
  onSeedAndChurn,
  onStopChurn,
  onPublishWhileEstablishing,
} from './Room.telefunc'

type PollResult = { done: boolean; result?: unknown }
/** The members one room view joins, and the named tracks one member publishes on (docs/pages/room, Reference). */
const ROOM_VIEW_LIMIT = 4096
/** 80 s of polls: past the 60 s reconnectTimeout the server waits for a closed tab on SSE. */
const CLOSED_TAB_POLLS = 400

async function createRoomId(label: string): Promise<string> {
  const roomId = `e2e-${label}:${crypto.randomUUID()}`
  await onCreateRoom(roomId)
  return roomId
}

async function createRoom(label: string) {
  const roomId = await createRoomId(label)
  return [roomId, await onGetRoom(roomId)] as const
}

let selfCloseBase = ''

type RoomView = Awaited<ReturnType<typeof onGetRoom>>

/** The join the `away-join` card starts when its second button is clicked. */
let startAwayJoin: (() => void) | null = null
/** The check the `member-keeps-room` card runs when its second button is clicked. */
let checkKeptMember: (() => void) | null = null
/** The check the `members-let-go` card runs when its second button is clicked. */
let checkLetGo: (() => void) | null = null
/** The chat the `ack-dm-close` card closes when its second button is clicked. */
let closeChat: (() => void) | null = null
/** The room view the `ack-dm-close-view` card closes when its second button is clicked. */
let closeViewChat: (() => void) | null = null
/** The call the `ack-dm-hang-up` card hangs up when its second button is clicked. */
let hangUp: (() => void) | null = null

const replyOf = (sent: Promise<{ response: unknown }>) =>
  sent.then(
    (receipt) => receipt.response,
    (err: Error) => err.message,
  )

/** A reply that links back to its thread, which can't be sent. */
function threadedReply() {
  const thread: { replies: unknown[] } = { replies: [] }
  const reply = { text: 'see thread', thread }
  thread.replies.push(reply)
  return reply
}

/** Joins through a room the page doesn't keep, and lets go of the member too, without leaving. */
async function joinAndLetGo(roomId: string, leaves: string[]): Promise<void> {
  const me = await (await onGetRoom(roomId)).join({ meta: { name: 'Dropped' } })
  me.onLeave((cause) => leaves.push(cause.type))
}

/** Joins the room named in the URL as `Mallory`, then sends what no client library sends, as a hostile page can. */
async function joinAndSendMalformed(kind: 'request' | 'publish' | 'reply'): Promise<void> {
  const roomId = new URLSearchParams(location.search).get('roomId')!
  const room = await onGetRoom(roomId)
  keepReachable(room)
  // The room's own channel, past the API that checks what goes on it.
  const { _end: end } = room as unknown as {
    _end: {
      send(data: unknown, options: { ack: true }): Promise<unknown>
      publish(data: unknown): Promise<unknown>
      _sendReply(reply: Promise<unknown>, failure: unknown): void
    }
  }
  const me = await room.join({ meta: { name: 'Mallory' } })
  if (kind === 'reply') {
    const sendReply = end._sendReply.bind(end)
    end._sendReply = (reply, failure) =>
      sendReply(
        reply.then((dmReply) => ({ ...(dmReply as object), reply: 'bogus' })),
        failure,
      )
    me.listen(() => 'hi')
    return
  }
  await (kind === 'request' ? end.send({ __r: 'req-bogus' }, { ack: true }) : end.publish({ __r: 'bogus' })).catch(
    () => {},
  )
}

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
  const pollUntil = async (render: () => PollResult | Promise<PollResult>, polls = 50) => {
    for (let poll = 0; poll < polls; poll++) {
      const { done, result } = await render()
      if (result !== undefined) setResult(JSON.stringify(result))
      if (done) break
      await new Promise((resolve) => setTimeout(resolve, 200))
    }
  }

  const getParticipantWhenJoined = async (room: RoomView, id: string) => {
    await pollUntil(async () => ({ done: (await room.getParticipant(id)) !== null }))
    const participant = await room.getParticipant(id)
    if (!participant) throw new Error(`Participant did not join within the test horizon: ${id}`)
    return participant
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

      {scenario('own-bytes', 'Bytes of its own', 'Listeners that write into or transfer their bytes', async () => {
        const roomId = await createRoomId('own-bytes')
        const room = await onWatchRoomBinary(roomId)
        const me = await room.join({ meta: { name: 'Pub' } })
        // The first listener detaches its buffer, as handing it to a worker does.
        room.subscribeBinary((data) => structuredClone(data.buffer, { transfer: [data.buffer] }))
        const seen: number[][] = []
        room.subscribeBinary((data) => seen.push([...data]))

        await me.publishBinary(new Uint8Array([1, 2, 3, 4]))
        await onPublishBufferAsServer(roomId, [5, 6, 7, 8])
        const late = await onGetRoom(roomId)
        const lateSeen: number[][] = []
        late.subscribeBinary((data) => lateSeen.push([...data]))

        await pollUntil(async () => {
          const serverSeen = await onGetWatchedBinary(roomId)
          return {
            result: { seen, serverSeen, lateSeen },
            done: seen.length >= 2 && serverSeen.length >= 2 && lateSeen.length >= 1,
          }
        })
      })}

      {scenario('at-call', 'Taken at the call', 'Writes whose caller reuses its object', async () => {
        const [roomId, room] = await createRoom('at-call')
        const me = await room.join({ meta: { name: 'Me' } })
        const messages: Array<{ data: unknown; from: string }> = []
        room.subscribe((data, _info, from) => messages.push({ data, from: from.meta.name as string }))
        const frames: number[][] = []
        room.subscribeBinary((data) => frames.push([...data]))
        const announcements: unknown[] = []
        room.onAnnounce((data) => announcements.push(data))
        const dms: unknown[] = []
        me.listen((data) => dms.push(data))
        await me.setAttributes({}) // same-channel ack orders the declarations before the server's writes

        // The second publish waits behind the first on its coalesce key.
        const first = { n: 10 }
        const second = { n: 11 }
        const coalesced = Promise.all([me.publish(first, { coalesce: 'c' }), me.publish(second, { coalesce: 'c' })])
        second.n = 12
        await coalesced
        const reuser = await getParticipantWhenJoined(room, await onSendReusingObjects(roomId, me.id))

        await pollUntil(() => {
          const state = {
            messages,
            frames,
            announcements,
            dms,
            reuserTags: (reuser.meta.tags as string[] | undefined) ?? null,
            roomTags: (room.meta.tags as string[] | undefined) ?? null,
          }
          return {
            result: state,
            done:
              messages.length >= 3 &&
              frames.length >= 1 &&
              announcements.length >= 1 &&
              dms.length >= 2 &&
              state.reuserTags?.[0] === 'b' &&
              state.roomTags !== null,
          }
        })
      })}

      {scenario('right-after', 'Publish right after subscribe', 'Subscribe, then publish at once', async () => {
        const roomId = await createRoomId('right-after')
        await onPublishRightAfterSubscribe(roomId)
        const room = await onGetRoom(roomId)
        const me = await room.join({ meta: { name: 'Page' } })
        const seen: unknown[] = []
        room.subscribe((data, _info, from) => {
          if (from.id === me.id) seen.push(data)
        })
        room.subscribeBinary((data, _info, from) => {
          if (from.id === me.id) seen.push([...data])
        })
        await Promise.all([me.publish('p1'), me.publishBinary(new Uint8Array([2])), me.publish('p2')])

        await pollUntil(async () => {
          const serverSeen = await onGetWatched(roomId)
          return { result: { seen, serverSeen }, done: seen.length >= 3 && serverSeen.length >= 4 }
        })
      })}

      {scenario('call-order', 'Publishes in call order', 'Publish behind a slow guard', async () => {
        const roomId = await createRoomId('call-order')
        const room = await onOpenSlowGuardRoom(roomId)
        const texts: number[] = []
        room.subscribe((data) => texts.push(data as number))
        const frames: number[] = []
        room.subscribeBinary((data) => frames.push(data[0]!))
        const me = await room.join({ meta: { name: 'Fast' } })

        const numbers = Array.from({ length: 20 }, (_, i) => i)
        await Promise.all(numbers.flatMap((n) => [me.publish(n), me.publishBinary(new Uint8Array([n]))]))

        await pollUntil(() => ({ result: { texts, frames }, done: texts.length >= 20 && frames.length >= 20 }))
      })}

      {scenario('meta-race', 'Concurrent meta writes', 'Many writers at once', async () => {
        const [roomId, room] = await createRoom('meta-race')
        const other = await onGetRoom(roomId)
        const stored = await onConcurrentMetaWrites(roomId)
        const member = await getParticipantWhenJoined(room, stored.memberId)
        const otherMember = await getParticipantWhenJoined(other, stored.memberId)

        await pollUntil(() => {
          const views = {
            roomMeta: Object.entries(room.meta),
            otherRoomMeta: Object.entries(other.meta),
            memberMeta: Object.entries(member.meta),
            otherMemberMeta: Object.entries(otherMember.meta),
          }
          const settled =
            JSON.stringify(views.roomMeta) === JSON.stringify(stored.roomMeta) &&
            JSON.stringify(views.otherRoomMeta) === JSON.stringify(stored.roomMeta) &&
            JSON.stringify(views.memberMeta) === JSON.stringify(stored.memberMeta) &&
            JSON.stringify(views.otherMemberMeta) === JSON.stringify(stored.memberMeta)
          return { result: { stored, ...views }, done: settled }
        })
      })}

      {scenario('meta-replace', 'Reads while meta is replaced', 'Read a big meta while it is replaced', async () => {
        const roomId = await createRoomId('meta-replace')
        const memberId = await onReplaceMetaInLoop(roomId)
        // Which meta a read got: `a…` or `b…` whole, or what it saw otherwise.
        const which = (meta: Record<string, unknown>) => {
          const keys = Object.keys(meta).filter((key) => key !== 'name')
          const prefix = keys[0]?.[0]
          const whole = keys.length === 1500 && keys.every((key, i) => key === `${prefix}${i}` && meta[key] === i)
          return whole ? prefix : `${keys.length} keys from ${keys[0]} to ${keys.at(-1)}`
        }
        const reads: Array<string | undefined> = []
        for (let i = 0; i < 6; i++) {
          const room = await onGetRoom(roomId)
          const member = await room.getParticipant(memberId)
          reads.push(which(room.meta), which(member?.meta ?? {}))
        }
        await onStopReplacingMeta(roomId)
        setResult(JSON.stringify({ reads }))
      })}

      {scenario('meta-across-write', null, "Read a member's big meta across a write", async () => {
        const reads: Record<string, string> = {}
        for (const [write, gen] of [
          ['replace with no keys', 1],
          ['merge', 1],
          ['replace', 9],
          ['replace', 99],
        ] as const) {
          reads[`${write} from ${gen}`] = await onReadMetaAcrossWrite(
            await createRoomId('meta-across-write'),
            write,
            gen,
          )
          setResult(JSON.stringify(reads))
        }
      })}

      {scenario('meta-across-drop', null, "Read a member's big meta across its drop", async () => {
        setResult(JSON.stringify({ read: await onReadMetaAcrossDrop(await createRoomId('meta-across-drop')) }))
      })}

      {scenario('split-meta-across-merge', null, "Read a member's split meta across a merge", async () => {
        const roomId = await createRoomId('split-meta-across-merge')
        setResult(JSON.stringify({ read: await onReadSplitMetaAcrossMerge(roomId) }))
      })}

      {scenario('members-cap', 'Limits', 'Join past the members one view joins', async () => {
        const roomId = await createRoomId('members-cap')
        const view = await onGetRoom(roomId)
        const releaseView = keepReachable(view)
        const joins = await Promise.allSettled(
          Array.from({ length: ROOM_VIEW_LIMIT + 1 }, (_, i) => view.join({ meta: { name: `m${i}` } })),
        )
        const joined = joins.flatMap((join) => (join.status === 'fulfilled' ? [join.value] : []))
        const errors = joins.flatMap((join) => (join.status === 'rejected' ? [(join.reason as Error).message] : []))
        // The members it joined keep working.
        await joined[0]!.setAttributes({ still: true })
        const count = (await onListParticipantNames(roomId)).length
        setResult(JSON.stringify({ joined: joined.length, errors, count, still: joined[0]!.meta.still }))
        releaseView()
      })}

      {scenario('tracks-cap', null, 'Publish past the named tracks one member publishes on', async () => {
        const roomId = await createRoomId('tracks-cap')
        const view = await onGetRoom(roomId)
        const releaseView = keepReachable(view)
        const me = await view.join({ meta: { name: 'Tracks' } })
        const publishes = await Promise.allSettled(
          Array.from({ length: ROOM_VIEW_LIMIT + 1 }, (_, i) =>
            me.publishBinary(new Uint8Array([1]), { track: `t${i}`, retain: true }),
          ),
        )
        const errors = publishes.flatMap((publish) =>
          publish.status === 'rejected' ? [(publish.reason as Error).message] : [],
        )
        // Its tracks and the default track keep working, and a late view gets each named track's retained frame.
        const kept = publishes.findIndex((publish) => publish.status === 'fulfilled')
        await me.publishBinary(new Uint8Array([2]), { track: `t${kept}` })
        await me.publishBinary(new Uint8Array([3]))
        const late = await onGetRoom(roomId)
        const releaseLate = keepReachable(late)
        const retained = new Set<string | null>()
        late.subscribeBinary((_data, info) => retained.add(info.track))
        await pollUntil(() => ({
          result: { accepted: publishes.length - errors.length, errors, retained: retained.size },
          done: retained.size >= ROOM_VIEW_LIMIT,
        }))
        releaseLate()
        releaseView()
      })}

      {scenario('sender', 'Rename during a guard', 'Rename while a publish waits on its guard', async () => {
        const roomId = await createRoomId('sender')
        const room = await onOpenSlowGuardRoom(roomId)
        const names: unknown[] = []
        room.subscribe((_data, _info, from) => names.push(from.meta.name))
        const me = await room.join({ meta: { name: 'A' } })
        // A view whose roster may still be on its way.
        const other = await onGetRoom(roomId)
        const otherNames: unknown[] = []
        other.subscribe((_data, _info, from) => otherNames.push(from.meta.name))

        const published = me.publish(0) // its guard waits 50 ms
        await me.setMeta({ name: 'B' })
        await published
        const view = await getParticipantWhenJoined(room, me.id)
        const otherView = await getParticipantWhenJoined(other, me.id)

        await pollUntil(() => {
          const state = { names, otherNames, viewName: view.meta.name, otherViewName: otherView.meta.name }
          return {
            result: state,
            done: names.length >= 1 && otherNames.length >= 1 && state.viewName === 'B' && state.otherViewName === 'B',
          }
        })
      })}

      {scenario('guard-calls', 'What guards are handed', 'Guarded join, send, publish and leave', async () => {
        const roomId = await createRoomId('guard-calls')
        const room = await onOpenRecordingGuardRoom(roomId)
        const me = await room.join({ meta: { name: 'G' } })
        const peer = await room.join({ meta: { name: 'P' } })
        await me.send(peer.id, 'hi')
        const binaryError = await me.publishBinary(new Uint8Array([1])).then(
          () => null,
          (err: Error) => err.message,
        )
        const afterHookAck = await me.publish('after').then(
          (ack) => ack.seq > 0,
          (err: Error) => err.message,
        )
        // The second publish waits behind the first's guard while its member leaves.
        const slow = me.publish('slow')
        const queued = me.publish('queued')
        await me.leave()
        await Promise.allSettled([slow, queued])
        setResult(JSON.stringify({ binaryError, afterHookAck, calls: await onGetGuardCalls(roomId) }))
      })}

      {scenario(
        'leave-causes',
        'Leave causes',
        'Leave with acknowledged DMs pending, and a tab that closes',
        async () => {
          const [roomId, room] = await createRoom('leave-causes')
          const causes: Array<{ name: unknown; type: string }> = []
          room.onLeave((member, cause) => causes.push({ name: member.meta.name, type: cause.type }))
          const a = await room.join({ meta: { name: 'A' } })
          const b = await room.join({ meta: { name: 'B' } })
          const c = await room.join({ meta: { name: 'C' } })
          // A and C take each DM and never answer.
          a.listen(() => new Promise(() => {}))
          c.listen(() => new Promise(() => {}))
          const outcome = (sent: Promise<unknown>) =>
            sent.then(
              () => 'resolved',
              () => 'rejected',
            )

          const start = performance.now()
          const toLeaving = b.send(a.id, 'ping', { ack: true })
          await a.leave()
          const recipientLeft = await outcome(toLeaving)
          const fromLeaving = b.send(c.id, 'ping', { ack: true })
          await b.leave()
          const senderLeft = await outcome(fromLeaving)
          const seconds = (performance.now() - start) / 1000

          // The test joins `Closer` from another tab, then closes it; on SSE the server waits out reconnectTimeout (60 s).
          await pollUntil(
            () => ({
              result: { roomId, recipientLeft, senderLeft, seconds, causes },
              done: causes.some((cause) => cause.name === 'Closer'),
            }),
            CLOSED_TAB_POLLS,
          )
        },
      )}

      {scenario('connection', 'Connection', "Watch another tab's member drop, come back and close", async () => {
        const [roomId, room] = await createRoom('connection')
        await room.join({ meta: { name: 'Watcher' } })
        const events: string[] = []
        room.onJoin((member) => {
          if (member.meta.name !== 'Closer') return
          member.onDisconnect(() => events.push(`disconnect:${member.isConnected}`))
          member.onReconnect(() => events.push(`reconnect:${member.isConnected}`))
          member.onLeave((cause) => events.push(`leave:${cause.type}`))
        })
        const closerConnected = () =>
          room.snapshot().participants.find((member) => member.meta.name === 'Closer')?.isConnected ?? null
        // The test joins `Closer` from another tab, takes that tab offline and back, then offline and closes it.
        await pollUntil(
          () => ({
            result: { roomId, events, closerConnected: closerConnected() },
            done: events.includes('leave:disconnected'),
          }),
          2 * CLOSED_TAB_POLLS,
        )
      })}

      {scenario('join-only', null, 'Join the room named in the URL', async () => {
        const roomId = new URLSearchParams(location.search).get('roomId')!
        const room = await onGetRoom(roomId)
        keepReachable(room)
        await room.join({ meta: { name: 'Closer' } })
        setResult(JSON.stringify('joined'))
      })}

      {scenario('malformed', 'Malformed requests', 'Join, and say goodbye to whoever leaves', async () => {
        const [roomId, room] = await createRoom('malformed')
        const events: string[] = []
        const received: unknown[] = []
        let dm: string | null = null
        room.subscribe((data) => received.push(data))
        room.onJoin((member) => {
          events.push(`join:${member.meta.name}`)
          if (member.meta.name !== 'Mallory') return
          void me.send(member.id, 'hello?', { ack: true }).then(
            () => (dm = 'answered'),
            () => (dm = 'rejected'),
          )
        })
        const me = await room.join({ meta: { name: 'Witness' } })
        room.onLeave((member, cause) => {
          events.push(`leave:${member.meta.name}:${cause.type}`)
          void me.publish(`bye ${member.meta.name}`)
        })
        // The test joins `Mallory` from another tab, which then sends a malformed request, publish or DM reply.
        await pollUntil(
          () => ({ result: { roomId, events, received, dm }, done: received.length > 0 && dm !== null }),
          150,
        )
      })}

      {scenario('malformed-request', null, 'Join the room named in the URL, then send a malformed request', () =>
        joinAndSendMalformed('request'),
      )}

      {scenario('malformed-publish', null, 'Join the room named in the URL, then send a malformed publish', () =>
        joinAndSendMalformed('publish'),
      )}

      {scenario('malformed-reply', null, 'Join the room named in the URL, then reply to a DM malformed', () =>
        joinAndSendMalformed('reply'),
      )}

      {scenario(
        'own-meta-race',
        'Own meta under concurrent writes',
        'Unawaited setMeta() and setAttributes()',
        async () => {
          const [roomId, room] = await createRoom('own-meta-race')
          const me = await room.join({ meta: { name: 'Mine', round: -1 } })
          const other = await onGetRoom(roomId)
          const remote = await getParticipantWhenJoined(other, me.id)
          await Promise.all(
            Array.from({ length: 20 }, (_, i) =>
              i % 5 === 0 ? me.setMeta({ name: 'Mine', round: i }) : me.setAttributes({ [`m${i}`]: i }),
            ),
          )
          await pollUntil(() => {
            const state = { mine: Object.entries(me.meta), remote: Object.entries(remote.meta) }
            return { result: state, done: JSON.stringify(state.mine) === JSON.stringify(state.remote) }
          })
        },
      )}

      {scenario('join-races', 'Join races', 'A closed tab during a slow join', async () => {
        const roomId = await createRoomId('join-races')
        const room = await onOpenSlowJoinRoom(roomId)
        const events: string[] = []
        room.onJoin((member) => events.push(`join:${member.meta.name}`))
        room.onLeave((member, cause) => events.push(`leave:${member.meta.name}:${cause.type}`))

        // The test joins `Closer` slowly from another tab and closes the tab during the wait.
        // On SSE the server holds the closed tab for reconnectTimeout (60 s), so `Closer` may join, then leave.
        let settledAt: number | null = null
        await pollUntil(async () => {
          const joins = await onGetSlowJoins(roomId)
          if (settledAt === null && joins.done.includes('Closer')) settledAt = performance.now()
          const serverCount = (await onGetRoom(roomId)).count
          return {
            result: {
              roomId,
              closerJoining: joins.entered.includes('Closer'),
              events,
              count: room.count,
              serverCount,
              settled: settledAt !== null,
            },
            // A member admitted after its tab closed would show within a second of the guard's end.
            done: settledAt !== null && performance.now() - settledAt > 1000 && room.count === 0 && serverCount === 0,
          }
        }, CLOSED_TAB_POLLS)
      })}

      {scenario('join-slowly', null, 'Join the room named in the URL, slowly', async () => {
        const roomId = new URLSearchParams(location.search).get('roomId')!
        const room = await onGetSlowJoinRoom(roomId)
        keepReachable(room)
        await room.join({ meta: { name: 'Closer', slow: true } })
      })}

      {scenario(
        'close-handles',
        'Close ends every handle',
        'Leave and kick into onEmpty, then Room.close()',
        async () => {
          const outcome = (done: Promise<unknown>) =>
            done.then(
              () => 'resolved',
              (err: Error) => err.name,
            )

          // The last member's leave() empties the room, and onEmpty closes it.
          const [leftId, left] = await createRoom('close-left')
          await onCloseWhenEmpty(leftId)
          const leaver = await left.join({ meta: { name: 'Leaver' } })
          const leave = await outcome(leaver.leave())

          // The last member's kick empties the room, and onEmpty closes it.
          const [kickedId, kicked] = await createRoom('close-kicked')
          await onCloseWhenEmpty(kickedId)
          const kickedMember = await kicked.join({ meta: { name: 'Kicked' } })
          const kick = await outcome(onKick(kickedId, kickedMember.id))

          // Room.close() with a member in: what its leave callback sees, an unsubscribe in onClose, an onClose added after.
          const [closedId, closed] = await createRoom('close-closed')
          const member = await closed.join({ meta: { name: 'Member' } })
          let atLeave: { cause: string; isClosed: boolean; count: number } | null = null
          member.onLeave((cause) => (atLeave = { cause: cause.type, isClosed: closed.isClosed, count: closed.count }))
          const unsubscribe = closed.subscribe(() => {})
          let unsubscribeInOnClose: string | null = null
          closed.onClose(() => {
            try {
              unsubscribe()
              unsubscribeInOnClose = 'returned'
            } catch (err) {
              unsubscribeInOnClose = (err as Error).name
            }
          })
          const close = await outcome(onCloseRoom(closedId))
          await pollUntil(() => ({ done: closed.isClosed }))
          let lateOnClose = false
          closed.onClose(() => (lateOnClose = true))

          await pollUntil(() => ({
            result: {
              leave,
              kick,
              close,
              atLeave,
              unsubscribeInOnClose,
              lateOnClose,
              isClosed: [left.isClosed, kicked.isClosed],
            },
            done: atLeave !== null && unsubscribeInOnClose !== null && lateOnClose && left.isClosed && kicked.isClosed,
          }))
        },
      )}

      {scenario('self-close', 'Rooms that close by themselves', 'Short timeouts', async () => {
        const base = `e2e-self-close:${crypto.randomUUID()}`
        selfCloseBase = base
        const ids = {
          empty: `${base}:empty`,
          departed: `${base}:departed`,
          held: `${base}:held`,
          never: `${base}:never`,
        }
        await onCreateRoomWith(ids.empty, { emptyTimeout: 1000 })
        await onCreateRoomWith(ids.departed, { emptyTimeout: 60_000, departureTimeout: 1000 })
        await onCreateRoomWith(ids.never, { emptyTimeout: Infinity, departureTimeout: Infinity })
        await onJoinAndLeaveAsServer(ids.departed)
        const isOpen = async () => ({
          empty: await onIsOpen(ids.empty),
          departed: await onIsOpen(ids.departed),
          held: await onIsOpen(ids.held),
          never: await onIsOpen(ids.never),
        })

        // The test creates `held` from another tab, which keeps the view it was returned.
        // Once the two closed, the held one and the one that never closes have outlived their 1 s by 1.5 s more.
        let closedAt: number | null = null
        await pollUntil(async () => {
          const open = await isOpen()
          if (closedAt === null && open.held && !open.empty && !open.departed) closedAt = performance.now()
          const outlived = closedAt !== null && performance.now() - closedAt > 1500
          return { result: { base, open, outlived }, done: outlived }
        })
      })}

      {scenario('hold-room', null, 'Create the room named in the URL and keep its view', async () => {
        const roomId = new URLSearchParams(location.search).get('roomId')!
        keepReachable(await onCreateRoomHeld(roomId))
        setResult(JSON.stringify('holding'))
      })}

      {scenario('self-close-released', null, 'After the holding tab closed', async () => {
        const held = `${selfCloseBase}:held`
        // On SSE the server holds the closed tab for reconnectTimeout (60 s).
        await pollUntil(async () => {
          const heldOpen = await onIsOpen(held)
          return { result: { heldOpen }, done: !heldOpen }
        }, CLOSED_TAB_POLLS)
      })}

      {scenario('list-races', 'Room.list(), close and recreate', 'List while closing, recreate', async () => {
        const base = `e2e-list:${crypto.randomUUID()}`
        for (const suffix of ['b', 'é', 'B', 'a', 'Z']) await onCreateRoom(`${base}:${suffix}`)
        const order = (await onListAsListed(base)).map((id) => id.slice(base.length + 1))
        const whileClosing = await onListWhileClosing(`${base}:c`)
        const recreatedId = `${base}:again`
        const recreated = await onRecreate(recreatedId)
        await pollUntil(async () => {
          const recreatedOpen = await onIsOpen(recreatedId)
          return { result: { order, whileClosing, recreated, recreatedOpen }, done: !recreatedOpen }
        })
      })}

      {scenario('ack-dm', 'Acknowledged DMs', 'DMs that wait for their reply', async () => {
        const [roomId, room] = await createRoom('ack-dm')
        const me = await room.join({ meta: { name: 'Asker' } })
        const peer = await room.join({ meta: { name: 'Peer' } })
        peer.listen((data) => ({ echo: data }))
        const threaded = await room.join({ meta: { name: 'Threaded' } })
        threaded.listen(threadedReply)
        const serverMember = await onJoinAnswering(roomId)
        const held = await onJoinAsServer(roomId, 'Held')
        held.listen(() => 'pong')
        const heldThreaded = await onJoinAsServer(roomId, 'Held threaded')
        heldThreaded.listen(threadedReply)
        const unhandled: string[] = []
        const recordUnhandled = (event: PromiseRejectionEvent) => unhandled.push(String(event.reason))
        window.addEventListener('unhandledrejection', recordUnhandled)
        try {
          setResult(
            JSON.stringify({
              fromPage: await replyOf(me.send(peer.id, 'hi', { ack: true })),
              fromPageUnsendable: await replyOf(me.send(threaded.id, 'where?', { ack: true })),
              fromServer: await replyOf(me.send(serverMember, 'hi', { ack: true })),
              fromHeld: await replyOf(me.send(held.id, 'ping', { ack: true })),
              fromHeldUnsendable: await replyOf(me.send(heldThreaded.id, 'where?', { ack: true })),
              refused: await replyOf(me.send(serverMember, 'refuse', { ack: true })),
              unhandled,
            }),
          )
        } finally {
          window.removeEventListener('unhandledrejection', recordUnhandled)
        }
      })}

      {scenario('ack-dm-close', null, 'Ask a member who saves their reply', async () => {
        const [roomId, room] = await createRoom('ack-dm-close')
        const me = await room.join({ meta: { name: 'Asker' } })
        const leaving = await onJoinAsServer(roomId, 'Leaving')
        leaving.listen(async () => {
          setResult(JSON.stringify({ roomId, saving: true }))
          await onSaveReply(roomId)
          return 'bye'
        })
        const answer = replyOf(me.send(leaving.id, 'thanks', { ack: true }))
        const { promise: closeClicked, resolve } = Promise.withResolvers<void>()
        closeChat = resolve
        await closeClicked
        const closing = close(leaving)
        setResult(JSON.stringify({ roomId, closing: true }))
        await closing
        setResult(JSON.stringify({ answer: await answer, closed: true }))
      })}
      <button id="test-room-ack-dm-close-chat" onClick={() => closeChat?.()}>
        Close the chat
      </button>

      {scenario('ack-dm-close-view', null, 'Ask a member a page joined, who saves their reply', async () => {
        const [roomId, room] = await createRoom('ack-dm-close-view')
        const me = await room.join({ meta: { name: 'Asker' } })
        const chat = await onGetRoom(roomId)
        const leaving = await chat.join({ meta: { name: 'Leaving' } })
        leaving.listen(async () => {
          setResult(JSON.stringify({ roomId, saving: true }))
          await onSaveReply(roomId)
          return 'bye'
        })
        const answer = replyOf(me.send(leaving.id, 'thanks', { ack: true }))
        const { promise: closeClicked, resolve } = Promise.withResolvers<void>()
        closeViewChat = resolve
        await closeClicked
        const closing = close(chat)
        setResult(JSON.stringify({ roomId, closing: true }))
        await closing
        setResult(JSON.stringify({ answer: await answer, closed: true }))
      })}
      <button id="test-room-ack-dm-close-view-chat" onClick={() => closeViewChat?.()}>
        Close the chat
      </button>

      {scenario('ack-dm-hang-up', null, 'Ask a member who saves their reply, then hang up', async () => {
        const [roomId, room] = await createRoom('ack-dm-hang-up')
        const me = await room.join({ meta: { name: 'Asker' } })
        const hangingUp = await onJoinAsServer(roomId, 'Hanging up')
        const { promise: saved, resolve: onSaved } = Promise.withResolvers<void>()
        hangingUp.listen(async () => {
          setResult(JSON.stringify({ roomId, saving: true }))
          await onSaveReply(roomId)
          onSaved()
          return 'bye'
        })
        const unhandled: string[] = []
        const recordUnhandled = (event: PromiseRejectionEvent) => unhandled.push(String(event.reason))
        window.addEventListener('unhandledrejection', recordUnhandled)
        try {
          const answer = replyOf(me.send(hangingUp.id, 'thanks', { ack: true }))
          const { promise: hangUpClicked, resolve } = Promise.withResolvers<void>()
          hangUp = resolve
          await hangUpClicked
          abort(hangingUp)
          const fromHangingUp = await answer
          setResult(JSON.stringify({ roomId, fromHangingUp }))
          await saved
          // The reply's rejection, if any, is reported by the next task.
          await new Promise((next) => setTimeout(next))
          setResult(JSON.stringify({ fromHangingUp, unhandled }))
        } finally {
          window.removeEventListener('unhandledrejection', recordUnhandled)
        }
      })}
      <button id="test-room-ack-dm-hang-up-now" onClick={() => hangUp?.()}>
        Hang up
      </button>

      {scenario('ack-timeout', null, 'An acknowledged DM nobody answers', async () => {
        const [, room] = await createRoom('ack-timeout')
        const me = await room.join({ meta: { name: 'Asker' } })
        const silent = await room.join({ meta: { name: 'Silent' } })
        silent.listen(() => new Promise(() => {}))
        const start = performance.now()
        const error = await me.send(silent.id, 'hello?', { ack: true }).then(
          () => null,
          () => 'rejected',
        )
        setResult(JSON.stringify({ error, seconds: Math.round((performance.now() - start) / 1000) }))
      })}

      {scenario(
        'roster-churn',
        'Roster under churn',
        'Open a big room while members come, go and write meta',
        async () => {
          const roomId = await createRoomId('roster-churn')
          await onSeedAndChurn(roomId)
          const room = await onGetRoom(roomId)
          let emptied = false
          room.onEmpty(() => (emptied = true))
          const listed = await room.getParticipants().then(
            (members) => members.length,
            (err: Error) => err.message,
          )
          const me = await room.join({ meta: { name: 'Page' } })
          const serverCount = await onStopChurn(roomId)
          await pollUntil(() => ({
            result: { listed, serverCount, count: room.count, emptied, joined: me.id !== '' },
            done: room.count === serverCount,
          }))
        },
      )}

      {scenario('dm-overflow', 'DM hold overflow', 'Three acknowledged DMs before listen()', async () => {
        const [, room] = await createRoom('dm-overflow')
        const late = await room.join({ meta: { name: 'Late' } })
        const sender = await room.join({ meta: { name: 'Sender' } })
        // Three 1,500-byte DMs: two fit the 4,096-byte bufferLimit of the lane that runs this, three don't.
        const sends = ['1', '2', '3'].map((n) =>
          sender.send(late.id, n.padEnd(1500, '.'), { ack: true }).then(
            () => 'answered',
            () => 'rejected',
          ),
        )
        // The third's arrival drops the oldest, whose acknowledged send rejects.
        const first = await Promise.race([sends[0]!, new Promise((resolve) => setTimeout(() => resolve('held'), 5000))])
        const got: string[] = []
        late.listen((data) => {
          got.push((data as string)[0]!)
        })
        setResult(JSON.stringify({ first, outcomes: await Promise.all(sends), got }))
      })}

      {scenario('hold-overflow', null, 'Publishes held past bufferLimit', async () => {
        const roomId = await createRoomId('hold-overflow')
        const serverOutcomes = await onPublishWhileEstablishing(roomId)
        await pollUntil(async () => {
          const seen = await onGetWatched(roomId)
          return { result: { serverOutcomes, seen }, done: seen.length >= 2 }
        })
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

      {scenario('member-keeps-room', 'A member keeps its room', 'Join, then let go of the room', async () => {
        const roomId = await createRoomId('member-keeps-room')
        // The page keeps only `me`, not the room it joined through.
        const me = await (await onGetRoom(roomId)).join({ meta: { name: 'Kept' } })
        let left: string | null = null
        me.onLeave((cause) => (left = cause.type))
        const { promise: checked, resolve } = Promise.withResolvers<void>()
        checkKeptMember = resolve
        setResult(JSON.stringify({ roomId }))
        await checked
        const watcher = await onGetRoom(roomId)
        const releaseWatcher = keepReachable(watcher)
        const received: unknown[] = []
        watcher.subscribe((data) => received.push(data))
        const published = await me.publish('after-gc', { retain: true }).then(
          () => 'ok',
          (error: Error) => error.message,
        )
        await pollUntil(() => ({
          result: { published, received, left },
          done: received.length > 0 || published !== 'ok',
        }))
        releaseWatcher()
      })}
      <button id="test-room-member-keeps-room-check" onClick={() => checkKeptMember?.()}>
        Check the member
      </button>

      {scenario(
        'members-let-go',
        null,
        'Let go of a member, of one that left, and of one in a closed room',
        async () => {
          // Closing 500 ms after their member left and nothing holds them.
          const dropped = `e2e-let-go-dropped:${crypto.randomUUID()}`
          const left = `e2e-let-go-left:${crypto.randomUUID()}`
          const timeouts = { emptyTimeout: 500, departureTimeout: 500 }
          await Promise.all([onCreateRoomWith(dropped, timeouts), onCreateRoomWith(left, timeouts)])
          const closed = await createRoomId('let-go-closed')
          const guarded = await createRoomId('let-go-guarded')
          const unhandled: unknown[] = []
          const onUnhandled = (event: PromiseRejectionEvent) => unhandled.push(event.reason)
          window.addEventListener('unhandledrejection', onUnhandled)
          const droppedLeaves: string[] = []
          await joinAndLetGo(dropped, droppedLeaves)
          // The page keeps only a member that left.
          const leaver = await (await onGetRoom(left)).join({ meta: { name: 'Leaver' } })
          await leaver.leave()
          const stays = await (await onGetRoom(closed)).join({ meta: { name: 'Stays' } })
          const closedLeaves: string[] = []
          stays.onLeave((cause) => closedLeaves.push(cause.type))
          await onCloseRoom(closed)
          const rejected = await (await onOpenGuardedRoom(guarded)).join({ meta: { name: 'Banned' } }).then(
            () => 'joined',
            (error: Error) => error.message,
          )
          const { promise: checked, resolve } = Promise.withResolvers<void>()
          checkLetGo = resolve
          setResult(JSON.stringify({ ready: true }))
          await checked
          await pollUntil(async () => {
            const result = {
              droppedLeaves,
              droppedOpen: await onIsOpen(dropped),
              leftOpen: await onIsOpen(left),
              leaverPublish: await Promise.resolve()
                .then(() => leaver.publish('x'))
                .then(
                  () => 'ok',
                  (error: Error) => error.message,
                ),
              closedLeaves,
              rejected,
              unhandled: unhandled.length,
            }
            return { result, done: !result.droppedOpen && !result.leftOpen }
          })
          window.removeEventListener('unhandledrejection', onUnhandled)
        },
      )}
      <button id="test-room-members-let-go-check" onClick={() => checkLetGo?.()}>
        Check the members let go of
      </button>

      {scenario('tail', 'Tail (single-call history)', 'Tail holds pre-subscribe messages', async () => {
        const [roomId, owner] = await createRoom('tail')
        const me = await owner.join({ meta: { name: 'Src' } })

        const tailed = await onGetRoomTail(roomId)
        const existing = await onGetOrCreateRoomTail(roomId)
        const plain = await onGetRoom(roomId)
        await me.publish({ t: 'between' })

        const createdId = `e2e-tail-created:${crypto.randomUUID()}`
        const created = await onGetOrCreateRoomTail(createdId)
        const createdSrc = await (await onGetRoom(createdId)).join({ meta: { name: 'Src' } })
        await createdSrc.publish({ t: 'between' })

        const subscribe = (view: RoomView) => {
          const texts: string[] = []
          view.subscribe((data) => texts.push((data as { t: string }).t))
          return texts
        }
        const result = {
          received: subscribe(tailed),
          existing: subscribe(existing),
          created: subscribe(created),
          plain: subscribe(plain),
        }
        await me.setAttributes({}) // same-channel ack orders the subscriptions before the live publish
        await me.publish({ t: 'after' })

        await pollUntil(() => ({
          result,
          done:
            result.received.includes('between') &&
            result.existing.includes('between') &&
            result.created.includes('between') &&
            result.plain.includes('after'),
        }))
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

      {scenario('retain-leave', null, 'A member leaves while a late view replays', async () => {
        const [roomId, pubView] = await createRoom('retain-leave')
        const releasePubView = keepReachable(pubView)
        const a = await pubView.join({ meta: { name: 'A' } })
        const b = await pubView.join({ meta: { name: 'B' } })
        await a.publishBinary(new Uint8Array([1]), { track: 'cam', retain: true })
        // Delivers the cam track on the server already, so live frames reach a late view while its replay reads.
        await onWatchRoomTrack(roomId, 'cam')

        await onHoldRetainedReads(roomId)
        const late = await onGetRoom(roomId)
        const frames: Array<[string | null, number, unknown]> = []
        const left: unknown[] = []
        late.subscribeBinary((data, info, from) => frames.push([info.track, data[0]!, from.meta.name ?? null]))
        late.onLeave((member) => left.push(member.meta.name))
        await onWatchEveryFrame(roomId)
        // The page's replay and the server view's each wait on the retained read.
        await pollUntil(async () => ({ done: (await onHeldRetainedReads(roomId)) >= 2 }))
        await a.publishBinary(new Uint8Array([4]), { track: 'cam' })
        await b.publishBinary(new Uint8Array([5]), { track: 'cam' })
        await pollUntil(async () => {
          const seen = (await onGetWatchedTrack(roomId)).frames
          return { done: seen.includes(4) && seen.includes(5) }
        })
        await a.leave()
        await pollUntil(() => ({ done: left.includes('A') }))
        await onReleaseRetainedReads(roomId)

        await b.publishBinary(new Uint8Array([9]), { track: 'cam' })
        await pollUntil(async () => {
          const server = await onGetEveryFrame(roomId)
          return {
            result: { frames, server },
            done: frames.some(([, n]) => n === 9) && server.some(([n]) => n === 9),
          }
        })
        releasePubView()
      })}

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
        await pollUntil(() => ({ done: closed && myCause !== null }), 150)
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

      {scenario('meta-away', 'Back from offline, twice', "The room's meta, each time", async () => {
        const [roomId, view] = await createRoom('meta-away')
        await view.join({ meta: { name: 'Me' } })
        await onRoomMetaWhileAway(roomId)
        keepReachable(view)
        const update = () => {
          const { meta, participants } = view.snapshot()
          const srv = participants.find((member) => member.meta.name === 'Srv')
          setResult(
            JSON.stringify({
              roomId,
              topic: (meta as { topic?: string }).topic ?? null,
              filler: (srv?.meta.filler as string | undefined)?.trim() ?? null,
            }),
          )
        }
        update()
        view.onChange(update)
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
        await pollUntil(() => ({ done: closed && myCause !== null }), 150)
        const leaves = await onGetRoomOverflow(roomId)
        setResult(JSON.stringify({ roomId, seen, myCause, closed, isClosed: view.isClosed, leaves }))
      })}

      {scenario('participant-overflow', null, 'A server-joined participant offline past bufferLimit', async () => {
        const [roomId] = await createRoom('participant-overflow')
        const me = await onJoinAsServer(roomId, 'Bob')
        const got: string[] = []
        me.listen((data) => got.push((data as string).trim()))
        let myCause: unknown = null
        me.onLeave((cause) => (myCause = cause))
        await onParticipantOverflow(roomId, me.id)
        await pollUntil(() => ({ done: got.includes('before') }))
        setResult(JSON.stringify({ roomId }))
        await pollUntil(() => ({ done: myCause !== null }), 150)
        setResult(JSON.stringify({ roomId, got, myCause }))
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
        await pollUntil(async () => {
          watched = await onGetWatchedCount(roomId)
          return { done: watched >= 80 }
        }, 100)
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
        await tailed.join({ meta: { name: 'Tail' } }) // same-channel ack orders the declaration before the announcement
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
        // The room closes then unless the heartbeat restored its view's hold; 5 s for that close to land.
        const closedUnlessHealedAt = performance.now() + 40_000
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

        await pollUntil(async () => {
          const listed = await onListParticipantNames(joinRoomId)
          return { done: listed.length === 1 && performance.now() >= closedUnlessHealedAt }
        }, CLOSED_TAB_POLLS)
        setResult(
          JSON.stringify({
            viewRoom: { listed: await onListRooms(viewRoomId), isClosed: view.isClosed },
            joinRoom: { firstJoin, listed: await onListParticipantNames(joinRoomId), count: joinView.count },
          }),
        )
      })}

      {scenario('scale', null, 'A room of 100,000 members', async () => {
        const roomId = `e2e-scale:${crypto.randomUUID()}`
        const seedStart = performance.now()
        await onSeedRoom(roomId, 100_000)
        const seedMs = Math.round(performance.now() - seedStart)
        const view = await onGetRoom(roomId)
        await onSeedChurn(roomId)
        let names: string[] = []
        await pollUntil(async () => {
          names = (await view.getParticipants()).map((member) => member.meta.name as string)
          return { done: names.includes('late') }
        }, 300)
        const roster = {
          count: view.count,
          listed: new Set(names).size,
          late: names.includes('late'),
          m0: names.includes('m0'),
        }
        // Through the room's next 30 s heartbeat.
        const pings: number[] = []
        const pingUntil = performance.now() + 32_000
        while (performance.now() < pingUntil) {
          const start = performance.now()
          await onPing()
          pings.push(performance.now() - start)
          await new Promise((r) => setTimeout(r, 50))
        }
        setResult(
          JSON.stringify({
            roster,
            pings: pings.length,
            maxPingMs: Math.round(Math.max(...pings)),
            seedMs,
          }),
        )
      })}

      {scenario('away-join', 'Joining while offline', 'Another member leaves meanwhile', async () => {
        const [roomId, view] = await createRoom('away-join')
        const leaves: unknown[] = []
        view.onLeave((member, cause) => leaves.push([member.meta.name, cause]))
        await onRoomLeaveWhileJoining(roomId)
        await pollUntil(() => ({ done: view.count === 1 }))
        const { promise: joinStarted, resolve } = Promise.withResolvers<void>()
        startAwayJoin = resolve
        setResult(JSON.stringify({ roomId }))
        await joinStarted
        const me = await view.join({ meta: { name: 'Me' } })
        await pollUntil(() => ({ done: leaves.length > 0 }))
        setResult(JSON.stringify({ roomId, joined: me.id !== undefined, leaves, count: view.count }))
      })}
      <button id="test-room-away-join-now" onClick={() => startAwayJoin?.()}>
        Join now
      </button>
    </div>
  )
}
