export { Room }

import React, { useEffect, useState } from 'react'
import {
  onCreateRoom,
  onGetRoom,
  onGetGuardedRoom,
  onJoinAsServer,
  onGetRoomWithMember,
  onAnnounce,
  onSystemSend,
  onKick,
  onCloseRoom,
} from './Room.telefunc'

/** Render every poll so the e2e autoRetry sees fresh data on each iteration (see Publish.tsx). */
async function pollUntil(render: () => { done: boolean }) {
  for (let poll = 0; poll < 50; poll++) {
    if (render().done) break
    await new Promise((r) => setTimeout(r, 200))
  }
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
        id="test-room-chat"
        onClick={async () => {
          setResult('')
          const roomId = `e2e-chat:${crypto.randomUUID()}`
          await onCreateRoom(roomId, { size: 10 })
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
          let activity = 0
          lobby.onActivity(() => activity++)

          const me = await lobby.join({ name: 'Alice' })
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
              activity,
            }
            setResult(JSON.stringify(state))
            return {
              done: events.length >= 2 && received.length >= 1 && updates.length >= 1 && activity >= 1 && changes >= 1,
            }
          })
        }}
      >
        Join, publish, setMeta, leave
      </button>

      <h2>Server-Joined Participant</h2>

      <button
        id="test-room-participant"
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
          const ally = await observer.join({ name: 'Ally' })
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

      <h2>Binary (isolated mode)</h2>

      <button
        id="test-room-binary"
        onClick={async () => {
          setResult('')
          const roomId = `e2e-binary:${crypto.randomUUID()}`
          await onCreateRoom(roomId, { isolated: true })
          const videoRoom = await onGetRoom(roomId)
          const me = await videoRoom.join({ name: 'Cam' })

          const frames: Array<{
            size: number
            firstByte: number
            fromSelf: boolean
            track: string | null
            keyFrame: boolean
          }> = []
          videoRoom.subscribeBinary((data, info, from) => {
            frames.push({
              size: data.byteLength,
              firstByte: data[0]!,
              fromSelf: from.id === me.id,
              track: info.track,
              keyFrame: info.keyFrame,
            })
          })
          const cameraOnly: number[] = []
          videoRoom.subscribeBinary((data) => cameraOnly.push(data[0]!), { track: 'camera' })

          // selfDelivery defaults to true — our own frames come back to us.
          for (let i = 0; i < 3; i++) {
            await me.publishBinary(new Uint8Array(64).fill(i + 1))
          }
          // Named track + keyframe bit — mic/camera/screen multiplex over one member lane.
          await me.publishBinary(new Uint8Array(32).fill(9), { track: 'camera', keyFrame: true })

          await pollUntil(() => {
            setResult(JSON.stringify({ frames, cameraOnly }))
            return { done: frames.length >= 4 && cameraOnly.length >= 1 }
          })
        }}
      >
        Publish 3 binary frames
      </button>

      <h2>Guards</h2>

      <button
        id="test-room-guard"
        onClick={async () => {
          setResult('')
          const roomId = `e2e-guard:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const room = await onGetGuardedRoom(roomId)

          const received: unknown[] = []
          room.subscribe((data) => received.push(data))
          // Admission is guarded too — the rejection reaches the joiner's promise over the wire.
          const joinError = await room.join({ name: 'Banned' }).then(
            () => null,
            (err: Error) => err.message,
          )
          const me = await room.join({ name: 'Mallory' })
          const peer = await room.join({ name: 'Peer' })
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

      <h2>Returnable member view</h2>

      <button
        id="test-room-member"
        onClick={async () => {
          setResult('')
          const roomId = `e2e-member:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)
          const me = await lobby.join({ name: 'Viewed' })

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
        id="test-room-admin"
        onClick={async () => {
          setResult('')
          const roomId = `e2e-admin:${crypto.randomUUID()}`
          await onCreateRoom(roomId)
          const lobby = await onGetRoom(roomId)
          const me = await lobby.join({ name: 'Eve' })

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
    </div>
  )
}
