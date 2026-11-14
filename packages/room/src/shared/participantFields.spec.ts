import { describe, expect, it, vi } from 'vitest'
import type { ClientBroadcast } from 'telefunc/__internal'
import { Room } from '../index.js'
import { ClientRoom } from '../client/room.js'
import { defineParticipantField, type ParticipantFieldView } from './participantFields.js'

/** A page's room of `members` on a channel that sends nothing, and what ends the channel's wire. */
function pageRoom(members: string[] = []) {
  const closes: Array<() => void> = []
  const end = {
    isClosed: false,
    isConnected: true,
    _subscribeLocal: () => () => {},
    _setSubscribed: () => {},
    onClose: (callback: () => void) => {
      closes.push(callback)
      return () => {}
    },
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
  }
  const room = new ClientRoom({
    end: end as unknown as ClientBroadcast,
    snapshot: {
      channelId: 'channel',
      roomId: crypto.randomUUID(),
      meta: [],
      metaSeq: 0,
      closed: false,
      members: members.map((id) => ({ id, meta: {}, metaSeq: 0, identity: null, joinedAt: 0 })),
    },
  })
  return { room, endWire: () => closes.forEach((close) => close()) }
}

/** A field that keeps the views of a room it runs on. */
function runningField(name: string): Set<unknown> {
  const running = new Set<unknown>()
  defineParticipantField(name, {
    initial: null,
    attach({ room }) {
      running.add(room)
      return () => running.delete(room)
    },
  })
  return running
}

describe("a package's field", () => {
  it("stops on a page's room when the room closes", () => {
    const running = runningField('stopsOnPage')
    const { room, endWire } = pageRoom()
    expect(running.has(room)).toBe(true)
    endWire()
    expect(running.has(room)).toBe(false)
  })

  it("stops on a server's room when the room closes while the app observes it", async () => {
    const running = runningField('stopsOnServer')
    const room = await Room.create(`fields-${crypto.randomUUID()}`)
    room.onChange(() => {})
    expect(running.has(room)).toBe(true)
    await Room.close(room.id)
    await vi.waitFor(() => expect(running.has(room)).toBe(false))
  })

  it("keeps running on a server's room while the app observes it, though the members it listened to left", async () => {
    const running = new Set<unknown>()
    defineParticipantField('listensToMembers', {
      initial: null,
      attach({ room }) {
        running.add(room)
        const stops = room.snapshot().participants.map((participant) => participant.onChange(() => {}))
        return () => {
          running.delete(room)
          stops.forEach((stop) => stop())
        }
      },
    })
    const room = await Room.create(`fields-${crypto.randomUUID()}`)
    const members = [await room.join({ meta: {} }), await room.join({ meta: {} })]
    const view = await Room.get(room.id)
    const stopObserving = view.onChange(() => {})
    await Promise.all(members.map((member) => member.leave()))
    await vi.waitFor(() => expect(view.snapshot().count).toBe(0))
    expect(running.has(view)).toBe(true)
    stopObserving()
    await vi.waitFor(() => expect(running.has(view)).toBe(false))
  })

  it('keeps its own copy of a value, so a value the package froze only at its root never changes a snapshot', () => {
    type Tracks = { tracks: string[] }
    const views: Array<ParticipantFieldView<Tracks>> = []
    defineParticipantField<Tracks>('ownCopy', {
      initial: { tracks: [] },
      attach(view) {
        views.push(view)
        return () => {}
      },
    })
    const { room } = pageRoom(['ann'])
    const value = Object.freeze({ tracks: ['camera'] })
    views.at(-1)!.set('ann', value)
    const before = room.snapshot()
    value.tracks.push('screen')
    expect((before.participants[0] as unknown as { ownCopy: Tracks }).ownCopy.tracks).toEqual(['camera'])
    expect(room.snapshot()).toBe(before)
  })
})
