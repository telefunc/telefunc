import { describe, expect, it, vi } from 'vitest'
import { Room } from '../index.js'
import { pageRoom } from '../test/pageRoom.js'
import { defineParticipantField, type ParticipantFieldView } from './participantFields.js'
import type { RemoteParticipant } from '../types.js'

type FieldRooms = Set<ParticipantFieldView<unknown>['room']>

/** How many views of the room `id` the field runs on. */
function runningOn(running: FieldRooms, id: string): number {
  return [...running].filter((room) => room.id === id).length
}

/** A field that keeps the rooms it runs on. */
function runningField(name: string): FieldRooms {
  const running: FieldRooms = new Set()
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
    expect(runningOn(running, room.id)).toBe(1)
    endWire()
    expect(runningOn(running, room.id)).toBe(0)
  })

  it("stops on a server's room when the room closes while the app observes it", async () => {
    const running = runningField('stopsOnServer')
    const room = await Room.create(`fields-${crypto.randomUUID()}`)
    room.onChange(() => {})
    expect(runningOn(running, room.id)).toBe(1)
    await Room.close(room.id)
    await vi.waitFor(() => expect(runningOn(running, room.id)).toBe(0))
  })

  it("keeps running on a server's room while the app observes it, though the members it listened to left", async () => {
    const running: FieldRooms = new Set()
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
    // The view only: the room joined through observes it no more.
    await vi.waitFor(() => expect(runningOn(running, view.id)).toBe(1))
    stopObserving()
    await vi.waitFor(() => expect(runningOn(running, view.id)).toBe(0))
  })

  it("stops on a server's room once the app stops observing it, though it follows those who joined later", async () => {
    const running: FieldRooms = new Set()
    defineParticipantField('followsLaterJoins', {
      initial: null,
      attach({ room }) {
        running.add(room)
        const stops: Array<() => void> = []
        const follow = (participant: RemoteParticipant) => stops.push(participant.onChange(() => {}))
        room.snapshot().participants.forEach(follow)
        stops.push(room.onJoin(follow))
        return () => {
          running.delete(room)
          stops.forEach((stop) => stop())
        }
      },
    })
    const room = await Room.create(`fields-${crypto.randomUUID()}`)
    const view = await Room.get(room.id)
    const stopObserving = view.onChange(() => {})
    await room.join({ meta: {} })
    await vi.waitFor(() => expect(view.snapshot().count).toBe(1))
    // The view, and the room joined through.
    expect(runningOn(running, room.id)).toBe(2)
    stopObserving()
    await vi.waitFor(() => expect(runningOn(running, room.id)).toBe(1))
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
    const { room } = pageRoom({ ann: 0 })
    const value = Object.freeze({ tracks: ['camera'] })
    views.at(-1)!.set('ann', value)
    const before = room.snapshot()
    value.tracks.push('screen')
    expect((before.participants[0] as unknown as { ownCopy: Tracks }).ownCopy.tracks).toEqual(['camera'])
    expect(room.snapshot()).toBe(before)
  })

  it('keeps its first definition when its name is defined again, as a second copy of the package does', () => {
    const attached: string[] = []
    for (const copy of ['first', 'second']) {
      defineParticipantField('definedTwice', {
        initial: copy,
        attach() {
          attached.push(copy)
          return () => {}
        },
      })
    }
    const { room } = pageRoom({ ann: 0 })
    expect(attached).toEqual(['first'])
    expect((room.snapshot().participants[0] as unknown as { definedTwice: string }).definedTwice).toBe('first')
  })
})
