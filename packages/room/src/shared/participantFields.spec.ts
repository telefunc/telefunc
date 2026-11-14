import { describe, expect, it, vi } from 'vitest'
import type { ClientBroadcast } from 'telefunc/__internal'
import { Room } from '../index.js'
import { ClientRoom } from '../client/room.js'
import { defineParticipantField } from './participantFields.js'

/** A page's room on a channel that sends nothing, and what ends the channel's wire. */
function pageRoom() {
  const closes: Array<() => void> = []
  const end = {
    isClosed: false,
    isConnected: true,
    _subscribeLocal: () => () => {},
    onClose: (callback: () => void) => {
      closes.push(callback)
      return () => {}
    },
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
  }
  const room = new ClientRoom({
    end: end as unknown as ClientBroadcast,
    snapshot: { channelId: 'channel', roomId: crypto.randomUUID(), meta: [], metaSeq: 0, closed: false, members: [] },
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
})
