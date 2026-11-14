import { describe, expect, it } from 'vitest'
import { Room } from '../index.js'
import { pageRoom } from '../test/pageRoom.js'
import type { LocalParticipant } from '../types.js'

async function pageMember(meta: { name: string }): Promise<LocalParticipant> {
  return await pageRoom().room.join({ meta })
}

async function serverMember(meta: { name: string }): Promise<LocalParticipant> {
  const room = await Room.create(`meta-${crypto.randomUUID()}`)
  return await room.join({ meta })
}

/** How many times `me.onChange` is called by each write, once its changes are in. */
async function changesBy(me: LocalParticipant, writes: Array<() => Promise<void>>): Promise<number[]> {
  let changes = 0
  me.onChange(() => changes++)
  const counts: number[] = []
  for (const write of writes) {
    changes = 0
    await write()
    await Promise.resolve()
    counts.push(changes)
  }
  return counts
}

describe.each([
  ['page', pageMember],
  ['server', serverMember],
])("a %s's setMeta()", (_, member) => {
  it('with the meta it has, the same object or an equal one, keeps it and changes nothing', async () => {
    const me = await member({ name: 'Pub' })
    const meta = me.meta
    const counts = await changesBy(me, [() => me.setMeta(meta), () => me.setMeta({ name: 'Pub' })])
    expect(counts).toEqual([0, 0])
    expect(me.meta).toBe(meta)
  })

  it('with another meta calls onChange once', async () => {
    const me = await member({ name: 'Pub' })
    expect(await changesBy(me, [() => me.setMeta({ name: 'Pat' })])).toEqual([1])
    expect(me.meta).toEqual({ name: 'Pat' })
  })
})

describe('a meta write that leaves the meta as it is', () => {
  it("is not sent from the page, unless a write before it hasn't been answered yet", async () => {
    const { room, requests } = pageRoom()
    const me = await room.join({ meta: { name: 'Pub', mic: true } })
    const sent = () => requests.filter(({ __r }) => __r !== 'req-join').length
    // Its keys in another order are another value.
    await me.setMeta({ mic: true, name: 'Pub' })
    await me.setMeta({ mic: true, name: 'Pub' })
    await me.setAttributes({ mic: true, cam: undefined })
    expect(sent()).toBe(1)
    void me.setAttributes({ mic: false })
    await me.setAttributes({ mic: true })
    expect(sent()).toBe(3)
    expect(me.meta).toEqual({ name: 'Pub', mic: true })
  })

  it('changes nothing a reader sees where it arrives', async () => {
    const room = await Room.create(`meta-${crypto.randomUUID()}`)
    const me = (await room.join({ meta: { name: 'Pub' } })) as LocalParticipant & {
      _setMeta(meta: object): Promise<unknown>
      _setAttributes(attrs: object): Promise<unknown>
    }
    const changes = { room: 0, participant: 0, me: 0 }
    room.onChange(() => changes.room++)
    room.getParticipant(me.id)!.onChange(() => changes.participant++)
    me.onChange(() => changes.me++)
    const snapshot = room.snapshot()
    // As a page holding `me` sends one that raced another.
    await me._setMeta({ name: 'Pub' })
    await me._setAttributes({ name: 'Pub' })
    await Promise.resolve()
    expect(changes).toEqual({ room: 0, participant: 0, me: 0 })
    expect(room.snapshot()).toBe(snapshot)
  })
})
