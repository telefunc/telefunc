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
