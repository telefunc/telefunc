import { describe, expect, it } from 'vitest'
import type { ClientBroadcast } from 'telefunc/__internal'
import { Room } from '../index.js'
import { ClientRoom } from '../client/room.js'
import type { RoomStubRequest } from './protocol.js'
import type { LocalParticipant } from '../types.js'

/** `me`, joined at a page through a channel that answers as the server does: each meta write is accepted at the next
 *  seq. */
async function pageMember(meta: { name: string }): Promise<LocalParticipant> {
  let seq = 0
  const end = {
    isClosed: false,
    isConnected: true,
    send: async (req: RoomStubRequest) => {
      if (req.__r === 'req-join') return { ok: true, value: { id: 'me', joinedAt: 0 } }
      if (req.__r === 'req-set-meta') return { ok: true, value: { seq: ++seq, meta: req.meta } }
      throw new Error(`Unexpected request ${req.__r}`)
    },
    _subscribeLocal: () => () => {},
    _setSubscribed: () => {},
    onClose: () => () => {},
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
  }
  const room = new ClientRoom({
    end: end as unknown as ClientBroadcast,
    snapshot: { channelId: 'channel', roomId: crypto.randomUUID(), meta: [], metaSeq: 0, closed: false, members: [] },
  })
  return await room.join({ meta })
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
