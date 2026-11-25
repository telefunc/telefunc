// A join with large meta on the memory store, which has no size rule: it is admitted, and its meta is what the room
// lists. The Redis store refuses the same join (packages/redis/src/join-size.store.spec.ts).
import { beforeAll, expect, it } from 'vitest'
import { Room } from '../index.js'
import { loadRoomServer } from './load.js'

// Room imports its server code on first use (`load.ts`), which vitest's transform can make take seconds on a busy
// machine: the row measures the join, not that import.
beforeAll(async () => {
  await loadRoomServer()
})

it('admits a join whose meta is larger than the Redis store commits, with that meta', async () => {
  const id = `join-large-${crypto.randomUUID()}`
  const room = await Room.create(id)
  const large = 'g'.repeat(29 * 1024 * 1024)
  const member = await room.join({ meta: { large } })
  const [listed] = await Room.getParticipants(id)

  expect({ joined: member.id === listed?.id, size: listed?.meta.large === large }).toEqual({ joined: true, size: true })
  await Room.close(id)
})
