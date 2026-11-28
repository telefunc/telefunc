import { expect, it } from 'vitest'
import { pageRoom } from '../test/pageRoom.js'

it('a write equal to the current meta survives an older change still awaiting its reply', async () => {
  const { room } = pageRoom()
  const me = await room.join({ meta: { name: 'Pub', mic: true } })

  const earlier = me.updateMeta({ mic: false })
  const restore = me.updateMeta({ mic: true })
  await Promise.all([earlier, restore])

  expect(me.meta).toEqual({ name: 'Pub', mic: true })
})
