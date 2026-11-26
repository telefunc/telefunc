import { expect, it, vi } from 'vitest'
import type { PublishBody, ServerBroadcastEnd } from 'telefunc/__internal'
import { Room } from '../index.js'
import { handleOf, ServerRoom } from './room.js'

/** The event each body the stub built for its page carries, by its tag and member. */
const eventOf = vi.hoisted(() => new WeakMap<object, string>())
vi.mock('./publishBodies.js', async (importOriginal) => {
  const bodies = await importOriginal<typeof import('./publishBodies.js')>()
  return {
    ...bodies,
    unorderedEventBody(event: Parameters<typeof bodies.unorderedEventBody>[0]) {
      const body = bodies.unorderedEventBody(event)
      if (event.__r === 'joined') eventOf.set(body, `joined:${event.reqId}:${event.id}`)
      return body
    },
  }
})

/** A page's end of a room's stub, which writes down what reaches the page, and hands the stub what the page sends. */
function pageEnd(sent: string[]) {
  let listener: Parameters<ServerBroadcastEnd['listen']>[0] | null = null
  const end: ServerBroadcastEnd = {
    id: crypto.randomUUID(),
    setOwner() {},
    terminateWire() {},
    trackAck: (promise) => promise,
    isAttached: true,
    isClosed: false,
    isConnected: true,
    deliver(body: PublishBody) {
      const event = eventOf.get(body)
      if (event !== undefined) sent.push(event)
    },
    listen(callback) {
      listener = callback
      return Object.assign(() => {}, { opened: Promise.resolve() })
    },
    onClose: () => () => {},
    onDisconnect() {},
    onReconnect() {},
    close: async () => null,
  }
  return {
    end,
    /** What the page's request answers with, once the stub answered it. */
    request: async (req: unknown) => {
      if (listener === null) throw new Error('The stub listens to no page')
      return listener(req)
    },
  }
}

// The page acts on a join's answer only once it read the `joined` that names the join's member
// (`client/ClientRoomHandle.ts`), and core keeps the order the stub sends them in
// (`telefunc/wire-protocol/channel/server/answerOrder.spec.ts`).
it("sends a page's join's `joined` before the join's answer", async () => {
  const roomId = `join-order-${crypto.randomUUID()}`
  const room = await Room.create(roomId)
  Room.guard(room, { anyoneCanJoin: true })
  if (!ServerRoom.isServerRoom(room)) throw new Error('Room.create() made no server room')
  const sent: string[] = []
  const { end, request } = pageEnd(sent)
  handleOf(room).openStub({ end, grantedHidden: new Set(), page: 'page' })
  const answer = await request({ __r: 'req-join', reqId: 7, meta: { name: 'Ann' }, selfDelivery: true })
  sent.push('answer')
  const [member] = await Room.getParticipants(roomId)
  expect({ answer, sent }).toEqual({
    answer: { ok: true, value: { id: member?.id, joinedAt: member?.joinedAt } },
    sent: [`joined:7:${member?.id}`, 'answer'],
  })
  await Room.close(roomId)
})
