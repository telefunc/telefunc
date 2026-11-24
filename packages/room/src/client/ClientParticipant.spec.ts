import { describe, expect, it } from 'vitest'
import { loneMember, pageRoom } from '../test/pageRoom.js'
import type { ClientRoom } from './room.js'

/** A request of the page's, answered with `answers` in place of the server's answer. */
type Row = [
  what: string,
  answers: Parameters<typeof pageRoom>[1],
  send: (room: ClientRoom) => Promise<unknown>,
  error: string,
]

const join = (room: ClientRoom) => room.join()
const setMeta = async (room: ClientRoom) => (await room.join()).setMeta({ name: 'Ann' })
const publish = async (room: ClientRoom) => (await room.join()).publish('hi')

describe("a page's request answered with a value Room doesn't write", () => {
  it.each<Row>([
    ['a join, with no id', { join: { joinedAt: 1 } }, join, "A joined member's id is not text"],
    [
      'a join, with a time of text',
      { join: { id: 'me', joinedAt: '1' } },
      join,
      "A joined member's time is not a number",
    ],
    [
      'a meta write, with no seq',
      { metaWrite: { meta: { name: 'Ann' } } },
      setMeta,
      "A meta update's seq is not a number",
    ],
    [
      'a meta write, with a meta of text',
      { metaWrite: { seq: 1, meta: 'Ann' } },
      setMeta,
      "A meta update's meta is not a meta",
    ],
    ['a publish, with no time', { publish: { key: 'room', seq: 1 } }, publish, "A publish's time is not a number"],
    [
      'a publish, with a key of a number',
      { publish: { key: 1, seq: 1, timestamp: 0 } },
      publish,
      "A publish's key is not text",
    ],
  ])('%s, rejects that request, and the room goes on taking events', async (_, answers, send, error) => {
    const { room, deliver } = pageRoom({ sam: 1 }, answers)
    const sent = send(room)
    deliver({ __r: 'join', id: 'tia', meta: {}, joinedAt: 2 })
    await expect(sent).rejects.toThrow(error)
    deliver({ __r: 'join', id: 'uma', meta: {}, joinedAt: 3 })
    // The join that arrived while the request was out, and the one after it.
    expect(room.getParticipants().map((participant) => participant.id)).toEqual(expect.arrayContaining(['tia', 'uma']))
  })
})

describe("a lone participant's publish answered with a value Room doesn't write", () => {
  it('rejects that publish, and the participant goes on taking notices', async () => {
    const { me, deliver } = loneMember({ publish: { key: 'room', seq: 1 } })
    const sent = me.publishBinary(new Uint8Array([1]))
    deliver({ __r: 'p-meta', id: me.id, seq: 1, attrs: { name: 'Ann' } })
    await expect(sent).rejects.toThrow("A publish's time is not a number")
    deliver({ __r: 'p-meta', id: me.id, seq: 2, attrs: { role: 'host' } })
    // The meta that arrived while the publish was out, and the one after it.
    expect(me.meta).toEqual({ name: 'Ann', role: 'host' })
  })
})

// The server admits a page's member once its `onBeforeJoin()` answered (`server/lifecycle/admit.ts`), which may take
// longer than the page waits for the join's answer: the `joined` naming that member then names a join the page settled.
describe("a page's join whose answer timed out before the server admitted its member", () => {
  it("takes the `joined` that comes after as no join of its own, and shows that member's events", async () => {
    const { room, requests, deliver } = pageRoom({ sam: 1 }, { joinTimesOut: true })
    await expect(room.join()).rejects.toThrow('Acknowledgement timed out')
    const [join] = requests
    if (join?.__r !== 'req-join') throw new Error('The page sent no join')
    deliver({ __r: 'joined', reqId: join.reqId, id: 'late' })
    deliver({ __r: 'join', id: 'late', meta: {}, joinedAt: 2 })
    deliver({ __r: 'p-meta', id: 'late', seq: 1, attrs: { name: 'Lee' } })
    expect(room.getParticipants().find((participant) => participant.id === 'late')?.meta).toEqual({ name: 'Lee' })
  })
})
