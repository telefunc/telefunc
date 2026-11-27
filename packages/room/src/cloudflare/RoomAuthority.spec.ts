import { afterEach, describe, expect, it, vi } from 'vitest'
import type { TelefuncExtensionNamespace } from 'telefunc/__internal'
import { encodeLaneKey } from '../backend/laneKey.js'
import type { OperationArgs } from '../server/store/roomOperations.js'
import { hasNodeSqlite, sqliteStorage } from '../../../telefunc/wire-protocol/test/cloudflareSql.js'
import { RoomAuthority, type OperationWire } from './RoomAuthority.js'

afterEach(() => vi.useRealTimers())

/** An open room's authority on the SQLite stand-in, whose hand-ons and directory calls are taken at once, and the seq
 *  of each commit it handed on. */
async function openRoom() {
  const namespace: TelefuncExtensionNamespace = {
    idFromName: (name) => name,
    idFromString: (id) => id,
    get: () => ({ telefuncDurableObjectCall: () => Promise.resolve(undefined) }),
  }
  const handedOn: number[] = []
  const room = new RoomAuthority(
    { storage: sqliteStorage().storage },
    {
      regions: { forward: async (_key, delivery) => void handedOn.push(delivery.info.seq) },
      getNamespace: () => namespace,
    },
  )
  await room.compareExchangeHead(
    { form: 'absent' },
    { head: { rev: 'rev', currentInc: 'inc', state: 'open', config: new Uint8Array() } },
    { roomId: 'room', bucket: 'weur' },
  )
  return { room, handedOn }
}

/** Alice's join, which carries her inbox's lease. */
const join: OperationArgs['join'] = {
  lease: { id: 'server', created: false },
  hold: null,
  id: 'alice',
  record: { joinedAt: 1, metaSeq: 0, metaGen: 0 },
  meta: {},
  join: { __r: 'join', id: 'alice', meta: {}, joinedAt: 1 },
}
const inbox = {
  roomId: 'room',
  inc: 'inc',
  laneKey: encodeLaneKey({ kind: 'inbox', member: 'alice' }),
  sessionDoId: 'session',
  leaseId: 'inbox-A',
  bucket: 'weur',
}

/** What `wire` answered: its value where it was done. */
const answered = <Value>(wire: OperationWire<Value> | 'regrant') =>
  wire === 'regrant' ? wire : wire.kind === 'done' ? wire.value : wire.kind

describe.runIf(hasNodeSqlite)("RoomAuthority: a member's join, sent again", () => {
  // A session waits 70 s for a join's answer: the room's 60 s for an earlier commit's hand-on, then a call's own 10 s.
  it('is answered as the join was while it comes within the join’s 70 s, and admits nothing once they passed', async () => {
    vi.useFakeTimers()
    const { room, handedOn } = await openRoom()
    const grant = await room.grantHolder('session')
    const send = async () =>
      answered(await room.runJoinWithInbox('inc', join, { route: inbox, order: { grant, seq: 1 } }))
    expect(await send()).toBe('joined')
    await vi.advanceTimersByTimeAsync(69_000)
    await room.alarm()
    const within = await send()
    await vi.advanceTimersByTimeAsync(1_000)
    await room.alarm()
    expect({ within, past: await send(), handedOn }).toEqual({ within: 'joined', past: 'overtaken', handedOn: [1] })
  })
})
