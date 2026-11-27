import { afterEach, describe, expect, it, vi } from 'vitest'
import type { TelefuncExtensionNamespace } from 'telefunc/__internal'
import { encodeLaneKey } from '../backend/laneKey.js'
import type { OperationArgs } from '../server/store/roomOperations.js'
import { hasNodeSqlite, sqliteStorage } from '../../../telefunc/wire-protocol/test/cloudflareSql.js'
import { RoomAuthority, type OperationWire } from './RoomAuthority.js'

afterEach(() => vi.useRealTimers())

/** An open room's authority on the SQLite stand-in, whose hand-ons and directory calls are taken at once: its storage,
 *  the seq of each commit it handed on, and the method and arguments of each call to a directory. */
async function openRoom() {
  const directoryCalls: unknown[][] = []
  const namespace: TelefuncExtensionNamespace = {
    idFromName: (name) => name,
    idFromString: (id) => id,
    get: () => ({
      telefuncDurableObjectCall: (_name, method, args) => {
        directoryCalls.push([method, ...args])
        return Promise.resolve(undefined)
      },
    }),
  }
  const handedOn: number[] = []
  const { storage } = sqliteStorage()
  const room = new RoomAuthority(
    { storage },
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
  return { room, storage, handedOn, directoryCalls }
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

describe.runIf(hasNodeSqlite)('RoomAuthority: a closing head whose closer stopped', () => {
  it("is closed on the alarm at its close lease's end: its closed frame handed on, its data dropped, its listing gone", async () => {
    vi.useFakeTimers()
    const { room, storage, handedOn, directoryCalls } = await openRoom()
    await room.commitLane('room', 'inc', { kind: 'semantic' }, new Uint8Array([1]))
    const closing = await room.compareExchangeHead(
      { form: 'rev', rev: 'rev' },
      {
        head: {
          rev: 'closing',
          currentInc: 'inc',
          state: 'closing',
          config: new Uint8Array(),
          closeLease: { id: 'lease', durationMs: 15_000 },
        },
      },
      { roomId: 'room', bucket: 'weur' },
    )
    if (closing.kind !== 'committed' || closing.head.closeLease === undefined) throw new Error("the room didn't close")
    // Its directory shows its listing, which then waits for nothing.
    await room.directoryShow()
    const alarm = await storage.getAlarm()
    const leaseEnd = closing.head.closeLease.until
    handedOn.length = 0
    directoryCalls.length = 0
    vi.setSystemTime(leaseEnd + 1)
    await room.alarm()
    expect({
      alarmPastLease: alarm !== null && alarm > leaseEnd && alarm <= leaseEnd + 1,
      state: (await room.readHead())?.state,
      control: (await room.readLaneOrder('inc', { kind: 'control' }))?.seq ?? null,
      semantic: (await room.readLaneOrder('inc', { kind: 'semantic' }))?.seq ?? null,
      handedOn,
      listed: await room.directoryGet(),
      shown: directoryCalls.map(([method, roomId, incTag]) => [method, roomId, incTag]),
    }).toEqual({
      alarmPastLease: true,
      state: 'closed',
      control: null,
      semantic: null,
      handedOn: [1],
      listed: undefined,
      shown: [['show', 'room', null]],
    })
  })
})
