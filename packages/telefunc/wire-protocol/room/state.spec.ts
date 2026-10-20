import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { DEFAULT_TRACK } from './binary.js'
import { disposeRoomBackend, installRoomBackend, newState } from '../test/room.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
describe('RoomState', () => {
  it('records the events of members it does not know for a roster read, and narrates only what a roster changes', async () => {
    const state = newState({ roomId: 'unknown-member-epoch' })
    const id = crypto.randomUUID()
    const events = await state.model.recordingMembership(
      async () => {
        state.model.applyParticipantMeta(id, { seq: 1, meta: { step: 1 } })
        state.model.applyLeave(id, { type: 'left' })
      },
      (_, events) => events,
    )
    expect(events).toEqual([{ meta: { id, update: { seq: 1, meta: { step: 1 } } } }, { leave: id }])
    const member = { id, meta: {}, joinedAt: 1, metaSeq: 0 }
    expect(await state.model.reconcileRoster({ members: [member], departing: new Set() }, [])).toBe(true)
    let changes = 0
    state.onChange(() => changes++)
    const reconcile = () =>
      state.model.reconcileRoster({ members: [{ ...member, metaSeq: 1 }], departing: new Set() }, [])
    expect(await reconcile()).toBe(true)
    expect(changes).toBe(1)
    expect(await reconcile()).toBe(false)
    expect(changes).toBe(1)
  })
  it('reconciles every member to the final state and preserves semantic joins', async () => {
    const alice = { id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0 }
    const bob = { id: crypto.randomUUID(), meta: {}, joinedAt: 2, metaSeq: 0 }
    const carol = { id: crypto.randomUUID(), meta: {}, joinedAt: 3, metaSeq: 0 }
    const state = newState({ roomId: 'reconcile-change-count', seed: { members: [alice] } })
    const observed: string[][] = []
    const joins: string[] = []
    state.onChange(() => observed.push([...state.model.memberIds()]))
    state.onJoin((member) => joins.push(member.id))
    await state.model.reconcileRoster({ members: [alice, bob, carol], departing: new Set() }, [])
    expect(observed.at(-1)).toEqual([alice.id, bob.id, carol.id])
    expect(joins).toEqual([bob.id, carol.id])
  })
  it('reports rejected async RoomState callbacks without awaiting delivery', async () => {
    const memberId = crypto.randomUUID()
    const errors: unknown[] = []
    const state = newState({
      roomId: 'async-state-callbacks',
      seed: { members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }] },
      onCallbackError: (error) => errors.push(error),
    })
    const remote = state.getRemote(memberId)!
    const failures = Array.from({ length: 5 }, (_, index) => new Error(`async state callback ${index}`))
    const rejected = failures.map((failure) => {
      const promise = Promise.reject(failure)
      void promise.catch(() => {})
      return promise
    })
    state.subscribe(() => rejected[0])
    remote.subscribe(() => rejected[1])
    state.subscribeBinary(() => rejected[2])
    remote.subscribeBinary(() => rejected[3])
    state.onClose(() => rejected[4])
    const info = { key: state.roomId, seq: 1, timestamp: 1 }
    state.applyData({ __r: 'data', from: memberId, fromMeta: {}, data: 'text' }, info)
    state.applyBinary(
      { from: memberId, payload: new Uint8Array(), track: DEFAULT_TRACK, meta: null, retain: false },
      info,
    )
    state.model.applyClosed()
    await Promise.resolve()
    expect(errors).toEqual(failures)
  })
  it("gives each binary listener bytes of its own, so one that transfers them leaves the next one's and the frame's intact", () => {
    const memberId = crypto.randomUUID()
    const state = newState({
      roomId: 'binary-ownership',
      seed: { members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }] },
    })
    const seen: number[][] = []
    const transfer = (bytes: Uint8Array) => structuredClone(bytes.buffer, { transfer: [bytes.buffer] })
    state.subscribeBinary(transfer)
    state.subscribeBinary((bytes) => seen.push([...bytes]))
    state.getRemote(memberId)!.subscribeBinary(transfer)
    state.getRemote(memberId)!.subscribeBinary((bytes) => seen.push([...bytes]))
    const payload = new Uint8Array([1, 2, 3])
    state.applyBinary(
      { from: memberId, payload, track: DEFAULT_TRACK, meta: null, retain: false },
      { key: state.roomId, seq: 1, timestamp: 1 },
    )
    expect(seen).toEqual([
      [1, 2, 3],
      [1, 2, 3],
    ])
    expect([...payload]).toEqual([1, 2, 3])
  })
})
