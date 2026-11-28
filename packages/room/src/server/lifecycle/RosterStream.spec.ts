import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import type { PublishBody } from 'telefunc/__internal'
import type { MemberSnapshot } from '../../shared/protocol.js'
import { ROOM_PLAN_SLICE_MS } from '../../shared/constants.js'
import { RoomModel } from '../../shared/RoomModel.js'
import { ROOM_ROSTER_PART_MEMBERS } from '../constants.js'
import { RosterStream, type RosterClient } from './RosterStream.js'

/** The part index of each roster body the stream built. */
const partOf = vi.hoisted(() => new WeakMap<object, number>())
vi.mock('../publishBodies.js', async (importOriginal) => {
  const bodies = await importOriginal<typeof import('../publishBodies.js')>()
  return {
    ...bodies,
    unorderedEventBody(event: Parameters<typeof bodies.unorderedEventBody>[0]) {
      const body = bodies.unorderedEventBody(event)
      if (event.__r === 'roster') partOf.set(body, event.part)
      return body
    },
  }
})

beforeEach(() => {
  // A slice runs out only when a test moves the clock.
  vi.useFakeTimers({ toFake: ['performance'] })
})
afterEach(() => {
  vi.useRealTimers()
})

/** A roster of three parts. */
const members: MemberSnapshot[] = Array.from({ length: 2 * ROOM_ROSTER_PART_MEMBERS + 1 }, (_, i) => ({
  id: `member-${i}`,
  meta: {},
  joinedAt: i,
  metaSeq: 0,
}))

function roomModel(): RoomModel {
  return new RoomModel({
    roomId: 'room',
    meta: [],
    seed: { members },
    metaSeq: 0,
    owner: { onMemberLeft() {}, onMemberMeta() {} },
  })
}

/** A client that records the index of each roster part it gets; `onPart` runs once it got one. */
function rosterClient(onPart: (part: number) => void = () => {}): RosterClient & { parts: number[] } {
  const parts: number[] = []
  return {
    parts,
    relayEventBody(body: PublishBody) {
      const part = partOf.get(body)
      if (part === undefined) return
      parts.push(part)
      onPart(part)
    },
    rosterMembers: (_part, visible) => visible,
  }
}

/** Until every slice waiting in the process resumed and ran out. */
async function settled(): Promise<void> {
  for (let turn = 0; turn < 10; turn++) await new Promise((resolve) => setImmediate(resolve))
}

// The page's `RosterIntake` abandons a roster whose next part isn't the one after the last it got
// (`client/RosterIntake.ts`): a client gets each roster from its first part on, with no part skipped.
test("a client sent a roster again while its last one streams gets the new one from its first part, and the old one's parts stop", async () => {
  let pauseAfterFirstPart = true
  const client = rosterClient((part) => {
    // The roster's first part ran its slice out: the stream waits for its next turn before part 1.
    if (part === 0 && pauseAfterFirstPart) vi.advanceTimersByTime(ROOM_PLAN_SLICE_MS)
  })
  const stream = new RosterStream({ model: roomModel(), clients: new Set([client]) })
  stream.sendTo(client)
  expect(client.parts).toEqual([0])
  pauseAfterFirstPart = false
  stream.sendTo(client)
  await settled()
  expect(client.parts).toEqual([0, 0, 1, 2])
  expect(stream.sentTo(client)).toBe(true)
})

test('a roster that corrected a drift reaches a client from its first part, while the roster it was being sent waits for its next part', async () => {
  const model = roomModel()
  let pauseAfterFirstPart = true
  const streaming = rosterClient((part) => {
    if (part === 0 && pauseAfterFirstPart) vi.advanceTimersByTime(ROOM_PLAN_SLICE_MS)
  })
  const other = rosterClient()
  const stream = new RosterStream({ model, clients: new Set([streaming, other]) })
  stream.sendTo(streaming)
  pauseAfterFirstPart = false
  const joined: MemberSnapshot = { id: 'joined', meta: {}, joinedAt: members.length, metaSeq: 0 }
  expect(await stream.reconcile({ members: [...members, joined] }, [])).toBe(true)
  await settled()
  expect(streaming.parts).toEqual([0, 0, 1, 2])
  expect(other.parts).toEqual([0, 1, 2])
})
