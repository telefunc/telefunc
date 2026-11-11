// The lane plan's thresholds, measured on the memory driver (`ARCHITECTURE.md`, "The lane plan"): where the
// `byMember` band switches and how many subscriptions a switch opens and closes, what the per-member overflow rule
// costs while a member's named tracks hover at the limit, and what a planning pass costs as the room grows.
// The members are joined as another server's (their own lease), so this process holds only the views' lanes.
// Run after `pnpm run build`: `node packages/room/probes/lane-plan.mjs`.
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { Room } from '@telefunc/room'
import { getRoomPlane } from '@telefunc/room/backend'
import { joinMember } from '../dist/server/store/members.js'
import { RoomLease } from '../dist/server/store/lease.js'
import { handleOf } from '../dist/server/room.js'

const BINARY_KINDS = ['binary', 'binary-all', 'binary-track']
const open = new Map(BINARY_KINDS.map((kind) => [kind, 0]))
let churn = 0
const plane = getRoomPlane()
const subscribeLane = plane.subscribeLane.bind(plane)
plane.subscribeLane = (roomId, inc, lane, listener) => {
  const subscription = subscribeLane(roomId, inc, lane, listener)
  if (!open.has(lane.kind)) return subscription
  open.set(lane.kind, open.get(lane.kind) + 1)
  churn++
  const unsubscribe = subscription.unsubscribe.bind(subscription)
  let ended = false
  subscription.unsubscribe = () => {
    if (!ended) {
      ended = true
      open.set(lane.kind, open.get(lane.kind) - 1)
      churn++
    }
    return unsubscribe()
  }
  return subscription
}
const lanes = () =>
  `${open.get('binary')} named, ${open.get('binary-all')} every-track, ${open.get('binary-track')} every-member`

async function newRoom(members) {
  const room = await Room.create(`probe-${crypto.randomUUID()}`, { emptyTimeout: Infinity, departureTimeout: Infinity })
  room.onJoin(() => {})
  const { incarnation, replica } = handleOf(room)
  const lease = new RoomLease()
  const ids = []
  for (let i = 0; i < members; i++) {
    const id = `m${i}`
    const joinedAt = Date.now()
    ids.push(id)
    await joinMember(incarnation, lease, {
      id,
      record: { joinedAt, metaSeq: 0, metaGen: 0 },
      meta: {},
      join: { __r: 'join', id, meta: {}, joinedAt },
    })
  }
  while (room.count < members) await new Promise((resolve) => setTimeout(resolve, 10))
  // The plan's wait for its pass and the lanes it opened; nothing public waits for both.
  const settled = () => replica.lanePlan.binaryReady()
  return { room, ids, settled }
}

/** A want change's pass: its time until the lanes it opened are ready, and the longest the event loop waited. */
async function timed(change, settled) {
  const delay = monitorEventLoopDelay({ resolution: 1 })
  delay.enable()
  const start = performance.now()
  change()
  await settled()
  const ms = performance.now() - start
  delay.disable()
  return { ms: ms.toFixed(1), maxLoopDelayMs: (delay.max / 1e6).toFixed(1) }
}

async function band(members) {
  const { room, settled } = await newRoom(members)
  const isByMember = () => open.get('binary-all') === members && open.get('binary-track') === 0
  const unsubscribes = []
  const switches = []
  let mode = false
  const step = async (tracks) => {
    const before = churn
    await settled()
    if (isByMember() !== mode) {
      mode = !mode
      switches.push(
        `${mode ? 'on' : 'off'} at ${tracks} tracks: ${lanes()}, ${churn - before} subscribes and unsubscribes`,
      )
    }
  }
  for (let k = 1; k <= 60; k++) {
    unsubscribes.push(room.subscribeBinary(() => {}, { track: `t${k}` }))
    await step(k)
  }
  while (unsubscribes.length > 0) {
    unsubscribes.pop()()
    await step(unsubscribes.length)
  }
  await Room.close(room.id)
  return switches
}

async function bandHover(members, low, high, times) {
  const { room, settled } = await newRoom(members)
  const unsubscribes = []
  for (let k = 1; k <= low; k++) unsubscribes.push(room.subscribeBinary(() => {}, { track: `t${k}` }))
  await settled()
  const cycles = []
  for (let i = 0; i < times; i++) {
    const before = churn
    for (let k = low + 1; k <= high; k++) unsubscribes.push(room.subscribeBinary(() => {}, { track: `t${k}` }))
    await settled()
    for (let k = low + 1; k <= high; k++) unsubscribes.pop()()
    await settled()
    cycles.push(churn - before)
  }
  await Room.close(room.id)
  return cycles
}

async function overflowHover(times) {
  const { room, ids, settled } = await newRoom(1)
  const member = await room.getParticipant(ids[0])
  const unsubscribes = []
  for (let k = 1; k <= 16; k++) unsubscribes.push(member.subscribeBinary(() => {}, { track: `n${k}` }))
  await settled()
  const at16 = lanes()
  const cycles = []
  for (let i = 0; i < times; i++) {
    const before = churn
    unsubscribes.push(member.subscribeBinary(() => {}, { track: 'n17' }))
    await settled()
    unsubscribes.pop()()
    await settled()
    cycles.push(churn - before)
  }
  unsubscribes.push(member.subscribeBinary(() => {}, { track: 'n17' }))
  await settled()
  const at17 = lanes()
  await Room.close(room.id)
  return { at16, at17, perCycle: cycles }
}

async function planningCost(members) {
  const { room, settled } = await newRoom(members)
  const oneTrack = await timed(() => room.subscribeBinary(() => {}, { track: 'video' }), settled)
  const everyTrack = members <= 10_000 ? await timed(() => room.subscribeBinary(() => {}), settled) : null
  await Room.close(room.id)
  return { members, oneTrackOfEveryMember: oneTrack, everyTrackOfEveryMember: everyTrack }
}

console.log('byMember band, tracks named of every member rising from 1 to 60, then falling:')
for (const members of [10, 20, 40]) console.log(`  ${members} members:`, await band(members))
console.log(
  'byMember band hovering, 20 members, 20 <-> 21 tracks, subscribes and unsubscribes per cycle:',
  await bandHover(20, 20, 21, 10),
)
console.log(
  'Overflow rule hovering, one member, 16 <-> 17 named tracks, subscribes and unsubscribes per cycle:',
  await overflowHover(10),
)
console.log('Planning cost:')
for (const members of [1_000, 10_000, 100_000]) console.log(' ', await planningCost(members))
process.exit(0)
