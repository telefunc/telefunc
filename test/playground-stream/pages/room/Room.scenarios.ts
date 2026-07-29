export { roomScenarios, roomScenario, assertRoomScenarioExecutions, type RoomScenarioId }

type RoomScenario = {
  /** Stable browser selector: both the rendered control and the e2e trigger consume it. */
  selector: `#${string}`
}

const roomScenarios = {
  chat: { selector: '#test-room-chat' },
  retain: { selector: '#test-room-retain' },
  participant: { selector: '#test-room-participant' },
  binary: { selector: '#test-room-binary' },
  member: { selector: '#test-room-member' },
  guard: { selector: '#test-room-guard' },
  shield: { selector: '#test-room-shield' },
  admin: { selector: '#test-room-admin' },
  conflate: { selector: '#test-room-conflate' },
  attributes: { selector: '#test-room-attributes' },
  demand: { selector: '#test-room-demand' },
  tail: { selector: '#test-room-tail' },
  hooks: { selector: '#test-room-hooks' },
  memberSub: { selector: '#test-room-member-sub' },
  dmHold: { selector: '#test-room-dm-hold' },
  self: { selector: '#test-room-self' },
  selfServer: { selector: '#test-room-self-server' },
  reconfig: { selector: '#test-room-reconfig' },
  identity: { selector: '#test-room-identity' },
  gcParticipant: { selector: '#test-room-gc-participant' },
} as const satisfies Record<string, RoomScenario>

type RoomScenarioId = keyof typeof roomScenarios

const selectors = Object.values(roomScenarios).map((scenario) => scenario.selector)
if (new Set(selectors).size !== selectors.length) {
  throw new Error('Room scenario reconciliation failed: duplicate selector')
}

function roomScenario(id: RoomScenarioId) {
  return roomScenarios[id]
}

/** Defined by the e2e module as tests are registered; fails before a run if a registry case is omitted or duplicated. */
function assertRoomScenarioExecutions(executed: readonly RoomScenarioId[]) {
  const registered = Object.keys(roomScenarios) as RoomScenarioId[]
  const duplicates = executed.filter((id, index) => executed.indexOf(id) !== index)
  const missing = registered.filter((id) => !executed.includes(id))
  const unknown = executed.filter((id) => !(id in roomScenarios))
  if (duplicates.length || missing.length || unknown.length || executed.length !== registered.length) {
    throw new Error(
      `Room scenario reconciliation failed: registered=${registered.length}, executed=${executed.length}, missing=${missing.join(',') || '-'}, duplicates=${duplicates.join(',') || '-'}, unknown=${unknown.join(',') || '-'}`,
    )
  }
}
