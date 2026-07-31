export { roomScenarios, roomScenario, assertRoomScenarioExecutions, type RoomScenarioId }

const roomScenarios = [
  'chat',
  'retain',
  'participant',
  'binary',
  'member',
  'guard',
  'shield',
  'admin',
  'conflate',
  'attributes',
  'demand',
  'tail',
  'hooks',
  'member-sub',
  'dm-hold',
  'self',
  'self-server',
  'reconfig',
  'identity',
  'gc-participant',
] as const

type RoomScenarioId = (typeof roomScenarios)[number]

function roomScenario(id: RoomScenarioId) {
  return { selector: `#test-room-${id}` as const }
}

/** Defined by the e2e module as tests are registered; fails before a run if a registry case is omitted or duplicated. */
function assertRoomScenarioExecutions(executed: readonly RoomScenarioId[]) {
  const duplicates = executed.filter((id, index) => executed.indexOf(id) !== index)
  const missing = roomScenarios.filter((id) => !executed.includes(id))
  if (duplicates.length || missing.length || executed.length !== roomScenarios.length) {
    throw new Error(
      `Room scenario reconciliation failed: registered=${roomScenarios.length}, executed=${executed.length}, missing=${missing.join(',') || '-'}, duplicates=${duplicates.join(',') || '-'}`,
    )
  }
}
