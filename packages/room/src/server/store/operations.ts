export { runOperation }

import { getRoomPlane } from '../../backend/plane.js'
import type { CellCounter, RoomOperation } from '../../backend/contract.js'
import { roomClosedError } from '../../shared/errors.js'
import { handoffLost } from '../lanes/lanes.js'
import type { RoomIncarnation } from '../protocol.js'

/** Runs `operation` where the room's state lives, in one atomic step; a closed incarnation throws, and the commit's
 *  handoff is awaited as a control commit's is. */
async function runOperation<Args, Value>(
  room: Pick<RoomIncarnation, 'id' | 'inc'>,
  operation: RoomOperation<Args, Value>,
  args: Args,
): Promise<{ value: Value; counters: Map<string, CellCounter> }> {
  const result = await getRoomPlane().runOperation(room.id, room.inc, operation, args)
  if (result.kind === 'stale-incarnation') throw roomClosedError(room.id)
  if (result.commit?.delivery !== undefined) await handoffLost(room.id, result.commit.delivery)
  return result
}
