export { runOperation, settledOperation }

import type {
  CellCounter,
  OperationArgs,
  OperationName,
  OperationOf,
  OperationOptions,
  OperationResult,
  OperationValue,
} from '../../backend/contract.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { roomClosedError } from '../../shared/errors.js'
import { handoffLost } from '../lanes/lanes.js'
import type { RoomIncarnation } from '../protocol.js'

/** Runs `operation` where the room's state lives, in one atomic step, as `settledOperation()` settles it. */
async function runOperation<N extends OperationName>(
  store: RoomStore,
  room: Pick<RoomIncarnation, 'id' | 'inc'>,
  operation: OperationOf<N>,
  args: OperationArgs[N],
  options?: OperationOptions,
): Promise<{ value: OperationValue[N]; counters: Map<string, CellCounter> }> {
  return settledOperation(room, await store.runOperation(room.id, room.inc, operation, args, options))
}

/** An operation's value and counters: a closed incarnation throws, and the commit's handoff is awaited as a control
 *  commit's is. */
async function settledOperation<Value>(
  room: Pick<RoomIncarnation, 'id'>,
  result: OperationResult<Value>,
): Promise<{ value: Value; counters: Map<string, CellCounter> }> {
  if (result.kind === 'stale-incarnation') throw roomClosedError(room.id)
  if (result.commit?.delivery !== undefined) await handoffLost(room.id, result.commit.delivery)
  return result
}
