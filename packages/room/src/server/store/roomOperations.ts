export { ROOM_OPERATIONS, operationRequest, recordRequest }
export type { RoomOperationRequest }

import type { RoomOperation } from '../../backend/contract.js'
import { recordOperation, type OperationReads, type OperationWrites } from '../../backend/operation.js'
import { assert } from '../../utils/assert.js'
import { STORE_DM_REPLY } from './dmReplies.js'
import { CLAIM_LEASE, HOLD_VIEWS, REMOVE_LEASE, REMOVE_VIEW, RENEW_LEASE } from './lease.js'
import { JOIN, LEAVE } from './members.js'
import { SET_MEMBER_CONNECTION, SET_MEMBER_META } from './meta.js'

const OPERATIONS = {
  join: JOIN,
  leave: LEAVE,
  setMemberMeta: SET_MEMBER_META,
  setMemberConnection: SET_MEMBER_CONNECTION,
  holdViews: HOLD_VIEWS,
  removeView: REMOVE_VIEW,
  renewLease: RENEW_LEASE,
  claimLease: CLAIM_LEASE,
  removeLease: REMOVE_LEASE,
  storeDmReply: STORE_DM_REPLY,
}

type OperationArgs = { [Name in keyof typeof OPERATIONS]: Parameters<(typeof OPERATIONS)[Name]['plan']>[1] }

/** Room's operations by name, which a Cloudflare room authority runs by the name its call carries. */
const ROOM_OPERATIONS: { readonly [Name in keyof OperationArgs]: RoomOperation<OperationArgs[Name], unknown> } =
  OPERATIONS

/** An operation as a call to a room authority carries it. */
type RoomOperationRequest<Name extends keyof OperationArgs = keyof OperationArgs> = {
  [N in Name]: { name: N; args: OperationArgs[N] }
}[Name]

/** `operation`, one of Room's, with `args`, as a request. */
function operationRequest<Args>(operation: RoomOperation<Args, unknown>, args: Args): RoomOperationRequest {
  const name = operation.name as keyof OperationArgs
  assert(ROOM_OPERATIONS[name] === operation)
  return { name, args } as RoomOperationRequest
}

/** Runs the plan of the operation `request` names over `reads`. */
function recordRequest<Name extends keyof OperationArgs>(
  request: RoomOperationRequest<Name>,
  reads: OperationReads,
): { value: unknown; writes: OperationWrites } {
  return recordOperation(ROOM_OPERATIONS[request.name], request.args, reads)
}
