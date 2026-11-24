export { ROOM_OPERATIONS, operationRequest, recordRequest }
export type { OperationName, OperationArgs, OperationValue, OperationOf, RoomOperationRequest }

import type { RoomOperation } from '../../backend/contract.js'
import { recordOperation, type OperationReads, type OperationWrites } from '../../backend/operation.js'
import { STORE_DM_REPLY } from './dmReplies.js'
import { CLAIM_LEASE, HOLD_VIEWS, REMOVE_LEASE, REMOVE_VIEW, RENEW_CLAIM, RENEW_LEASE } from './lease.js'
import { JOIN, LEAVE } from './members.js'
import { SET_MEMBER_CONNECTION, SET_MEMBER_META, SET_ROOM_META } from './meta.js'

const OPERATIONS = {
  join: JOIN,
  leave: LEAVE,
  setRoomMeta: SET_ROOM_META,
  setMemberMeta: SET_MEMBER_META,
  setMemberConnection: SET_MEMBER_CONNECTION,
  holdViews: HOLD_VIEWS,
  removeView: REMOVE_VIEW,
  renewLease: RENEW_LEASE,
  claimLease: CLAIM_LEASE,
  renewClaim: RENEW_CLAIM,
  removeLease: REMOVE_LEASE,
  storeDmReply: STORE_DM_REPLY,
}

type OperationName = keyof typeof OPERATIONS
type OperationArgs = { [N in OperationName]: Parameters<(typeof OPERATIONS)[N]['plan']>[1] }
type OperationValue = { [N in OperationName]: ReturnType<(typeof OPERATIONS)[N]['plan']> }
/** Room's operation named `N`. */
type OperationOf<N extends OperationName> = RoomOperation<N, OperationArgs[N], OperationValue[N]>

/** Room's operations by name, each under its own: a Cloudflare room authority runs the one a call names. */
const ROOM_OPERATIONS: { readonly [N in OperationName]: OperationOf<N> } = OPERATIONS

/** An operation as a call to a room authority carries it. */
type RoomOperationRequest<N extends OperationName = OperationName> = {
  [K in N]: { name: K; args: OperationArgs[K] }
}[N]

/** `operation` with `args`, as a request. */
function operationRequest<N extends OperationName>(
  operation: OperationOf<N>,
  args: OperationArgs[N],
): RoomOperationRequest<N> {
  return { name: operation.name, args }
}

/** Runs the plan of the operation `request` names over `reads`. */
function recordRequest<N extends OperationName>(
  request: RoomOperationRequest<N>,
  reads: OperationReads,
): { value: OperationValue[N]; writes: OperationWrites } {
  return recordOperation(ROOM_OPERATIONS[request.name], request.args, reads)
}
