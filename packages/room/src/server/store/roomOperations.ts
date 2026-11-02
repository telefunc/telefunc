export { ROOM_OPERATIONS }

import type { RoomOperation } from '../../backend/contract.js'
import { JOIN, LEAVE } from './members.js'
import { SET_MEMBER_META } from './meta.js'

/** Room's operations by name, which a Cloudflare room authority runs by the name its call carries. */
const ROOM_OPERATIONS: ReadonlyMap<string, RoomOperation<never, unknown>> = new Map(
  [JOIN, LEAVE, SET_MEMBER_META].map((operation) => [operation.name, operation as RoomOperation<never, unknown>]),
)
