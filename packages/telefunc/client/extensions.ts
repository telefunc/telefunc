export type { TelefuncClientExtension }

import type {
  ReplacerType,
  ReviverType,
  TypeContract,
  ClientReplacerContext,
  ClientReviverContext,
} from '../wire-protocol/types.js'

type TelefuncClientExtension = {
  name: string
  /** Custom reviver types for server→client deserialization (appended after built-in types). */
  responseTypes?: ReviverType<TypeContract, ClientReviverContext>[]
  /** Custom replacer types for client→server serialization (appended after built-in types). */
  requestTypes?: ReplacerType<TypeContract, ClientReplacerContext>[]
}
