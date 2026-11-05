export type { TelefuncClientExtension, LazyResponseTypes }

import type {
  ReplacerType,
  ReviverType,
  TypeContract,
  ClientReplacerContext,
  ClientReviverContext,
} from '../wire-protocol/values/types.js'

type TelefuncClientExtension = {
  name: string
  /** Custom reviver types for server→client deserialization (appended after built-in types). */
  responseTypes?: ReviverType<TypeContract, ClientReviverContext>[]
  /** Custom replacer types for client→server serialization (appended after built-in types). */
  requestTypes?: ReplacerType<TypeContract, ClientReplacerContext>[]
  /** Reviver types used only by a response whose body contains `stem`; that response waits for `load()`. */
  lazyResponseTypes?: LazyResponseTypes
}

type LazyResponseTypes = {
  stem: string
  load: () => Promise<ReviverType<TypeContract, ClientReviverContext>[]>
}
