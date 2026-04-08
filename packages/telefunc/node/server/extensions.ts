export type { TelefuncServerExtension }

import type {
  ReplacerType,
  ReviverType,
  TypeContract,
  ServerReplacerContext,
  ServerReviverContext,
} from '../../wire-protocol/types.js'

type TelefuncServerExtension = {
  name: string
  hooks?: {
    onTransformResult?: (ctx: { result: unknown; data: Record<string, unknown> }) => unknown | Promise<unknown>
  }
  /** Custom replacer types for server→client serialization (appended after built-in types). */
  responseTypes?: ReplacerType<TypeContract, ServerReplacerContext>[]
  /** Custom reviver types for client→server deserialization (appended after built-in types). */
  requestTypes?: ReviverType<TypeContract, ServerReviverContext>[]
  /** Custom shield type verifiers. Each entry registers a `shield.type[name]` validator. */
  shieldTypes?: Record<string, (input: unknown) => boolean>
}
