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
  /** Lifecycle hooks. All run in context (`getContext()`/`getRawContext()` work). All ALWAYS run —
   *  presence of the request's extension `data` no longer gates them; `data` is simply passed through
   *  when the request carried it. See `runTelefunc/executeTelefunction.ts` for the phase state machine. */
  hooks?: {
    /** Before the body. */
    onTelefunctionStart?: (ctx: { data?: Record<string, unknown> }) => void | Promise<void>
    /** After the body resolves (success only), before serialization. Result hooks chain in registration
     *  order — each receives the previous hook's returned result; the return value replaces the result. */
    onTelefunctionResult?: (ctx: { result: unknown; data?: Record<string, unknown> }) => unknown | Promise<unknown>
    /** At settle, on every path. `error` is the primary error when `outcome === 'error'`. */
    onTelefunctionSettled?: (ctx: {
      outcome: 'success' | 'error' | 'abort'
      error?: unknown
      data?: Record<string, unknown>
    }) => void | Promise<void>
  }
  /** Custom replacer types for server→client serialization (appended after built-in types). */
  responseTypes?: ReplacerType<TypeContract, ServerReplacerContext>[]
  /** Custom reviver types for client→server deserialization (appended after built-in types). */
  requestTypes?: ReviverType<TypeContract, ServerReviverContext>[]
  /** Custom shield type verifiers. Each entry registers a `shield.type[name]` validator. */
  shieldTypes?: Record<string, (input: unknown) => boolean>
}
