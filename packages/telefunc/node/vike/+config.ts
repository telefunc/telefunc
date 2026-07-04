export { config as default }

import type { Config } from 'vike/types'

/**
 * Vike extension adding Telefunc's universal middleware to Vike's `+middleware`.
 *
 * https://telefunc.com/vike
 *
 * @example
 * ```ts
 * // +config.ts
 * import telefunc from 'telefunc/vike'
 *
 * export default {
 *   extends: [telefunc],
 * }
 * ```
 */
const config = {
  name: 'telefunc',
  // Adds Telefunc's universal middleware to Vike's `+middleware`.
  // It's a Vike pointer import resolved (and imported on the server-side) by Vike at runtime.
  // https://vike.dev/config#pointer-imports
  middleware: 'import:telefunc/universal-middleware:default' as unknown as Config['middleware'],
} satisfies Config
