export declare namespace Telefunc {
  /**
   * Globally set the type of the `context` object (`const context = getContext()`).
   *
   * https://telefunc.com/getContext#typescript
   *
   * @deprecated Augment the global `Telefunc` namespace instead of the `'telefunc'` module:
   * ```ts
   * // TelefuncContext.d.ts
   *
   * import type { User } from './User.js'
   *
   * declare global {
   *   namespace Telefunc {
   *     interface Context {
   *       user: null | User
   *     }
   *   }
   * }
   * ```
   */
  export interface Context extends globalThis.Telefunc.Context {}
}

declare global {
  namespace Telefunc {
    /**
     * Globally set the type of the `context` object (`const context = getContext()`).
     *
     * https://telefunc.com/getContext#typescript
     */
    // Can be overridden by the user
    interface Context {}
  }
}
