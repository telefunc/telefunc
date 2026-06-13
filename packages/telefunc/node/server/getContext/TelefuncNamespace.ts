// TO-DO/next-major-release: remove
export declare namespace Telefunc {
  /**
   * @deprecated Replace `declare module 'telefunc'` with `declare global`:
   *
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
     * Set the type of the `context` object (`const context = getContext()`).
     *
     * https://telefunc.com/getContext#typescript
     */
    interface Context {}
  }
}
