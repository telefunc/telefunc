// TO-DO/next-major-release: remove, see https://github.com/telefunc/telefunc/commit/7758568044edb4d4008c11be0667c13cedb7a149
export declare namespace Telefunc {
  /**
   * @deprecated Replace `declare module 'telefunc'` with `declare global`
   *
   * https://telefunc.com/getContext#typescript
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
