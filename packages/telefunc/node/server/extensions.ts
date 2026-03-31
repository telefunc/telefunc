export type { TelefuncServerExtension, TelefuncExtensionRegistry }

/** Module-augmentation point for extension data types.
 *
 *  Extensions augment this interface to register their name → data mapping:
 *  ```ts
 *  declare module 'telefunc' {
 *    interface TelefuncExtensionRegistry {
 *      'my-extension': { someField: string }
 *    }
 *  }
 *  ```
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface TelefuncExtensionRegistry {}

/** Base extension type used by the core runtime. */
type TelefuncServerExtensionBase = {
  name: string
  hooks: {
    onTransformResult?: (ctx: { result: unknown; data: any }) => unknown | Promise<unknown>
  }
}

/**
 * When extensions are registered via module augmentation, this is a discriminated union
 * that narrows `ctx.data` based on the `name` field. When no extensions are registered,
 * falls back to the base type.
 *
 * Extension authors use `satisfies TelefuncServerExtension` — the `name` literal
 * automatically narrows `ctx.data` without a manual generic parameter.
 */
type TelefuncServerExtension = keyof TelefuncExtensionRegistry extends never
  ? TelefuncServerExtensionBase
  : {
      [Name in keyof TelefuncExtensionRegistry]: {
        name: Name
        hooks: {
          onTransformResult?: (ctx: {
            result: unknown
            data: TelefuncExtensionRegistry[Name]
          }) => unknown | Promise<unknown>
        }
      }
    }[keyof TelefuncExtensionRegistry]
