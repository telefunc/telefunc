export type { TelefuncServerExtension }

type TelefuncServerExtension = {
  name: string
  hooks: {
    onTransformResult?: (ctx: { result: unknown; data: Record<string, unknown> }) => unknown | Promise<unknown>
  }
}
