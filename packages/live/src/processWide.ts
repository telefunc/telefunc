export { processWide }

const GLOBAL = Symbol.for('@telefunc/live')

/**
 * The one `name` of the process, created by whichever copy of this package asks first. An app may install several
 * copies (two versions, or a bundle besides `node_modules`): a `.live()` query of one copy runs statements another
 * copy captures, a `derived()` of one reads values of another, and two copies wrap one database client.
 */
function processWide<T>(name: string, create: () => T): T {
  const shared: Record<string, unknown> = ((globalThis as any)[GLOBAL] ??= {})
  return (shared[name] ??= create()) as T
}
