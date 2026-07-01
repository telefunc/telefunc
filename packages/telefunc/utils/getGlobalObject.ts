export function getGlobalObject<T extends Record<string, unknown> = never>(
  // We use the filename as key; each `getGlobalObject()` call should live inside a file with a unique filename.
  key: `${string}.ts`,
  defaultValue: T | (() => T),
): T {
  const globalObjectsAll = ((globalThis as any)[projectKey] = (globalThis as any)[projectKey] || {})
  if (globalObjectsAll[key]) return globalObjectsAll[key]
  globalObjectsAll[key] = typeof defaultValue === 'function' ? (defaultValue as () => T)() : defaultValue
  return globalObjectsAll[key]
}
const projectKey = '_telefunc'
