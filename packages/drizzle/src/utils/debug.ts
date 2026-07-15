export { createDebug }
export type { Debug }

type Debug = (message: string, ...details: unknown[]) => void

/** Namespaced logger gated by the `DEBUG` env var (`telefunc:drizzle*` or `*`).
 *  Read through `globalThis` so this module needs no Node type dependency and is
 *  inert on edge runtimes where `process` is absent. */
function createDebug(namespace: string): Debug {
  const full = `telefunc:drizzle:${namespace}`
  return (message, ...details) => {
    if (isEnabled(full)) console.log(`[${full}] ${message}`, ...details)
  }
}

function isEnabled(namespace: string): boolean {
  const flag = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.DEBUG
  if (!flag) return false
  return flag.split(',').some((entry) => {
    const pattern = entry.trim()
    return (
      pattern === '*' || pattern === namespace || (pattern.endsWith('*') && namespace.startsWith(pattern.slice(0, -1)))
    )
  })
}
