export { onServerAction, runServerAction }

// What a telefunction leaves for a test to start while the page can't reach the server (it's offline).
const actions: Map<string, () => void> = ((globalThis as Record<symbol, unknown>)[
  Symbol.for('playground-stream:serverActions')
] ??= new Map<string, () => void>()) as Map<string, () => void>

function onServerAction(key: string, action: () => void) {
  actions.set(key, action)
}

/** Returns false when no telefunction left an action under `key`. */
function runServerAction(key: string): boolean {
  const action = actions.get(key)
  if (!action) return false
  action()
  return true
}
