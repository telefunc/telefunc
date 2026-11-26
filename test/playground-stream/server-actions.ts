export { onServerAction, forgetServerAction, runServerAction }

// What a telefunction leaves for a test to start while the page can't reach the server (it's offline).
declare global {
  /** The server actions of every copy of this module the server loaded. */
  var playgroundServerActions: Map<string, { action: () => void; once: boolean }> | undefined
}
const actions = (globalThis.playgroundServerActions ??= new Map())

/** `once`, the default: the action is let go as it runs, with what it holds. */
function onServerAction(key: string, action: () => void, { once = true } = {}) {
  actions.set(key, { action, once })
}

/** Lets go of an action no test will run. */
function forgetServerAction(key: string) {
  actions.delete(key)
}

/** Returns false when no telefunction left an action under `key`. */
function runServerAction(key: string): boolean {
  const entry = actions.get(key)
  if (!entry) return false
  if (entry.once) actions.delete(key)
  entry.action()
  return true
}
