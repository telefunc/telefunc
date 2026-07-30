export { addTelefunctionCallBarrier, waitForTelefunctionCallBarriers }
export type { TelefunctionCallBarrierScope }

type TelefunctionCallBarrierScope = { telefuncUrl: string; connectionKey?: string }

const pendingByScope = new Map<string, Set<Promise<void>>>()

/** Preserve causality only between controls and HTTP calls sharing one client connection scope. */
function addTelefunctionCallBarrier(scope: TelefunctionCallBarrierScope, promise: Promise<unknown>): void {
  const key = scopeKey(scope)
  let pending = pendingByScope.get(key)
  if (pending === undefined) {
    pending = new Set()
    pendingByScope.set(key, pending)
  }
  const barrier = promise.then(() => undefined)
  // Mark the stored branch observed while retaining its rejection for a dependent Promise.all().
  void barrier.catch(() => {})
  pending.add(barrier)
  const remove = () => {
    pending!.delete(barrier)
    if (pending!.size === 0) pendingByScope.delete(key)
  }
  void barrier.then(remove, remove)
}

function waitForTelefunctionCallBarriers(scope: TelefunctionCallBarrierScope): Promise<void> | null {
  const pending = pendingByScope.get(scopeKey(scope))
  if (pending === undefined || pending.size === 0) return null
  return Promise.all([...pending]).then(() => undefined)
}

function scopeKey({ telefuncUrl, connectionKey }: TelefunctionCallBarrierScope): string {
  return JSON.stringify([telefuncUrl, connectionKey ?? null])
}
