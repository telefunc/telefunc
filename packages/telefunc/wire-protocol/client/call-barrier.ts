export { addTelefunctionCallBarrier, waitForTelefunctionCallBarriers }

const pending = new Set<Promise<void>>()

/** Preserve client causality when an HTTP telefunction call follows a control sent on a channel:
 * the call waits until the server has accepted that earlier control. */
function addTelefunctionCallBarrier(promise: Promise<unknown>): void {
  const settled = promise.then(
    () => undefined,
    () => undefined,
  )
  pending.add(settled)
  void settled.finally(() => pending.delete(settled))
}

function waitForTelefunctionCallBarriers(): Promise<void> | null {
  if (pending.size === 0) return null
  return Promise.all([...pending]).then(() => undefined)
}
