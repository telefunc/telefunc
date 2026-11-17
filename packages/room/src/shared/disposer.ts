export { makeDisposer, replayToLateListener }
export type { ListenerOptions }

import { invokeChannelListener, untether } from 'telefunc/__internal'

/** A listener registration's options: aborting `signal` unregisters it, as its returned function does. */
type ListenerOptions = { signal?: AbortSignal }

/** A one-shot cleanup handle, run by the call, by `group`'s owner, or by `signal`'s abort; no action creates an
 *  already-terminal handle. A registration checks `signal.aborted` first: an aborted one registers nothing. */
function makeDisposer(dispose?: () => void, group?: Set<() => void>, signal?: AbortSignal): () => void {
  let action = dispose
  const token = () => {
    const current = action
    action = undefined
    group?.delete(token)
    signal?.removeEventListener('abort', token)
    untether(token)
    current?.()
  }
  if (action) {
    group?.add(token)
    signal?.addEventListener('abort', token, { once: true })
  } else untether(token)
  return token
}

/** A listener added after the terminal event it listens for (a close, a leave) hears it at once, unless `signal`
 *  aborted; there is nothing left to unregister. */
function replayToLateListener<Args extends unknown[]>(
  cb: (...args: Args) => unknown,
  args: Args,
  options: ListenerOptions | undefined,
  reportError: (error: unknown) => void,
): () => void {
  if (!options?.signal?.aborted) invokeChannelListener(cb, args, reportError)
  return makeDisposer()
}
