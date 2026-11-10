export { watch }

import type { ClientChannel } from 'telefunc'
import { withContext } from 'telefunc/client'
import { EXTENSION_NAME, type LiveResult } from './shared.js'

/** Calls `call`, gives its result to `onData`, and calls it again whenever the result changes on the server.
 *  At most one call runs at a time. Returns `stop`.
 *
 *  ```ts
 *  const stop = watch(() => onGetTodos(userId), (todos) => render(todos))
 *  ```
 */
function watch<T>(
  call: () => Promise<T>,
  onData: (data: T) => void,
  onError: (err: unknown) => void = console.error,
): () => void {
  const liveCall = withContext(call, { extensions: { [EXTENSION_NAME]: {} } })
  let changes: ClientChannel<never, 'changed'> | undefined
  let stopped = false
  let running = false
  let changedWhileRunning = false
  void refetch()
  return () => {
    stopped = true
    void changes?.close()
  }

  async function refetch(): Promise<void> {
    if (running) {
      changedWhileRunning = true
      return
    }
    running = true
    do {
      changedWhileRunning = false
      await fetchOnce()
    } while (changedWhileRunning && !stopped)
    running = false
  }

  async function fetchOnce(): Promise<void> {
    let result: LiveResult
    try {
      result = (await liveCall()) as unknown as LiveResult
    } catch (err) {
      if (!stopped) onError(err)
      return
    }
    void changes?.close()
    changes = result.changes
    if (stopped) {
      void changes.close()
      return
    }
    changes.listen(() => void refetch())
    onData(result.data as T)
  }
}
