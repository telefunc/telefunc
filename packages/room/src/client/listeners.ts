export { installAbortableListeners }

import { untether } from 'telefunc/__internal'
import { makeDisposer } from '../shared/disposer.js'

/** Add cancellation at the client boundary; the original registrations still own their lifecycle. */
function installAbortableListeners(target: object, names: readonly string[], end: string): void {
  const methods = target as Record<string, (...args: any[]) => () => void>
  const onEnd = methods[end]!.bind(target)
  for (const name of names) {
    const register = methods[name]!.bind(target)
    methods[name] = (callback: (...args: any[]) => unknown, options?: { signal?: AbortSignal }) => {
      const { signal, ...registrationOptions } = options ?? {}
      if (!signal) return register(callback, registrationOptions)
      if (signal.aborted) return makeDisposer()
      let remove: (() => void) | undefined
      let removeEnd: (() => void) | undefined
      let ended = false
      const dispose = () => {
        ended = true
        untether(dispose)
        signal.removeEventListener('abort', dispose)
        remove?.()
        removeEnd?.()
      }
      signal.addEventListener('abort', dispose, { once: true })
      try {
        remove = register((...args: unknown[]) => {
          if (!ended && !signal.aborted) return callback(...args)
        }, registrationOptions)
        removeEnd = onEnd(dispose)
        if (ended) dispose()
        return dispose
      } catch (error) {
        dispose()
        throw error
      }
    }
  }
}
