export { useGenerator }
export type { UseGeneratorState }

import { useCallback, useEffect, useRef, useState } from 'react'

type UseGeneratorState<T, Args extends unknown[]> = {
  /** Values yielded by the current invocation (resets on each `invoke`) */
  values: T[]
  /** The most recently yielded value, or undefined if nothing yielded yet */
  lastValue: T | undefined
  /** All values across all invocations (only resets via `clearHistory`) */
  history: T[]
  /** Whether the generator is currently yielding values */
  isStreaming: boolean
  /** Error thrown during iteration, if any */
  error: unknown
  /** Abort the current generator early */
  abort: () => void
  /** Call the telefunction and start streaming */
  invoke: (...args: Args) => void
  /** Clear the history */
  clearHistory: () => void
}

/**
 * React hook for consuming a telefunction that returns an AsyncGenerator.
 * Event-driven: call `invoke(args)` to start streaming, `abort()` to stop.
 * Cleans up automatically on unmount or when `invoke` is called again.
 *
 * @param fn - A telefunction that returns an `AsyncGenerator<T>`
 *
 * @example
 * ```tsx
 * const { invoke, values, isStreaming } = useGenerator(onChat)
 *
 * <button onClick={() => invoke(prompt)}>Send</button>
 * {values.map((msg, i) => <p key={i}>{msg}</p>)}
 * ```
 */
function useGenerator<T, Args extends unknown[]>(
  fn: (...args: Args) => Promise<AsyncGenerator<T>>,
): UseGeneratorState<T, Args> {
  const [values, setValues] = useState<T[]>([])
  const [history, setHistory] = useState<T[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<unknown>(undefined)

  const versionRef = useRef(0)
  const fnRef = useRef(fn)
  fnRef.current = fn

  const invoke = useCallback((...args: Args) => {
    const version = ++versionRef.current
    const isCurrent = () => version === versionRef.current

    setValues([])
    setError(undefined)
    setIsStreaming(true)
    ;(async () => {
      try {
        const gen = await fnRef.current(...args)
        for await (const value of gen) {
          if (!isCurrent()) break
          setValues((prev) => [...prev, value])
          setHistory((prev) => [...prev, value])
        }
      } catch (err) {
        if (isCurrent()) setError(err)
      } finally {
        if (isCurrent()) setIsStreaming(false)
      }
    })()
  }, [])

  const abort = useCallback(() => {
    versionRef.current++
    setIsStreaming(false)
  }, [])

  const clearHistory = useCallback(() => {
    setHistory([])
  }, [])

  useEffect(() => {
    return () => {
      versionRef.current++
    }
  }, [])

  return {
    values,
    lastValue: values.length > 0 ? values[values.length - 1]! : undefined,
    history,
    isStreaming,
    error,
    abort,
    invoke,
    clearHistory,
  }
}
