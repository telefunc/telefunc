export { useGenerator }
export type { UseGeneratorState, UseGeneratorOptions, GeneratorError }

import { useCallback, useEffect, useRef, useState } from 'react'
import { abort } from '../../client/abort.js'

type GeneratorError = {
  message: string
  name?: string
  stack?: string
  cause?: unknown
}

type UseGeneratorOptions<T> = {
  /** Called for each yielded value */
  onValue?: (value: T) => void
  /** Called when streaming starts */
  onStart?: () => void
  /** Called when streaming ends (whether completed, aborted, or errored) */
  onEnd?: () => void
  /** Called when an error occurs */
  onError?: (error: GeneratorError) => void
}

type UseGeneratorState<T, Args extends unknown[]> = {
  /** Values yielded by the current invocation (resets on each `invoke`) */
  values: T[]
  /** The most recently yielded value, or undefined if nothing yielded yet */
  lastValue: T | undefined
  /** Whether the generator is currently yielding values */
  isStreaming: boolean
  /** Error thrown during iteration, or null if no error */
  error: GeneratorError | null
  /** Abort the current generator early */
  abort: () => void
  /** Call the telefunction and start streaming */
  invoke: (...args: Args) => void
}

/**
 * @experimental — DO NOT USE unless you reached out to a Telefunc maintainer — this component hook will break upon minor version releases.
 *
 * React hook for consuming a telefunction that returns an AsyncGenerator.
 * Call `invoke(args)` to start streaming, `abort()` to stop.
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
  options?: UseGeneratorOptions<T>,
): UseGeneratorState<T, Args> {
  const [values, setValues] = useState<T[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<GeneratorError | null>(null)

  // The in-flight telefunc call — holds the AbortController telefunc attached to it.
  const callRef = useRef<Promise<AsyncGenerator<T>> | null>(null)
  const fnRef = useRef(fn)
  fnRef.current = fn
  const optionsRef = useRef(options)
  optionsRef.current = options

  const abortCurrent = useCallback(() => {
    if (callRef.current) {
      abort(callRef.current)
      callRef.current = null
    }
    setIsStreaming(false)
  }, [])

  const invoke = useCallback(
    (...args: Args) => {
      // Cancel any previous call before starting a new one.
      abortCurrent()

      const call = fnRef.current(...args)
      callRef.current = call

      setValues([])
      setError(null)
      setIsStreaming(true)
      optionsRef.current?.onStart?.()
      ;(async () => {
        try {
          const gen = await call
          for await (const value of gen) {
            if (callRef.current !== call) break
            setValues((prev) => [...prev, value])
            optionsRef.current?.onValue?.(value)
          }
        } catch (err: unknown) {
          if (callRef.current !== call) return
          const generatorError = toGeneratorError(err)
          setError(generatorError)
          optionsRef.current?.onError?.(generatorError)
        } finally {
          if (callRef.current === call) {
            callRef.current = null
            setIsStreaming(false)
            optionsRef.current?.onEnd?.()
          }
        }
      })()
    },
    [abortCurrent],
  )

  // Abort on unmount.
  useEffect(() => abortCurrent, [abortCurrent])

  return {
    values,
    lastValue: values.length > 0 ? values[values.length - 1]! : undefined,
    isStreaming,
    error,
    abort: abortCurrent,
    invoke,
  }
}

function toGeneratorError(err: unknown): GeneratorError {
  if (err instanceof Error) {
    return { message: err.message, name: err.name, stack: err.stack, cause: err.cause }
  }
  if (err && typeof err === 'object' && 'message' in err && typeof err.message === 'string') {
    return { ...err, message: err.message }
  }
  return { message: typeof err === 'string' ? err : 'Unknown error' }
}
