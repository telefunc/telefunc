export { useGenerator }
export type { UseGeneratorState, UseGeneratorOptions, GeneratorError }

import { useCallback, useEffect, useRef, useState } from 'react'

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
  fn: (...args: Args) => Promise<AsyncGenerator<T>> | AsyncGenerator<T>,
  options?: UseGeneratorOptions<T>,
): UseGeneratorState<T, Args> {
  const [values, setValues] = useState<T[]>([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<GeneratorError | null>(null)

  const versionRef = useRef(0)
  const fnRef = useRef(fn)
  fnRef.current = fn
  const optionsRef = useRef(options)
  optionsRef.current = options

  const invoke = useCallback((...args: Args) => {
    const version = ++versionRef.current
    const isCurrent = () => version === versionRef.current

    setValues([])
    setError(null)
    setIsStreaming(true)
    optionsRef.current?.onStart?.()
    ;(async () => {
      try {
        const gen = await fnRef.current(...args)
        for await (const value of gen) {
          if (!isCurrent()) break
          setValues((prev) => [...prev, value])
          optionsRef.current?.onValue?.(value)
        }
      } catch (err) {
        if (isCurrent()) {
          const generatorError = toGeneratorError(err)
          setError(generatorError)
          optionsRef.current?.onError?.(generatorError)
        }
      } finally {
        if (isCurrent()) {
          setIsStreaming(false)
          optionsRef.current?.onEnd?.()
        }
      }
    })()
  }, [])

  const abort = useCallback(() => {
    versionRef.current++
    setIsStreaming(false)
  }, [])

  useEffect(() => {
    return () => {
      versionRef.current++
    }
  }, [])

  return {
    values,
    lastValue: values.length > 0 ? values[values.length - 1]! : undefined,
    isStreaming,
    error,
    abort,
    invoke,
  }
}

function toGeneratorError(originalError: unknown): GeneratorError {
  const message = getErrorMessage(originalError)
  const error: GeneratorError = { message }
  if (originalError && typeof originalError === 'object') {
    Object.assign(error, originalError)
    for (const key of ['name', 'stack', 'cause'] as const) {
      if (key in originalError) {
        Object.assign(error, { [key]: (originalError as Record<string, unknown>)[key] })
      }
    }
  }
  return error
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message
  }
  if (error && typeof error === 'object' && 'message' in error && typeof error.message === 'string') {
    return error.message
  }
  if (typeof error === 'string') {
    return error
  }
  return 'Unknown error'
}
