export { settledOrAborted }

/** Settles on either, discards both outcomes — a caller that needs to know re-reads the signal.
 *  Taking the rejection matters: on an abort-first race nothing else would handle it. */
function settledOrAborted(promise: Promise<unknown>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise<void>((resolve) => {
    const settle = (): void => {
      signal.removeEventListener('abort', settle)
      resolve()
    }
    signal.addEventListener('abort', settle, { once: true })
    promise.then(settle, settle)
  })
}
