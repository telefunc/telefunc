export { markHandled }

/** A fire-and-forget call that fails leaves no unhandled rejection; a caller that awaits it still sees the error. */
function markHandled<T>(result: Promise<T>): Promise<T> {
  result.catch(() => {})
  return result
}
