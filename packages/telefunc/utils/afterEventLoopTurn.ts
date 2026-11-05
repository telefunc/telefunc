export { afterEventLoopTurn }

// setImmediate runs after pending I/O, where it exists.
function afterEventLoopTurn(fn: () => void): void {
  if (typeof setImmediate === 'function') setImmediate(fn)
  else setTimeout(fn, 0)
}
