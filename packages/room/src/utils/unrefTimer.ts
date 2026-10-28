export { unrefTimer }

/** Unref a timer if the runtime supports it (Node.js), then return it unchanged. */
function unrefTimer<T extends ReturnType<typeof setTimeout>>(timer: T): T {
  const unref = (timer as { unref?: unknown }).unref
  if (typeof unref === 'function') unref.call(timer)
  return timer
}
