export { keepReachable }

const reachable = new Set<object>()

/** Keeps `handle` from the GC pass that would close it, until the returned release. The build drops a bare `void handle`. */
function keepReachable(handle: object): () => void {
  reachable.add(handle)
  return () => reachable.delete(handle)
}
