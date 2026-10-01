export { makeDisposer }

import { untether } from '../wrapProxy.js'

/** A one-shot cleanup handle; no action creates an already-terminal handle. */
function makeDisposer(dispose?: () => void, group?: Set<() => void>): () => void {
  let action = dispose
  const token = () => {
    const current = action
    action = undefined
    group?.delete(token)
    untether(token)
    current?.()
  }
  if (action) group?.add(token)
  else untether(token)
  return token
}
