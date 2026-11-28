// Workarounds of crossws's hooks; remove once Telefunc's own WebSocket adapter replaces crossws.
// - crossws calls a hook with no catch: on Node, a hook's throw ends the process.
// - crossws takes a wire's namespace from `new URL(request.url)` unless `getNamespace` gives one, which throws before
//   the upgrade hook runs on the path alone that Bun gives a request whose `Host` it can't read.
export { guardCrosswsHook, getCrosswsNamespace }

import type { Peer } from 'crossws'

// TODO: remove once https://github.com/h3js/crossws/issues/228 is released.
/** `hook`, whose throw is printed as a call's error is, and `ends` the hook's peer where it isn't ending already. */
function guardCrosswsHook<Args extends unknown[]>(
  hook: (peer: Peer, ...args: Args) => unknown,
  ends: ((peer: Peer) => void) | null,
  onError: (err: unknown) => void,
) {
  const failed = (peer: Peer, err: unknown) => {
    onError(err)
    ends?.(peer)
  }
  return (peer: Peer, ...args: Args): void => {
    try {
      const result = hook(peer, ...args)
      if (result instanceof Promise) result.catch((err: unknown) => failed(peer, err))
    } catch (err) {
      failed(peer, err)
    }
  }
}

// TODO: remove once https://github.com/h3js/crossws/issues/229 is released.
/** One namespace for every wire, read off no URL. */
function getCrosswsNamespace(): string {
  return 'telefunc'
}
