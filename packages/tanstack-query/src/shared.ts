export { __TQ__DATA_KEY, __TQ__CHANNEL_KEY, isWrapper }
export type { TelefuncLiveWrapper }

import type { ClientChannel } from 'telefunc'

const __TQ__DATA_KEY = '__tq__data'
const __TQ__CHANNEL_KEY = '__tq__channel'

/** The result a live telefunction returns: the real data plus the server-owned invalidation
 *  channel. The client persister unwraps `__tq__data` and wires `__tq__channel`. */
type TelefuncLiveWrapper = {
  [__TQ__DATA_KEY]: unknown
  [__TQ__CHANNEL_KEY]: ClientChannel<never, 'invalidate'>
}

/** True only when `v` carries BOTH wrapper keys as its OWN properties AND `__tq__channel` is an
 *  actual channel (callable `listen` + `close`). Inherited / prototype-forged keys never qualify,
 *  and an ordinary business object that happens to own the two reserved keys with a non-channel
 *  value is returned unchanged rather than misread as live (which would throw on `.listen`). */
function isWrapper(v: unknown): v is TelefuncLiveWrapper {
  if (typeof v !== 'object' || v === null) return false
  if (!Object.hasOwn(v, __TQ__DATA_KEY) || !Object.hasOwn(v, __TQ__CHANNEL_KEY)) return false
  const channel = (v as Record<string, unknown>)[__TQ__CHANNEL_KEY]
  return (
    typeof channel === 'object' &&
    channel !== null &&
    typeof (channel as { listen?: unknown }).listen === 'function' &&
    typeof (channel as { close?: unknown }).close === 'function'
  )
}
