// no-op
export const telefuncConfig = {}
export const config = {}
export const onAbort = () => {}
export const abort = () => {}
export const close = () => {}
export { Abort } from './server/Abort.js'
export { ConnectionError } from '../client/ConnectionError.js'
export const withContext = <F extends (...args: any[]) => any>(fn: F, _ctx?: any): F => fn
/** `consumed: true` is the honest answer here, not a stub value: on the server a telefunction is called
 *  directly rather than through a generated stub, so there is no per-call context to pick up and nothing
 *  for a caller to have silently dropped. */
export const withContextChecked = <F extends (...args: any[]) => any>(
  fn: F,
  _ctx: any,
  ...args: Parameters<F>
): { result: ReturnType<F>; consumed: boolean } => ({ result: fn(...args), consumed: true })
/** @deprecated */
export const onTelefunctionRemoteCallError = () => {}
