// no-op
export const telefuncConfig = {}
export const config = {}
export const onAbort = () => {}
export const abort = () => {}
export const close = () => {}
export { Abort } from './server/Abort.js'
export { ConnectionError } from '../client/ConnectionError.js'
export const withContext = <F extends (...args: any[]) => any>(fn: F, _ctx?: any): F => fn
export const createClient = () => {
  return new Proxy(
    {},
    {
      get(_target, prop) {
        if (prop === 'then') return undefined
        return () => {}
      },
    },
  )
}
export type { Client, ClientOptions } from '../client/createClient.js'
/** @deprecated */
export const onTelefunctionRemoteCallError = () => {}
