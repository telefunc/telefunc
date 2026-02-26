// no-op
export const telefuncConfig = {}
export const config = {}
export const onAbort = () => {}
export const abort = () => {}
export const withContext = <F extends (...args: any[]) => any>(fn: F, _ctx?: any): F => fn
/** @deprecated */
export const onTelefunctionRemoteCallError = () => {}
