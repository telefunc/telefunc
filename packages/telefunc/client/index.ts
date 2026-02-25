import { config } from './clientConfig.js'
export { config }
// TO-DO/next-major: remove this redundant export
export { config as telefuncConfig }
export { onAbort, onTelefunctionRemoteCallError } from './remoteTelefunctionCall/onAbort.js'
export { abort, withAbort } from './abort.js'
export type { TelefunctionError } from './TelefunctionError.js'

export { remoteTelefunctionCall as __remoteTelefunctionCall } from './remoteTelefunctionCall.js'
