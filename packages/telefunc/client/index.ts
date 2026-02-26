import { config } from './clientConfig.js'
export { config }
// TO-DO/next-major: remove this redundant export
export { config as telefuncConfig }
export { onAbort, onTelefunctionRemoteCallError } from './remoteTelefunctionCall/onAbort.js'
export { abort } from './abort.js'
export { withContext } from './withContext.js'
export type { TelefunctionError } from './TelefunctionError.js'

export { remoteTelefunctionCall as __remoteTelefunctionCall } from './remoteTelefunctionCall.js'
