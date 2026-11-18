export { reportBroadcastDeliveryLost }

import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { isPromise } from '../../../utils/isPromise.js'
import { config } from '../../../node/server/serverConfig.js'

/** Tells the app of messages its backend lost: delivery is at-most-once, so nothing else hears of them. What its
 *  callback throws, or what a promise it returns rejects with, is the app's bug. */
function reportBroadcastDeliveryLost(key: string, reason: string): void {
  const { onDeliveryLost = logDeliveryLost } = config.broadcast
  let reporting: unknown
  try {
    reporting = onDeliveryLost({ key, reason })
  } catch (err) {
    return handleCallbackBug(err)
  }
  if (isPromise(reporting)) void reporting.then(undefined, handleCallbackBug)
}

function logDeliveryLost({ key, reason }: { key: string; reason: string }): void {
  console.error(`Broadcast delivery of '${key}' lost: ${reason}`)
}
