export { reportBroadcastDeliveryLost }

import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { config } from '../../../node/server/serverConfig.js'

/** Tells the app of messages its backend lost: delivery is at-most-once, so nothing else hears of them. A throw in its
 *  callback is the app's bug. */
function reportBroadcastDeliveryLost(key: string, reason: string): void {
  const { onDeliveryLost = logDeliveryLost } = config.broadcast
  try {
    onDeliveryLost({ key, reason })
  } catch (err) {
    handleCallbackBug(err)
  }
}

function logDeliveryLost({ key, reason }: { key: string; reason: string }): void {
  console.error(`Broadcast delivery of '${key}' lost: ${reason}`)
}
