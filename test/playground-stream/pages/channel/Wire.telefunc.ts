export { onWireResume }

import { Channel } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { cutWireSockets } from '../../wire-sockets'
import { Tally, textMessage, binaryMessage } from './wire-messages'

/**
 * Each end sends `count` numbered messages as text and as binary, awaiting each send; the server drops the page's
 * connections halfway through each direction, once after its own send of count/2 and once on receiving the page's.
 *
 * cleanupState: wireResume_<id> = the server's tally of what the page sent; wireResume_<id>_cut = the connections each drop cut.
 */
async function onWireResume(count: number) {
  const ch = new Channel<(message: string) => void, (message: string) => void>()
  const tally = new Tally()
  const report = () => {
    cleanupState[`wireResume_${ch.id}`] = JSON.stringify(tally.state)
  }
  const cuts: number[] = []
  const cut = () => {
    cuts.push(cutWireSockets())
    cleanupState[`wireResume_${ch.id}_cut`] = JSON.stringify(cuts)
  }
  report()
  ch.listen((message) => {
    if (tally.text(message) === count / 2) cut()
    report()
  })
  ch.listenBinary((message) => {
    tally.binary(message)
    report()
  })
  ch.onOpen(async () => {
    for (let n = 0; n < count; n++) {
      await ch.send(textMessage(n))
      await ch.sendBinary(binaryMessage(n))
      if (n === count / 2) cut()
    }
  })
  return { channel: ch.client, channelId: ch.id }
}
