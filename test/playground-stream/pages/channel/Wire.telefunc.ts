export { onWireResume, onWireOverrun }

import { Channel } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { cutWireSockets } from '../../wire-sockets'
import { onServerAction } from '../../server-actions'
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

/**
 * `count` numbered messages the page sends, or the server sends at server action wireOverrun_<id>, without awaiting them;
 * the server drops the page's connections right after its own.
 *
 * cleanupState: wireOverrun_<id> = the server's tally of what the page sent; wireOverrun_<id>_onClose = its onClose error's name.
 */
async function onWireOverrun(count: number) {
  const ch = new Channel<(message: string) => void, (message: string) => void>()
  const tally = new Tally()
  const report = () => {
    cleanupState[`wireOverrun_${ch.id}`] = JSON.stringify(tally.state)
  }
  report()
  ch.onClose((err) => {
    cleanupState[`wireOverrun_${ch.id}_onClose`] = err ? err.name : 'none'
  })
  ch.listen((message) => {
    tally.text(message)
    report()
  })
  onServerAction(`wireOverrun_${ch.id}`, () => {
    for (let n = 0; n < count; n++) void ch.send(textMessage(n))
    cutWireSockets()
  })
  return { channel: ch.client, channelId: ch.id }
}
