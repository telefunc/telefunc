export { onWireResume, onWireOverrun, onWireEnd, onWireLarge, onWireOpen, onWireRelease, onWireUpload }

import { Channel, Abort } from 'telefunc'
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

/**
 * A channel the server closes or aborts at server action wireEnd_<id>_close or wireEnd_<id>_abort, after sending
 * `before` messages, or the page ends.
 *
 * cleanupState: wireEnd_<id>_onOpen = 'true' once it opens; wireEnd_<id>_onClose = 'none', 'abort:<value as JSON>' or the error's name; wireEnd_<id>_close = what the server's close() resolved to.
 */
async function onWireEnd(before: number) {
  const ch = new Channel<never, (message: string) => void>()
  const sendBefore = () => {
    for (let n = 0; n < before; n++) void ch.send(`m${n}`)
  }
  ch.onOpen(() => {
    cleanupState[`wireEnd_${ch.id}_onOpen`] = 'true'
  })
  ch.onClose((err) => {
    cleanupState[`wireEnd_${ch.id}_onClose`] = describeEnd(err)
  })
  onServerAction(`wireEnd_${ch.id}_close`, async () => {
    sendBefore()
    cleanupState[`wireEnd_${ch.id}_close`] = String(await ch.close())
  })
  onServerAction(`wireEnd_${ch.id}_abort`, () => {
    sendBefore()
    ch.abort({ by: 'server' })
  })
  return { channel: ch.client, channelId: ch.id }
}

function describeEnd(err: Error | undefined): string {
  if (!err) return 'none'
  if (err instanceof Abort) return `abort:${JSON.stringify(err.abortValue)}`
  return err.name
}

/** A channel the server sends one text message of `bytes` on as it opens. */
async function onWireLarge(bytes: number) {
  const ch = new Channel<never, (message: string) => void>()
  ch.onOpen(() => {
    void ch.send('x'.repeat(bytes))
  })
  return { channel: ch.client }
}

async function onWireOpen() {
  return new Channel<never, never>().client
}

/**
 * A channel the server sends `count` messages on as it opens, and holds by a WeakRef only.
 *
 * Server action wireRelease_<id> sets cleanupState wireRelease_<id> to 'released' once the server let it go, else 'held'.
 */
async function onWireRelease(count: number) {
  const ch = new Channel<never, (message: string) => void>()
  ch.onOpen(() => {
    for (let n = 0; n < count; n++) void ch.send(`m${n}`)
  })
  reportRelease(ch.id, new WeakRef(ch))
  return { channel: ch.client, channelId: ch.id }
}

// Apart from onWireRelease: a closure made there would share the scope that holds the channel.
function reportRelease(id: string, channel: WeakRef<object>) {
  onServerAction(`wireRelease_${id}`, () => {
    cleanupState[`wireRelease_${id}`] = channel.deref() ? 'held' : 'released'
  })
}

/** A channel the page sends numbered text on. cleanupState: wireUpload_<id> = the server's tally. */
async function onWireUpload() {
  const ch = new Channel<(message: string) => void, never>()
  const tally = new Tally()
  ch.listen((message) => {
    tally.text(message)
    cleanupState[`wireUpload_${ch.id}`] = JSON.stringify(tally.state)
  })
  return { channel: ch.client, channelId: ch.id }
}
