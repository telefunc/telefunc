export {
  onWireResume,
  onWireOverrun,
  onWireEnd,
  onWireLarge,
  onWireOpen,
  onWireHeld,
  onWireRelease,
  onWireUpload,
  onWireConnection,
  onWirePresence,
}

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
 * cleanupState: wireOverrun_<id> = the server's tally of what the page sent; wireOverrun_<id>_onOpen = 'true' once it opens; wireOverrun_<id>_onClose = its onClose error's name.
 */
async function onWireOverrun(count: number) {
  const ch = new Channel<(message: string) => void, (message: string) => void>()
  const tally = new Tally()
  const report = () => {
    cleanupState[`wireOverrun_${ch.id}`] = JSON.stringify(tally.state)
  }
  report()
  ch.onOpen(() => {
    cleanupState[`wireOverrun_${ch.id}_onOpen`] = 'true'
  })
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

/** A channel the server sends `first <n>` on as it opens. cleanupState: wireHeld_<run> = how many closed without an error,
 *  wireHeldKept_<run> = how many messages the run's channels got. */
async function onWireHeld(run: string, n: number) {
  const ch = new Channel<(message: string) => void, (message: string) => void>()
  ch.listen(() => {
    cleanupState[`wireHeldKept_${run}`] = String(Number(cleanupState[`wireHeldKept_${run}`] ?? 0) + 1)
  })
  ch.onOpen(() => {
    void ch.send(`first ${n}`)
  })
  ch.onClose((err) => {
    if (!err) cleanupState[`wireHeld_${run}`] = String(Number(cleanupState[`wireHeld_${run}`] ?? 0) + 1)
  })
  return ch.client
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

/** cleanupState: wireConnection_<id> = what the server end saw: 'start' (before open), 'open', 'disconnect', 'reconnect', 'close',
 *  each with isConnected. */
async function onWireConnection() {
  const ch = new Channel<never, never>()
  const events: string[] = []
  const report = (event: string) => {
    events.push(`${event}:${ch.isConnected}`)
    cleanupState[`wireConnection_${ch.id}`] = JSON.stringify(events)
  }
  report('start')
  ch.onOpen(() => report('open'))
  ch.onDisconnect(() => report('disconnect'))
  ch.onReconnect(() => report('reconnect'))
  ch.onClose(() => report('close'))
  return { channel: ch.client, channelId: ch.id }
}

const sessions = new Set<string>()

/** A member's channel that saves whether they're online, and ends once they signed out; cleanupState:
 *  wirePresence_<id> = the status the database holds. */
async function onWirePresence() {
  const ch = new Channel<never, never>()
  const database = connectToDatabase()
  const session = crypto.randomUUID()
  sessions.add(session)
  // The member signs out on another device.
  onServerAction(`wirePresence_${ch.id}_signOut`, () => sessions.delete(session))
  ch.onDisconnect(async () => {
    await database.saveStatus(ch.id, 'away')
  })
  ch.onReconnect(async () => {
    if (!(await isSignedIn(session))) throw Abort({ reason: 'signed out' })
    await database.saveStatus(ch.id, 'online')
  })
  return { channel: ch.client, channelId: ch.id }
}

async function isSignedIn(session: string) {
  return sessions.has(session)
}

/** A database that is unavailable for the first write. */
function connectToDatabase() {
  let connected = false
  return {
    async saveStatus(memberId: string, status: string) {
      if (!connected) {
        connected = true
        throw new Error('Database unavailable')
      }
      cleanupState[`wirePresence_${memberId}`] = status
    },
  }
}
