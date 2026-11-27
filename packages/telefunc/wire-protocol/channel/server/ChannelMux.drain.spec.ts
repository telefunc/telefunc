// What a WebSocket's socket holds beyond its channels is held in `config.channel.maxMemory` for its wire, and goes from
// it once the socket drains, before the page sends anything more.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { upgradesTakenHere } from '../../../node/server/runTelefunc/takesUpgrades.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { getChannelMux, type ServerTransport } from './ChannelMux.js'

config.channel = { maxMemory: '16m' }
/** What the ledger holds, for every holder together. */
const ledger = getGlobalObject<{ total: number }>('channelMemory.ts', { total: 0 })

test("a WebSocket's socket that drains lets go of what the ledger held for its wire", async () => {
  const mux = getChannelMux()
  const wire = {}
  const socket = { buffered: 2 ** 20 }
  const transport: ServerTransport<typeof wire> = {
    getConnId: () => null,
    sendNow: () => {},
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => socket.buffered,
    close: () => {},
  }
  mux.onWireOpen(wire, transport, upgradesTakenHere)
  try {
    // Its PONG finds the socket holding 1 MiB, which no channel holds.
    await mux.onWireMessage(wire, encode.ping([]))
    expect(ledger.total).toBeGreaterThan(0)
    socket.buffered = 0
    mux.onWireDrain(wire)
    expect(ledger.total).toBe(0)
  } finally {
    mux.onWireClosed(wire, 'pageLeft')
  }
})
