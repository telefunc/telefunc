import { describe, expect, it, vi } from 'vitest'

import { WireSender } from './WireSender.js'
import type { ServerTransport } from './mux.js'
import { Deliveries } from '../../flowControl/Deliveries.js'
import { SocketWait } from '../../flowControl/server/SocketWait.js'
import { encode } from '../../wire/frame.js'

/** A wire without pieces whose socket holds `held` bytes, with two deliveries 10 ms apart. */
function openWire(held: number) {
  const transport = { sendNow: vi.fn(), bufferedAmount: () => held, terminateConnection: vi.fn() }
  const deliveries = new Deliveries()
  deliveries.note(0, 100)
  deliveries.note(10, 50)
  const sender = new WireSender({
    wire: {},
    transport: transport as unknown as ServerTransport<unknown>,
    pieces: null,
    wait: new SocketWait({ held: () => held, ahead: () => held }),
    deliveries,
    channels: () => [],
  })
  sender.frameDelivery = 1
  return { sender, transport }
}

describe('WireSender', () => {
  it('drops what its channels send once its wire closed', () => {
    const { sender, transport } = openWire(0)
    sender.send(encode.window(1, 1024, 0))
    expect(transport.sendNow).toHaveBeenCalledTimes(1)
    sender.close()
    sender.send(encode.window(1, 1024, 0))
    sender.carry(encode.msgWindow(1, 8))
    expect(transport.sendNow).toHaveBeenCalledTimes(1)
  })

  it('holds nothing and delivered nothing once its wire closed', () => {
    const { sender } = openWire(100)
    expect(sender.bufferedAmount()).toBe(100)
    expect(sender.deliveredBeforeFrame(20)).toBe(100)
    sender.close()
    expect(sender.bufferedAmount()).toBe(0)
    expect(sender.deliveredBeforeFrame(20)).toBe(0)
  })
})
