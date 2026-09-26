import { afterEach, expect, test, vi } from 'vitest'
import type { Peer } from 'crossws'
import { getTelefuncChannelHooks } from './ws.js'
import { getChannelMux } from './mux.js'
import { ServerChannel, reconnectWindow } from './channel.js'
import { encode } from '../shared-ws.js'

afterEach(() => {
  vi.useRealTimers()
})

test("a client that goes silent is detached at its ping deadline, though the peer's terminate() never closes", async () => {
  vi.useFakeTimers()
  const hooks = getTelefuncChannelHooks()
  // A Durable Object peer: terminate() starts a close handshake the vanished client never answers.
  const peer = { context: {}, send() {}, terminate() {} } as unknown as Peer
  const channel = new ServerChannel<string, string>()
  let closed = false
  channel.onClose(() => {
    closed = true
  })
  getChannelMux().registerChannel(channel)
  await hooks.open!(peer)
  const reconcile = encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] })
  await hooks.message!(peer, { uint8Array: () => reconcile } as never)
  await vi.advanceTimersByTimeAsync(reconnectWindow() + 1_000)
  expect(closed).toBe(true)
})
