import { afterEach, describe, expect, it, vi } from 'vitest'
import { BACKEND_SPI_VERSION, type BackendDriverPair } from './driver-pair.js'
import {
  configureBroadcastTransport,
  disposeBackend,
  getBroadcastBackend,
  getRoomBackend,
  installBackend,
  setDefaultBackend,
} from './install.js'
import type { BroadcastTransport } from './broadcast/transport.js'
import { MemoryBackend } from './memory/backend.js'
import { SubscriptionManager } from './subscription-manager.js'
afterEach(async () => {
  await disposeBackend().catch(() => {})
  vi.restoreAllMocks()
})
describe('backend installation lifecycle', () => {
  it('promotes a reused default pair to explicit selection', () => {
    const selected = memoryPair(new MemoryBackend())
    setDefaultBackend(() => selected)
    const explicit = getRoomBackend()
    installBackend(() => selected)
    setDefaultBackend(() => {
      throw new Error('default constructed after explicit selection')
    })
    expect(getRoomBackend()).toBe(explicit)
  })
  it('accepts equal halves while supervising their subscriptions independently', async () => {
    const driver = new MemoryBackend()
    const bind = vi.spyOn(driver.subscriptions, 'bind')
    installBackend(() => memoryPair(driver))
    const broadcast = getBroadcastBackend().subscribe({ key: 'same', kind: 'text' }, () => {})
    const room = getRoomBackend().subscribeLane('missing', 'inc', { kind: 'semantic' }, () => {})
    await expect(broadcast.ready).resolves.toBeUndefined()
    await expect(room.ready).rejects.toThrow('Backend subscription closed: missing:inc:semantic')
    expect(bind.mock.calls.map(([source]) => source)).toEqual([
      { key: 'same', kind: 'text' },
      { roomId: 'missing', inc: 'inc', lane: { kind: 'semantic' } },
    ])
  })
  it('rejects an incomplete half and restores the selected pair after construction failure', () => {
    const selected = memoryPair(new MemoryBackend())
    setDefaultBackend(() => selected)
    const current = getRoomBackend()
    const incomplete = Object.assign(new MemoryBackend(), { readHead: undefined })
    expect(() =>
      installBackend(() => ({ ...memoryPair(new MemoryBackend()), driver: incomplete }) as BackendDriverPair),
    ).toThrow('missing required method "readHead"')
    expect(getRoomBackend()).toBe(current)
  })
  it('disposes both managers before invoking the pair disposer exactly once', async () => {
    const gate = Promise.withResolvers<void>()
    const stops = vi.spyOn(SubscriptionManager.prototype, 'dispose').mockReturnValue(gate.promise)
    const dispose = vi.fn(async () => {})
    installBackend(() => memoryPair(new MemoryBackend(), dispose))
    const first = disposeBackend()
    expect(disposeBackend()).toBe(first)
    await Promise.resolve()
    expect([stops.mock.calls.length, new Set(stops.mock.instances).size, dispose.mock.calls.length]).toEqual([2, 2, 0])
    gate.resolve()
    await first
    expect(dispose).toHaveBeenCalledOnce()
  })
  it('rejects a different backend after acquisition and disposes the rejected candidate', async () => {
    const active = memoryPair(new MemoryBackend())
    const rejectedDispose = vi.fn(async () => {})
    setDefaultBackend(() => active)
    expect(() => installBackend(() => memoryPair(new MemoryBackend(), rejectedDispose))).toThrow(
      'a backend is already active',
    )
    await vi.waitFor(() => expect(rejectedDispose).toHaveBeenCalledOnce())
    expect(getRoomBackend()).toBeDefined()
  })
  it('composes a broadcast override with a full backend in either configuration order', async () => {
    const transport = localTransport()
    installBackend(() => memoryPair(new MemoryBackend()))
    const roomInstalledFirst = getRoomBackend()
    configureBroadcastTransport(transport)
    await expectBroadcastRoundTrip('installed-first')
    expect(getRoomBackend()).toBe(roomInstalledFirst)

    await disposeBackend()
    installBackend(() => memoryPair(new MemoryBackend()))
    const roomConfiguredFirst = getRoomBackend()
    await expectBroadcastRoundTrip('configured-first')
    expect(getRoomBackend()).toBe(roomConfiguredFirst)

    await disposeBackend()
    await expectBroadcastRoundTrip('transport-only')
    expect(() => getRoomBackend()).toThrow('Room requires a full backend')
  })
})
function memoryPair(driver: MemoryBackend, dispose = () => driver.dispose()): BackendDriverPair {
  return { spiVersion: BACKEND_SPI_VERSION, driver, dispose }
}

function localTransport(): BroadcastTransport {
  let seq = 0
  const text = new Map<string, Set<(payload: string, info: { seq: number; timestamp: number }) => void>>()
  const binary = new Map<string, Set<(payload: Uint8Array, info: { seq: number; timestamp: number }) => void>>()
  const listen = <Payload>(
    routes: Map<string, Set<(payload: Payload, info: { seq: number; timestamp: number }) => void>>,
    key: string,
    receiver: (payload: Payload, info: { seq: number; timestamp: number }) => void,
  ) => {
    const receivers = routes.get(key) ?? new Set()
    receivers.add(receiver)
    routes.set(key, receivers)
    return () => receivers.delete(receiver)
  }
  const send = <Payload>(
    routes: Map<string, Set<(payload: Payload, info: { seq: number; timestamp: number }) => void>>,
    key: string,
    payload: Payload,
  ) => {
    const info = { seq: ++seq, timestamp: Date.now() }
    for (const receiver of routes.get(key) ?? []) receiver(payload, info)
    return info
  }
  return {
    send: (key, payload) => send(text, key, payload),
    listen: (key, receiver) => listen(text, key, receiver),
    sendBinary: (key, payload) => send(binary, key, payload),
    listenBinary: (key, receiver) => listen(binary, key, receiver),
  }
}

async function expectBroadcastRoundTrip(payload: string): Promise<void> {
  const lane = { key: 'override-order', kind: 'text' } as const
  const seen: string[] = []
  const subscription = getBroadcastBackend().subscribe(lane, (bytes) => seen.push(new TextDecoder().decode(bytes)))
  await subscription.ready
  await getBroadcastBackend().publish(lane, new TextEncoder().encode(payload))
  expect(seen).toEqual([payload])
  await subscription.unsubscribe()
}
