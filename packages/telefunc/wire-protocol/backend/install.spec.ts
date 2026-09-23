import { afterEach, describe, expect, it, vi } from 'vitest'
import type { BackendDriverPair } from './driver-pair.js'
import {
  configureBroadcastTransport,
  disposeBackend,
  getBroadcastBackend,
  getRoomBackend,
  installBackend,
} from './install.js'
import type { BroadcastTransport } from './broadcast/transport.js'
import { MemoryBackend } from './memory/backend.js'
import { SubscriptionManager } from './subscription-manager.js'
afterEach(async () => {
  await disposeBackend().catch(() => {})
  vi.restoreAllMocks()
})
describe('backend installation lifecycle', () => {
  it('reuses the installed backend when the same entry installs it again', () => {
    const factory = () => memoryPair(new MemoryBackend())
    installBackend(factory, 'entry')
    const installed = getRoomBackend()
    installBackend(() => {
      throw new Error('a repeated entry must not construct another backend')
    }, 'entry')
    expect(getRoomBackend()).toBe(installed)
  })
  it("supervises one driver's Broadcast and Room subscriptions independently", async () => {
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
  it('rejects an incomplete driver and stays uninstalled', () => {
    const incomplete = Object.assign(new MemoryBackend(), { readHead: undefined })
    expect(() =>
      installBackend(() => ({ ...memoryPair(new MemoryBackend()), driver: incomplete }) as BackendDriverPair),
    ).toThrow('missing required method "readHead"')
    const factory = () => memoryPair(new MemoryBackend())
    installBackend(factory)
    expect(getRoomBackend()).toBeDefined()
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
  it('rejects a second backend without constructing it', () => {
    installBackend(() => memoryPair(new MemoryBackend()))
    const factory = vi.fn(() => memoryPair(new MemoryBackend()))
    expect(() => installBackend(factory)).toThrow('a backend is already active')
    expect(factory).not.toHaveBeenCalled()
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

  it.each([
    { seq: 0, timestamp: 1 },
    { seq: 1.5, timestamp: 1 },
    { seq: 1, timestamp: -1 },
    { seq: 1, timestamp: Number.NaN },
  ])('rejects transport ordering marks %o as a usage error on both kinds', async (mark) => {
    const delivered: Array<(info: { seq: number; timestamp: number }) => void> = []
    configureBroadcastTransport({
      send: async () => mark,
      sendBinary: () => mark,
      listen: (_key, onMessage) => {
        delivered.push((info) => onMessage('"x"', info))
        return () => {}
      },
      listenBinary: (_key, onMessage) => {
        delivered.push((info) => onMessage(new Uint8Array([1]), info))
        return () => {}
      },
    })
    const backend = getBroadcastBackend()
    const usage = /\[Wrong Usage\] config\.broadcast\.transport/
    await expect(backend.publish({ key: 'k', kind: 'text' }, new Uint8Array())).rejects.toThrow(usage)
    expect(() => backend.publish({ key: 'k', kind: 'binary' }, new Uint8Array())).toThrow(usage)

    const received = vi.fn()
    backend.subscribe({ key: 'k', kind: 'text' }, received)
    backend.subscribe({ key: 'k', kind: 'binary' }, received)
    expect(delivered).toHaveLength(2)
    for (const deliver of delivered) expect(() => deliver(mark)).toThrow(usage)
    expect(received).not.toHaveBeenCalled()
  })
})
function memoryPair(driver: MemoryBackend, dispose = () => driver.dispose()): BackendDriverPair {
  return { driver, dispose }
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
