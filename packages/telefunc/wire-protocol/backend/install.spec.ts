import { afterEach, describe, expect, it, vi } from 'vitest'
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
import { config } from '../../node/server/serverConfig.js'
afterEach(async () => {
  await disposeBackend().catch(() => {})
  config.broadcast = {}
  vi.restoreAllMocks()
})
describe('backend installation lifecycle', () => {
  it('reuses the installed backend when the same entry installs it again', () => {
    const driver = installBackend(() => new MemoryBackend(), ['entry', 1])
    const installed = getRoomBackend()
    const again = installBackend(() => {
      throw new Error('a repeated entry must not construct another backend')
    }, ['entry', 1])
    expect(again).toBe(driver)
    expect(getRoomBackend()).toBe(installed)
  })
  it("supervises one driver's Broadcast and Room subscriptions independently", async () => {
    const driver = new MemoryBackend()
    const bind = vi.spyOn(driver.subscriptions, 'bind')
    installBackend(() => driver)
    const broadcast = getBroadcastBackend().subscribe({ key: 'same', kind: 'text' }, () => {})
    const room = getRoomBackend().subscribeLane('missing', 'inc', { kind: 'semantic' }, () => {})
    await expect(broadcast.ready).resolves.toBeUndefined()
    await expect(room.ready).rejects.toThrow('Backend subscription closed: missing:inc:semantic')
    expect(bind.mock.calls.map(([source]) => source)).toEqual([
      { key: 'same', kind: 'text' },
      { roomId: 'missing', inc: 'inc', lane: { kind: 'semantic' } },
    ])
  })
  it('disposes both managers before disposing the driver exactly once', async () => {
    const gate = Promise.withResolvers<void>()
    const stops = vi.spyOn(SubscriptionManager.prototype, 'dispose').mockReturnValue(gate.promise)
    const driver = new MemoryBackend()
    const dispose = vi.spyOn(driver, 'dispose')
    installBackend(() => driver)
    const first = disposeBackend()
    expect(disposeBackend()).toBe(first)
    await Promise.resolve()
    expect([stops.mock.calls.length, new Set(stops.mock.instances).size, dispose.mock.calls.length]).toEqual([2, 2, 0])
    gate.resolve()
    await first
    expect(dispose).toHaveBeenCalledOnce()
  })
  it('rejects a second backend without constructing it', () => {
    installBackend(() => new MemoryBackend())
    const factory = vi.fn(() => new MemoryBackend())
    expect(() => installBackend(factory)).toThrow('a backend is already active')
    expect(factory).not.toHaveBeenCalled()
  })
  it('composes a broadcast override with a full backend in either configuration order', async () => {
    const transport = localTransport()
    installBackend(() => new MemoryBackend())
    const roomInstalledFirst = getRoomBackend()
    configureBroadcastTransport(transport)
    await expectBroadcastRoundTrip('installed-first')
    expect(getRoomBackend()).toBe(roomInstalledFirst)

    await disposeBackend()
    installBackend(() => new MemoryBackend())
    const roomConfiguredFirst = getRoomBackend()
    await expectBroadcastRoundTrip('configured-first')
    expect(getRoomBackend()).toBe(roomConfiguredFirst)

    await disposeBackend()
    await expectBroadcastRoundTrip('transport-only')
    expect(() => getRoomBackend()).toThrow('Room requires a full backend')
  })

  it('removing config.broadcast.transport hands Broadcast back to the backend', async () => {
    config.broadcast = { transport: localTransport() }
    expect(() => getRoomBackend()).toThrow('Room requires a full backend')
    config.broadcast = {}
    expect(getRoomBackend()).toBeDefined()
    await expectBroadcastRoundTrip('removed')
  })

  it('refuses to change config.broadcast.transport while Broadcast subscriptions are open', async () => {
    const transport = localTransport()
    config.broadcast = { transport }
    const subscription = getBroadcastBackend().subscribe({ key: 'live', kind: 'text' }, () => {})
    expect(() => (config.broadcast = {})).toThrow('set it once, before the first subscription')
    expect(config.broadcast.transport).toBe(transport)
    await subscription.unsubscribe()
    config.broadcast = {}
    expect(getRoomBackend()).toBeDefined()
  })

  it('setting config.broadcast.transport to undefined removes it too', async () => {
    config.broadcast.transport = localTransport()
    config.broadcast.transport = undefined
    expect(getRoomBackend()).toBeDefined()
    await expectBroadcastRoundTrip('unset')
  })

  it('rejects a Broadcast transport missing a binary method when it is configured', () => {
    const { sendBinary: _, ...textOnly } = localTransport()
    expect(() => {
      config.broadcast = { transport: textOnly as BroadcastTransport }
    }).toThrow('config.broadcast.transport must be a BroadcastTransport with send(), listen(), sendBinary()')
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
    const usage = 'config.broadcast.transport returned'
    await expect(backend.publish({ key: 'k', kind: 'text' }, new Uint8Array(), 1024)).rejects.toThrow(usage)
    expect(() => backend.publish({ key: 'k', kind: 'binary' }, new Uint8Array(), 1024)).toThrow(usage)

    const received = vi.fn()
    backend.subscribe({ key: 'k', kind: 'text' }, received)
    backend.subscribe({ key: 'k', kind: 'binary' }, received)
    expect(delivered).toHaveLength(2)
    for (const deliver of delivered) expect(() => deliver(mark)).toThrow(usage)
    expect(received).not.toHaveBeenCalled()
  })
})

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
  const subscription = getBroadcastBackend().subscribe(lane, (bytes) => void seen.push(new TextDecoder().decode(bytes)))
  await subscription.ready
  await getBroadcastBackend().publish(lane, new TextEncoder().encode(payload), 1024)
  expect(seen).toEqual([payload])
  await subscription.unsubscribe()
}
