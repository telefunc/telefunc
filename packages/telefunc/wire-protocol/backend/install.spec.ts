import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  configureBroadcastTransport,
  disposeBackend,
  getBroadcastBackend,
  getRoomBackend,
  installBackend,
} from './install.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { MemoryBackend } from './memory/backend.js'
import { config } from '../../node/server/serverConfig.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
afterEach(async () => {
  await disposeBackend()
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
    await expect(room.ready).rejects.toThrow("subscribeLane: room 'missing' has no open incarnation 'inc'")
    expect(bind.mock.calls.map(([source]) => source)).toEqual([
      { key: 'same', kind: 'text' },
      { roomId: 'missing', inc: 'inc', lane: { kind: 'semantic' } },
    ])
  })
  it('rejects a second backend without constructing it', () => {
    installBackend(() => new MemoryBackend())
    const factory = vi.fn(() => new MemoryBackend())
    expect(() => installBackend(factory)).toThrow('a different backend is already installed')
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

  it('a Broadcast channel that published before config.broadcast.transport was set uses the transport', async () => {
    const channel = new ServerBroadcast<string>({ key: 'late-transport' })
    await channel.publish('before')
    const transport = localTransport()
    config.broadcast = { transport }
    const seen: string[] = []
    const unsubscribe = channel.subscribe((message) => void seen.push(message))
    transport.send('late-transport', JSON.stringify('from another instance'))
    await vi.waitFor(() => expect(seen).toEqual(['from another instance']))
    unsubscribe()
  })
  it('setting config.broadcast.transport to undefined removes it too', async () => {
    config.broadcast.transport = localTransport()
    config.broadcast.transport = undefined
    expect(getRoomBackend()).toBeDefined()
    await expectBroadcastRoundTrip('unset')
  })

  it('keeps one live listen per key, so a per-key transport keeps delivering across a subscriber swap', async () => {
    const handlers = new Map<string, (payload: string, info: { seq: number; timestamp: number }) => void>()
    const calls: string[] = []
    let seq = 0
    const transport: BroadcastTransport = {
      send: (key, payload) => {
        const info = { seq: ++seq, timestamp: 1 }
        handlers.get(key)?.(payload, info)
        return info
      },
      listen: (key, onMessage) => {
        calls.push('listen')
        handlers.set(key, onMessage)
        return () => {
          calls.push('unlisten')
          handlers.delete(key)
        }
      },
      sendBinary: () => ({ seq: ++seq, timestamp: 1 }),
      listenBinary: () => () => {},
    }
    config.broadcast = { transport }
    const route = { key: 'swap', kind: 'text' } as const
    const seen: string[] = []
    void getBroadcastBackend()
      .subscribe(route, () => {})
      .unsubscribe()
    const next = getBroadcastBackend().subscribe(route, (bytes) => void seen.push(new TextDecoder().decode(bytes)))
    await next.ready
    await new Promise((resolve) => setTimeout(resolve, 0))
    transport.send('swap', 'after the swap')
    expect(seen).toEqual(['after the swap'])
    expect(calls).toEqual(['listen'])
    await next.unsubscribe()
    expect(calls).toEqual(['listen', 'unlisten'])
  })
  it('a publish reaches every instance sharing the transport once, with the transport-assigned receipt', async () => {
    const shared = localTransport()
    const instances = [0, 1].map(() => superviseBroadcastDriver(createBroadcastTransportDriver(shared)))
    const route = { key: 'cross-instance', kind: 'text' } as const
    const seen: string[] = []
    for (const [index, instance] of instances.entries()) {
      await instance.subscribe(route, (bytes) => void seen.push(`${index}:${new TextDecoder().decode(bytes)}`)).ready
    }
    const receipt = await instances[0]!.publish(route, new TextEncoder().encode('hi'), 1024)
    expect(seen.sort()).toEqual(['0:hi', '1:hi'])
    expect(receipt).toEqual({ seq: 1, timestamp: expect.any(Number) })
    await Promise.all(instances.map((instance) => instance.dispose()))
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
  const route = { key: 'override-order', kind: 'text' } as const
  const seen: string[] = []
  const subscription = getBroadcastBackend().subscribe(
    route,
    (bytes) => void seen.push(new TextDecoder().decode(bytes)),
  )
  await subscription.ready
  await getBroadcastBackend().publish(route, new TextEncoder().encode(payload), 1024)
  expect(seen).toEqual([payload])
  await subscription.unsubscribe()
}
