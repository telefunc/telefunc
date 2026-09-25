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
import { superviseRoomDriver } from './room/supervise.js'
import type { RoomDriver } from './room/contract.js'
import { MemoryBackend } from './memory/backend.js'
import { DriverAttempt } from './attempt.js'
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
  it('asks for the install before the first Broadcast or Room use, which started the in-memory backend', () => {
    getBroadcastBackend()
    expect(() => installBackend(() => new MemoryBackend(), ['redis'])).toThrow('before the first Broadcast or Room use')
  })
  it('rejects a second backend without constructing it', () => {
    installBackend(() => new MemoryBackend())
    const factory = vi.fn(() => new MemoryBackend())
    expect(() => installBackend(factory)).toThrow(
      '[Wrong Usage] Install one backend per process: a different backend is already installed',
    )
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

  it('unlistens a key before listening to it again, so a per-key transport keeps delivering across a subscriber swap', async () => {
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
    expect(calls).toEqual(['listen', 'unlisten', 'listen'])
    await next.unsubscribe()
    expect(calls).toEqual(['listen', 'unlisten', 'listen', 'unlisten'])
  })
  it('a publish reaches every instance sharing the transport once, with the transport-assigned receipt', async () => {
    const shared = localTransport()
    const instances = [0, 1].map(() => superviseBroadcastDriver(createBroadcastTransportDriver(shared)))
    const route = { key: 'cross-instance', kind: 'text' } as const
    const seen: string[] = []
    for (const [index, instance] of instances.entries()) {
      await instance.subscribe(route, (bytes) => void seen.push(`${index}:${new TextDecoder().decode(bytes)}`)).ready
    }
    const receipt = await instances[0]!.publish(route, new TextEncoder().encode('hi'))
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

describe('supervised publishes and commits', () => {
  it('holds a publish while a subscription of the other kind on its key establishes', async () => {
    const driver = new MemoryBackend()
    const attempt = new ManualAttempt()
    driver.subscriptions.bind = () => ({ partition: '', open: () => attempt })
    const publish = vi.spyOn(driver, 'publish')
    const backend = superviseBroadcastDriver(driver)
    const subscription = backend.subscribe({ key: 'mixed', kind: 'binary' }, () => {})
    const publishing = backend.publish({ key: 'mixed', kind: 'text' }, new TextEncoder().encode('text'))
    await Promise.resolve()
    expect(publish).not.toHaveBeenCalled()
    attempt.ready()
    await publishing
    expect(publish).toHaveBeenCalledOnce()
    await subscription.unsubscribe()
  })
  it("sends a close's leased commit at once while its lane's subscription establishes, as the lease is shorter than the hold", async () => {
    const attempt = new ManualAttempt()
    const committed: string[] = []
    const driver = {
      subscriptions: { bind: () => ({ partition: '', open: () => attempt }) },
      commitLane: async (_roomId: string, _inc: string, _lane: unknown, payload: Uint8Array) => {
        committed.push(new TextDecoder().decode(payload))
        return { accepted: true, seq: committed.length, timestamp: 1, delivery: Promise.resolve() }
      },
    } as unknown as RoomDriver
    const backend = superviseRoomDriver(driver)
    const control = { kind: 'control' } as const
    const subscription = backend.subscribeLane('room', 'inc', control, () => {})
    const held = backend.commitLane('room', 'inc', control, new TextEncoder().encode('join'))
    const closing = backend.commitLane('room', 'inc', control, new TextEncoder().encode('closed'), {
      closingLease: 'lease',
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(committed).toEqual(['closed'])
    attempt.ready()
    await Promise.all([held, closing])
    expect(committed).toEqual(['closed', 'join'])
    await subscription.unsubscribe()
  })
})

class ManualAttempt extends DriverAttempt {
  async unsubscribe() {
    this.transition('closed')
  }
  ready() {
    this.transition('ready')
  }
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
  const route = { key: 'override-order', kind: 'text' } as const
  const seen: string[] = []
  const subscription = getBroadcastBackend().subscribe(
    route,
    (bytes) => void seen.push(new TextDecoder().decode(bytes)),
  )
  await subscription.ready
  await getBroadcastBackend().publish(route, new TextEncoder().encode(payload))
  expect(seen).toEqual([payload])
  await subscription.unsubscribe()
}
