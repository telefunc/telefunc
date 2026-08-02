import { afterEach, describe, expect, it, vi } from 'vitest'
import { BACKEND_SPI_VERSION, type BackendDriverPair } from './driver-pair.js'
import { disposeBackend, getBroadcastBackend, getRoomBackend, installBackend, setDefaultBackend } from './install.js'
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
})
function memoryPair(driver: MemoryBackend, dispose = () => driver.dispose()): BackendDriverPair {
  return { spiVersion: BACKEND_SPI_VERSION, driver, dispose }
}
