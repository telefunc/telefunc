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
  it('reports replacement cleanup failure and includes it in the explicit disposal barrier', async () => {
    const failure = new Error('old backend cleanup failed')
    let rejectOld!: (error: Error) => void
    const oldDisposal = new Promise<void>((_resolve, reject) => {
      rejectOld = reject
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    setDefaultBackend(() => memoryPair(new MemoryBackend(), () => oldDisposal))
    installBackend(() => memoryPair(new MemoryBackend()))
    const disposal = disposeBackend()
    let settled = false
    void disposal.then(
      () => (settled = true),
      () => (settled = true),
    )
    await Promise.resolve()
    expect(settled).toBe(false)
    rejectOld(failure)
    await expect(disposal).rejects.toBe(failure)
    await vi.waitFor(() =>
      expect(report).toHaveBeenCalledWith('telefunc/backend: replaced backend disposal failed', failure),
    )
  })
})
function memoryPair(driver: MemoryBackend, dispose = () => driver.dispose()): BackendDriverPair {
  return { spiVersion: BACKEND_SPI_VERSION, driver, dispose }
}
