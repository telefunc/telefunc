import { afterEach, describe, expect, it, vi } from 'vitest'
import { MemoryBackend } from './memory/backend.js'
import { disposeBackend, installBackend, setDefaultBackend } from './install.js'
afterEach(async () => {
  await disposeBackend().catch(() => {})
  vi.restoreAllMocks()
})
describe('backend installation lifecycle', () => {
  it('promotes a reused default driver to explicit selection', () => {
    const driver = new MemoryBackend()
    const explicit = setDefaultBackend(() => driver)
    expect(installBackend(() => driver)).toBe(explicit)
    expect(setDefaultBackend(() => new MemoryBackend())).toBe(explicit)
  })
  it('reports replacement cleanup failure and includes it in the explicit disposal barrier', async () => {
    const failure = new Error('old backend cleanup failed')
    let rejectOld!: (error: Error) => void
    const oldDisposal = new Promise<void>((_resolve, reject) => {
      rejectOld = reject
    })
    const oldDriver = new MemoryBackend()
    vi.spyOn(oldDriver, 'dispose').mockReturnValue(oldDisposal)
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    setDefaultBackend(() => oldDriver)
    installBackend(() => new MemoryBackend())
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
