import { describe, expect, test } from 'vitest'
import { GcRegistry } from './gcRegistry.js'

const hasGc = typeof (globalThis as { gc?: () => void }).gc === 'function'

async function forceGc(): Promise<void> {
  // A few cycles so collection + the WeakRef scan / FinalizationRegistry callbacks all run.
  for (let i = 0; i < 8; i++) {
    ;(globalThis as { gc?: () => void }).gc!()
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
}

// Registers and drops the target inside a sync frame so it can't be retained by the caller's
// (async) frame — the whole point is that dropping it triggers close.
function registerAndDrop(gc: GcRegistry, onClose: () => void): void {
  gc.register({ marker: true }, onClose)
}

// GC-driven cleanup is what holder-side stub tracking relies on (client response + server request
// revival both register a wrapped stub, then close its channel once the consumer drops it). Needs
// real GC, so these run only under `node --expose-gc` and skip otherwise.
describe.skipIf(!hasGc)('GcRegistry (real GC)', () => {
  test('fires close once the registered target is collected', async () => {
    const gc = new GcRegistry(10)
    let closed = 0
    registerAndDrop(gc, () => {
      closed++
    })

    await forceGc()

    expect(closed).toBe(1)
  })

  test('keeps close pending while the target is still referenced', async () => {
    const gc = new GcRegistry(10)
    let closed = 0
    const target = { marker: true }
    gc.register(target, () => {
      closed++
    })

    await forceGc()

    expect(target.marker).toBe(true)
    expect(closed).toBe(0)
  })
})
