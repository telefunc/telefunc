import { afterEach, describe, expect, test, vi } from 'vitest'
import { disposeBackend, getBackend, installBackend, setDefaultBackend } from './install.js'
import { MemoryBackend } from './memory/backend.js'
import type { BackendDriver } from './spi.js'

afterEach(async () => {
  await disposeBackend()
})

describe('Room backend installation', () => {
  test('lazily installs one implicit memory backend', () => {
    const backend = getBackend()
    expect(backend).not.toBeInstanceOf(MemoryBackend)
    expect(getBackend()).toBe(backend)
  })

  test('replaces implicit memory with a default and disposes the fallback once', () => {
    const implicit = getBackend()
    const dispose = vi.spyOn(implicit, 'dispose')
    const driver = memoryBackend()

    const backend = setDefaultBackend(() => driver)
    expect(getBackend()).toBe(backend)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  test('lets an explicit install replace and dispose a default', async () => {
    const driver = memoryBackend()
    const dispose = vi.spyOn(driver, 'dispose')
    const explicitDriver = memoryBackend()
    setDefaultBackend(() => driver)

    const explicit = installBackend(() => explicitDriver)
    expect(getBackend()).toBe(explicit)
    await settle()
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  test('never lets a later default overwrite an explicit install', () => {
    const explicitDriver = memoryBackend()
    const factory = vi.fn(() => memoryBackend())
    const explicit = installBackend(() => explicitDriver)

    expect(setDefaultBackend(factory)).toBe(explicit)
    expect(getBackend()).toBe(explicit)
    expect(factory).not.toHaveBeenCalled()
  })

  test('keeps an equivalent default connection-idempotent', () => {
    const identity = {}
    const driver = memoryBackend()
    const dispose = vi.spyOn(driver, 'dispose')
    const replacement = vi.fn(() => memoryBackend())

    const backend = setDefaultBackend(() => driver, identity)
    expect(setDefaultBackend(replacement, identity)).toBe(backend)
    expect(replacement).not.toHaveBeenCalled()
    expect(dispose).not.toHaveBeenCalled()
  })

  test('uses last-default-wins for different identities and disposes each retired default once', async () => {
    const firstDriver = memoryBackend()
    const firstDispose = vi.spyOn(firstDriver, 'dispose')
    const secondDriver = memoryBackend()
    const secondDispose = vi.spyOn(secondDriver, 'dispose')

    const first = setDefaultBackend(() => firstDriver, {})
    const second = setDefaultBackend(() => secondDriver, {})
    expect(first).not.toBe(second)
    expect(getBackend()).toBe(second)
    await settle()
    expect(firstDispose).toHaveBeenCalledTimes(1)
    expect(secondDispose).not.toHaveBeenCalled()

    await Promise.all([disposeBackend(), disposeBackend()])
    expect(firstDispose).toHaveBeenCalledTimes(1)
    expect(secondDispose).toHaveBeenCalledTimes(1)
  })

  test('does not self-dispose when a replacement factory returns the installed raw driver', async () => {
    const driver = memoryBackend()
    const dispose = vi.spyOn(driver, 'dispose')
    const backend = setDefaultBackend(() => driver, {})

    const replacement = setDefaultBackend(() => driver, {})
    await settle()
    await expect(backend.readHead('same-driver')).resolves.toBe(null)
    expect(replacement).toBe(backend)
    expect(dispose).not.toHaveBeenCalled()
  })

  test('does not self-dispose when an explicit factory returns the installed default instance', async () => {
    const driver = memoryBackend()
    const dispose = vi.spyOn(driver, 'dispose')
    const backend = setDefaultBackend(() => driver)

    expect(installBackend(() => driver)).toBe(backend)
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-default')).resolves.toBe(null)
  })

  test('keeps an explicit instance live when installation is repeated', async () => {
    const driver = memoryBackend()
    const dispose = vi.spyOn(driver, 'dispose')
    const repeatedFactory = vi.fn(() => driver)
    const backend = installBackend(() => driver)

    expect(installBackend(repeatedFactory)).toBe(backend)
    expect(repeatedFactory).not.toHaveBeenCalled()
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-explicit')).resolves.toBe(null)
  })

  test('makes installing explicit before a factory can reenter', () => {
    const nestedFactory = vi.fn(() => memoryBackend())
    const driver = memoryBackend()

    const backend = installBackend(() => {
      expect(() => getBackend()).toThrow('still installing')
      expect(() => installBackend(nestedFactory)).toThrow('still installing')
      return driver
    })
    expect(nestedFactory).not.toHaveBeenCalled()
    expect(getBackend()).toBe(backend)
  })

  test('clears a throwing factory before a retry', () => {
    const failure = new Error('factory failed')
    expect(() =>
      installBackend(() => {
        throw failure
      }),
    ).toThrow(failure)
    expect(installBackend(() => memoryBackend())).not.toBeInstanceOf(MemoryBackend)
  })

  test('rejects a mismatched SPI version before committing the installation', () => {
    const backend = memoryBackend()
    ;(backend as unknown as { spiVersion: number }).spiVersion = 2

    expect(() => installBackend(() => backend)).toThrow('spiVersion 2; expected 1')
    expect(getBackend()).not.toBeInstanceOf(MemoryBackend)
  })

  test('rejects a backend with a missing core method', () => {
    const backend = memoryBackend()
    ;(backend as unknown as Record<string, unknown>).commitLane = undefined

    expect(() => installBackend(() => backend)).toThrow('missing required method "commitLane"')
  })

  test('rejects a driver without the raw subscription edge', () => {
    const backend = memoryBackend()
    ;(backend.subscriptions as unknown as Record<string, unknown>).open = undefined

    expect(() => installBackend(() => backend)).toThrow('missing required method "open"')
  })

  test('rejects unusable receiver and directory capability declarations', () => {
    const noReceivers = memoryBackend()
    ;(noReceivers.capabilities as { receivers: string }).receivers = 'none'
    expect(() => installBackend(() => noReceivers)).toThrow('capabilities.receivers')

    const noDirectoryMethod = memoryBackend()
    ;(noDirectoryMethod as unknown as Record<string, unknown>).directoryList = undefined
    expect(() => installBackend(() => noDirectoryMethod)).toThrow('missing required method "directoryList"')
  })

  test('latches the first validated factory and disposes it once before a fresh installation', async () => {
    const firstDriver = memoryBackend()
    const dispose = vi.spyOn(firstDriver, 'dispose')
    const secondFactory = vi.fn(() => memoryBackend())

    const first = installBackend(() => firstDriver)
    expect(installBackend(secondFactory)).toBe(first)
    expect(secondFactory).not.toHaveBeenCalled()
    expect(getBackend()).toBe(first)

    await Promise.all([disposeBackend(), disposeBackend()])
    expect(dispose).toHaveBeenCalledTimes(1)
    const implicit = getBackend()
    expect(implicit).not.toBeInstanceOf(MemoryBackend)

    const second = installBackend(() => memoryBackend())
    expect(second).not.toBe(first)
    expect(second).not.toBe(implicit)
  })

  test('terminates supervised generation sources only after the durable drop succeeds', async () => {
    const backend = installBackend(() => memoryBackend())
    const created = await backend.compareExchangeHead(
      'drop-order',
      { expect: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: new Uint8Array() } },
    )
    expect(created).toMatchObject({ ok: true })
    const subscription = backend.subscribeLane('drop-order', 'inc-1', { kind: 'semantic' }, () => {})
    await subscription.ready

    await expect(backend.dropGeneration('drop-order', 'inc-1')).rejects.toThrow('refusing to drop the current')
    expect(subscription.state()).toBe('ready')
  })

  test('blocks acquisition and installation for the full disposal barrier', async () => {
    const deferred = createDeferred<void>()
    const driver = memoryBackendWithDispose(() => deferred.promise)
    const backend = installBackend(() => driver)

    const disposing = disposeBackend()
    try {
      expect(getBackend).toThrow('still disposing')
      expect(() => installBackend(() => memoryBackend())).toThrow('still disposing')
      expect(disposeBackend()).toBe(disposing)
    } finally {
      deferred.resolve()
      await disposing
    }
    expect(getBackend()).not.toBeInstanceOf(MemoryBackend)
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('normalizes synchronous disposal throws and permits a different backend afterward', async () => {
    const failure = new Error('dispose synchronously failed')
    const driver = memoryBackendWithDispose(() => {
      throw failure
    })
    const backend = installBackend(() => driver)

    const first = disposeBackend()
    const second = disposeBackend()
    expect(second).toBe(first)
    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(getBackend()).not.toBeInstanceOf(MemoryBackend)
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('clears a rejected asynchronous disposal before retrying', async () => {
    const failure = new Error('dispose asynchronously failed')
    const driver = memoryBackendWithDispose(() => Promise.reject(failure))
    const backend = installBackend(() => driver)

    const first = disposeBackend()
    const second = disposeBackend()
    expect(second).toBe(first)
    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(getBackend()).not.toBeInstanceOf(MemoryBackend)
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('rejects an uncaught direct synchronous reentrant disposal through the outer outcome', async () => {
    let calls = 0
    const driver = memoryBackendWithDispose(() => {
      calls += 1
      return disposeBackend()
    })
    installBackend(() => driver)

    await expect(disposeBackend()).rejects.toThrow('backend.dispose() must not call disposeBackend()')
    expect(calls).toBe(1)
  })

  test('lets caught self-reentry defer to the backend real disposal and shares ordinary callers afterward', async () => {
    const deferred = createDeferred<void>()
    let reentrant: Promise<void> | undefined
    const driver = memoryBackendWithDispose(() => {
      reentrant = disposeBackend()
      return reentrant.catch(() => deferred.promise)
    })
    installBackend(() => driver)

    const outer = disposeBackend()
    await settle()
    await expect(reentrant).rejects.toThrow('backend.dispose() must not call disposeBackend()')
    expect(disposeBackend()).toBe(outer)
    deferred.resolve()
    await outer
  })

  test('rejects an unrelated synchronous shutdown callback while real disposal remains pending', async () => {
    const deferred = createDeferred<void>()
    let callbackPromise: Promise<void> | undefined
    const driver = memoryBackendWithDispose(() => {
      callbackPromise = disposeBackend()
      return deferred.promise
    })
    installBackend(() => driver)

    const outer = disposeBackend()
    try {
      await settle()
      expect(callbackPromise).not.toBe(outer)
      await expect(callbackPromise).rejects.toThrow('backend.dispose() must not call disposeBackend()')
      expect(getBackend).toThrow('still disposing')
      expect(disposeBackend()).toBe(outer)
    } finally {
      deferred.resolve()
      await outer
    }
  })

  test('never reinstalls an instance after disposal has begun', async () => {
    const deferred = createDeferred<void>()
    const driver = memoryBackendWithDispose(() => deferred.promise)
    installBackend(() => driver)

    const disposing = disposeBackend()
    deferred.resolve()
    await disposing
    expect(() => installBackend(() => driver)).toThrow('cannot be reinstalled')
  })

  test('defines disposal while empty and disposal during installation', async () => {
    await expect(disposeBackend()).resolves.toBeUndefined()

    const driver = memoryBackend()
    let disposing: Promise<void> | undefined
    installBackend(() => {
      disposing = disposeBackend()
      return driver
    })
    await expect(disposing).rejects.toThrow('still installing')
  })
})

function memoryBackend(): BackendDriver {
  const backend = new MemoryBackend()
  return new Proxy(backend, {
    get(target, property) {
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}

function memoryBackendWithDispose(dispose: () => void | Promise<void>): BackendDriver {
  const backend = memoryBackend()
  ;(backend as unknown as Record<string, unknown>).dispose = dispose
  return backend
}

function createDeferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

async function settle(): Promise<void> {
  for (let turn = 0; turn < 4; turn++) await Promise.resolve()
}
