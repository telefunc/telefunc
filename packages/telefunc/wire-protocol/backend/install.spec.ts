import { afterEach, describe, expect, test, vi } from 'vitest'
import { disposeRoomBackend, getRoomBackend, installRoomBackend, type RoomBackendSpi } from './install.js'
import { MemoryRoomBackend } from './memory/backend.js'

afterEach(async () => {
  await disposeRoomBackend()
})

describe('Room backend installation', () => {
  test('fails clearly before an installation exists', () => {
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
  })

  test('rejects a mismatched SPI version before committing the installation', () => {
    const backend = memoryBackend()
    ;(backend as unknown as { spiVersion: number }).spiVersion = 2

    expect(() => installRoomBackend(() => backend)).toThrow('spiVersion 2; expected 1')
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
  })

  test('rejects a backend with a missing core method', () => {
    const backend = memoryBackend()
    ;(backend as unknown as Record<string, unknown>).commitLane = undefined

    expect(() => installRoomBackend(() => backend)).toThrow('missing required method "commitLane"')
  })

  test('rejects unusable receiver and directory capability declarations', () => {
    const noReceivers = memoryBackend()
    ;(noReceivers.capabilities as { receivers: string }).receivers = 'none'
    expect(() => installRoomBackend(() => noReceivers)).toThrow('capabilities.receivers')

    const noDirectoryMethod = memoryBackend()
    ;(noDirectoryMethod as unknown as Record<string, unknown>).directoryList = undefined
    expect(() => installRoomBackend(() => noDirectoryMethod)).toThrow('missing required method "directoryList"')
  })

  test('latches the first validated factory and disposes it once before a fresh installation', async () => {
    const first = memoryBackend()
    const dispose = vi.spyOn(first, 'dispose')
    const secondFactory = vi.fn(() => memoryBackend())

    expect(installRoomBackend(() => first)).toBe(first)
    expect(installRoomBackend(secondFactory)).toBe(first)
    expect(secondFactory).not.toHaveBeenCalled()
    expect(getRoomBackend()).toBe(first)

    await Promise.all([disposeRoomBackend(), disposeRoomBackend()])
    expect(dispose).toHaveBeenCalledTimes(1)
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')

    const second = installRoomBackend(() => memoryBackend())
    expect(second).not.toBe(first)
  })
})

function memoryBackend(): RoomBackendSpi {
  const backend = new MemoryRoomBackend()
  return new Proxy(backend, {
    get(target, property) {
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}
