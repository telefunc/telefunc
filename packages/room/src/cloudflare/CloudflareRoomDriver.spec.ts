import { describe, expect, it } from 'vitest'
import { isChannelOverflowError, type TelefuncExtensionNamespace } from 'telefunc/__internal'
import { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

/** A driver whose room authority rejects every call with `refusal`. */
function refusingDriver(refusal: Error): CloudflareRoomDriver {
  const namespace: TelefuncExtensionNamespace = {
    idFromName: (name) => name,
    idFromString: (id) => id,
    get: () => ({ telefuncDurableObjectCall: () => Promise.reject(refusal) }),
  }
  return new CloudflareRoomDriver({ getNamespace: () => namespace, locationFallback: 'weur' })
}

const exchange = { expect: [], mutations: [{ key: 'k', bytes: new Uint8Array(1) }] }

describe('CloudflareRoomDriver: what its SQLite row limit refuses', () => {
  it('takes a payload of 8,388,512 bytes, the row limit less what the rows it guards hold besides it, and refuses one more', () => {
    const driver = refusingDriver(new Error('The test makes no call'))
    const lane = { kind: 'control' } as const
    expect(() => driver.assertCommittable(lane, new Uint8Array(8_388_512))).not.toThrow()
    let refused: unknown
    try {
      driver.assertCommittable(lane, new Uint8Array(8_388_513))
    } catch (error) {
      refused = error
    }
    expect(isChannelOverflowError(refused)).toBe(true)
  })

  it("tells a write the authority's SQLite refused for its size as ChannelOverflowError", async () => {
    const driver = refusingDriver(new Error('string or blob too big: SQLITE_TOOBIG'))
    const refused = await driver.compareExchangeCells('room', 'inc', exchange).then(
      () => null,
      (error: unknown) => error,
    )
    expect(isChannelOverflowError(refused)).toBe(true)
  })

  it('leaves any other refusal of a write as it is', async () => {
    const other = new Error('Network connection lost.')
    const driver = refusingDriver(other)
    await expect(driver.compareExchangeCells('room', 'inc', exchange)).rejects.toBe(other)
  })
})
