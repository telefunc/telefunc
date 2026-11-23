import { afterEach, describe, expect, it, vi } from 'vitest'
import { isChannelOverflowError, type TelefuncExtensionNamespace } from 'telefunc/__internal'
import { RoomStore } from '../backend/RoomStore.js'
import { Room } from '../index.js'
import { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

afterEach(() => vi.restoreAllMocks())

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
  it('takes a stored payload of 8,388,512 bytes, the row limit less what the rows it guards hold besides it, and refuses one more', () => {
    const driver = refusingDriver(new Error('The test makes no call'))
    const lane = { kind: 'control' } as const
    expect(() => driver.assertCommittable(lane, new Uint8Array(8_388_512), 'stored')).not.toThrow()
    let refused: unknown
    try {
      driver.assertCommittable(lane, new Uint8Array(8_388_513), 'stored')
    } catch (error) {
      refused = error
    }
    expect(isChannelOverflowError(refused)).toBe(true)
  })

  it('takes a delivered payload over the row limit, as a commit keeps no row of it', () => {
    const driver = refusingDriver(new Error('The test makes no call'))
    const lane = { kind: 'inbox', member: 'm' } as const
    expect(() => driver.assertCommittable(lane, new Uint8Array(8 * 1024 * 1024 + 35), 'delivered')).not.toThrow()
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

describe('CloudflareRoomDriver: its size rule, as a call checks it before it writes', () => {
  /** From here on, the memory store checks sizes by the Cloudflare driver's rule. */
  function withCloudflareRule(): void {
    const driver = refusingDriver(new Error('The test makes no call'))
    vi.spyOn(RoomStore.prototype, 'assertCommittable').mockImplementation((...checked) =>
      driver.assertCommittable(...checked),
    )
  }

  /** Over the row limit: the longest value the rows the rule guards take, and 130 bytes more. */
  const OVER_ROW = 'x'.repeat(8 * 1024 * 1024 + 34)

  function told(error: unknown) {
    return error instanceof Error ? error.name : typeof error
  }

  it('lets through a Room.send() of a message over the row limit, as its commit keeps no row of it', async () => {
    const id = `cloudflare-send-${crypto.randomUUID()}`
    await Room.create(id)
    const recipient = await Room.join(id)
    withCloudflareRule()
    const answer = await Room.send(id, { id: recipient.id }, OVER_ROW).then(() => 'resolved', told)
    vi.restoreAllMocks()

    expect(answer).toBe('resolved')
    await Room.close(id)
  })

  it("delivers a participant's send() of a message over the row limit, as its commit keeps no row of it", async () => {
    const id = `cloudflare-participant-send-${crypto.randomUUID()}`
    const room = await Room.create(id)
    const sender = await room.join()
    const recipient = await room.join()
    const read: number[] = []
    recipient.listen((data) => {
      const length = typeof data === 'string' ? data.length : -1
      read.push(length)
      return length
    })
    withCloudflareRule()
    const answer = await sender.send(recipient.id, OVER_ROW, { ack: true }).then((reply) => reply, told)
    vi.restoreAllMocks()

    expect({ answer, read }).toEqual({ answer: OVER_ROW.length, read: [OVER_ROW.length] })
    await Room.close(id)
  })

  it("refuses a participant's send() whose recipient answers over the row limit, as the answer is kept in a row", async () => {
    const id = `cloudflare-participant-reply-${crypto.randomUUID()}`
    const room = await Room.create(id)
    const sender = await room.join()
    const recipient = await room.join()
    recipient.listen(() => OVER_ROW)
    withCloudflareRule()
    const answer = await sender.send(recipient.id, 'small', { ack: true }).then(() => 'resolved', told)
    vi.restoreAllMocks()

    expect(answer).toBe('ChannelOverflowError')
    await Room.close(id)
  })

  it('refuses a Room.removeParticipant() whose reason is over the row limit, as its leave is kept in rows', async () => {
    const id = `cloudflare-remove-${crypto.randomUUID()}`
    await Room.create(id)
    const participant = await Room.join(id)
    withCloudflareRule()
    const answer = await Room.removeParticipant(id, { id: participant.id, reason: OVER_ROW }).then(
      () => 'resolved',
      told,
    )
    vi.restoreAllMocks()

    expect({ answer, listed: (await Room.getParticipants(id)).length }).toEqual({
      answer: 'ChannelOverflowError',
      listed: 1,
    })
    await Room.close(id)
  })
})
