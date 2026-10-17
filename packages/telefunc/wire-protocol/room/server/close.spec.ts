import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_SESSION_TTL_MS,
  ROOM_EMPTY_TIMEOUT_MS,
  ROOM_DEPARTURE_TIMEOUT_MS,
} from './constants.js'
import { HOLDS_CELL_KEY } from './cells.js'
import { Room } from './statics.js'
import { ServerRoom } from './room.js'
import { config } from '../../../node/server/serverConfig.js'
import { RoomStubChannel } from './stub.js'
import { getRoomBackend } from '../../backend/install.js'
import { MemoryBackendState } from '../../backend/memory/backend.js'
import {
  anotherServer,
  captureOutcome,
  decoder,
  disposeRoomBackend,
  driver,
  HOLD_RECORD_KEYS,
  installRoomBackend,
  joinThrough,
  register,
  serve,
  syncOf,
} from '../../test/room.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
describe('a room nothing holds closes on its own', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    config.room = {}
  })
  const isListed = async (id: string) => (await Room.list()).some((room) => room.id === id)
  /** Watches the room from a server handle, which doesn't hold it. */
  async function observe(id: string) {
    const observer = await Room.get(id)
    const closed = vi.fn()
    observer.onClose(closed)
    return closed
  }
  /** As `Room.close()` leaves it: a tombstone, its holders told, and no longer listed. */
  async function expectClosed(id: string, closed: () => void) {
    await vi.waitFor(async () => expect((await getRoomBackend().readHead(id))?.state).toBe('closed'))
    await vi.waitFor(() => expect(closed).toHaveBeenCalledOnce())
    expect(await isListed(id)).toBe(false)
  }
  /** One member joins and leaves. */
  async function visit(room: Room) {
    await (await room.join()).leave()
  }

  it.each([120_000, -120_000])(
    "closes a room departureTimeout after its last member left, and a new room nobody joins emptyTimeout after its creation, with the app servers' clocks %i ms off the store's",
    async (skew) => {
      await installRoomBackend(new MemoryBackendState({ now: () => Date.now() - skew }))
      const left = await Room.create('skewed-left', { departureTimeout: 20_000 })
      const leftClosed = await observe(left.id)
      await visit(left)
      const unjoined = await Room.create('skewed-unjoined', { emptyTimeout: 20_000 })
      const unjoinedClosed = await observe(unjoined.id)
      await vi.advanceTimersByTimeAsync(19_000)
      expect((await getRoomBackend().readHead(left.id))?.state).toBe('open')
      expect((await getRoomBackend().readHead(unjoined.id))?.state).toBe('open')
      await vi.advanceTimersByTimeAsync(2_000)
      await expectClosed(left.id, leftClosed)
      await expectClosed(unjoined.id, unjoinedClosed)
    },
  )
  it('keeps a room a page was returned open for 10 minutes, and its join then works, while one nobody holds closes at emptyTimeout', async () => {
    const held = (await Room.create('page-holds')) as ServerRoom
    const unheld = await Room.create('nobody-holds')
    const unheldClosed = await observe(unheld.id)
    const { stub } = serve(held)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS)
    await expectClosed(unheld.id, unheldClosed)
    await vi.advanceTimersByTimeAsync(10 * 60_000 - ROOM_EMPTY_TIMEOUT_MS)
    const id = await joinThrough(stub)
    expect((await Room.getParticipants(held.id)).map((member) => member.id)).toEqual([id])
  })
  it('closes a room emptyTimeout after the page it was returned to closes, when nobody joined it', async () => {
    const room = (await Room.create('page-closed-unjoined')) as ServerRoom
    const closed = await observe(room.id)
    const { stub } = serve(room)
    await vi.advanceTimersByTimeAsync(60_000)
    stub._onPeerClose()
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS - 1_000)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1_000)
    await expectClosed(room.id, closed)
  })
  it('closes a room departureTimeout after its last member left with its page', async () => {
    const room = (await Room.create('page-closed-joined')) as ServerRoom
    const closed = await observe(room.id)
    const { stub } = serve(room)
    await joinThrough(stub)
    await vi.advanceTimersByTimeAsync(60_000)
    stub._onPeerClose()
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS - 1_000)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1_000)
    await expectClosed(room.id, closed)
  })
  it("restores a page's view whose first write failed at the next heartbeat", async () => {
    const room = (await Room.create('view-write-failed')) as ServerRoom
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(new Error('backend unavailable'))
    serve(room)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS + ROOM_HEARTBEAT_INTERVAL_MS)
    expect(await isListed(room.id)).toBe(true)
  })
  it("renews the views of hundreds of pages on one server in one write, with the room's other writes alongside", async () => {
    const room = (await Room.create('many-views')) as ServerRoom
    const views = (room._replica as unknown as { _views: Map<RoomStubChannel, Promise<void>> })._views
    for (let page = 0; page < 300; page++) await views.get(register(room))
    const writes = vi.spyOn(driver, 'compareExchangeCells')
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const tick = captureOutcome(syncOf(room)._heartbeatTick())
    const join = captureOutcome(room.join())
    await vi.advanceTimersByTimeAsync(5_000)
    const viewWrites = writes.mock.calls.filter(([, , cx]) => cx.mutations.some(({ key }) => key.startsWith('v:')))
    expect({
      tick: tick.value,
      joined: (join.value as { id?: string }).id !== undefined,
      report: report.mock.calls,
    }).toEqual({
      tick: undefined,
      joined: true,
      report: [],
    })
    expect(viewWrites.length).toBeLessThanOrEqual(2)
  })
  it("lets a page's view lapse once its server stops renewing it, as a member does, and closes the room emptyTimeout after", async () => {
    const room = (await Room.create('view-lapses')) as ServerRoom
    const closed = await observe(room.id)
    // A view the server holding the page wrote before it died, renewed by nothing since.
    const other = await anotherServer()
    const page = (await other.Room.get(room.id)) as ServerRoom
    await (page._replica as unknown as { _views: Map<RoomStubChannel, Promise<void>> })._views.get(register(page))
    await other.crash()
    await vi.advanceTimersByTimeAsync(ROOM_SESSION_TTL_MS + ROOM_HEARTBEAT_INTERVAL_MS)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS)
    await expectClosed(room.id, closed)
  })
  it("keeps a room whose creation's hold count failed to write open under its members", async () => {
    vi.spyOn(driver, 'compareExchangeCells').mockRejectedValueOnce(new Error('transient'))
    await expect(Room.create('hold-write-failed')).rejects.toThrow('transient')
    const room = await Room.getOrCreate('hold-write-failed')
    const member = await room.join()
    member.onLeave(() => {})
    await vi.advanceTimersByTimeAsync(10 * 60_000)
    expect((await getRoomBackend().readHead(room.id))?.state).toBe('open')
    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
  })
  it("counts a member who joins from another server before the new room's first cell write, closing it only after they leave", async () => {
    const other = await anotherServer()
    const exchange = driver.compareExchangeCells.bind(driver)
    let joined: ReturnType<typeof Room.join> | undefined
    vi.spyOn(driver, 'compareExchangeCells').mockImplementation(async (roomId, inc, cx) => {
      if (joined === undefined) {
        joined = other.Room.join(roomId)
        await joined
      }
      return await exchange(roomId, inc, cx)
    })
    const room = await Room.create('join-before-hold', { emptyTimeout: 1_000, departureTimeout: 5_000 })
    const closed = await observe(room.id)
    const member = await joined!
    await vi.advanceTimersByTimeAsync(60_000)
    expect((await Room.getParticipants(room.id)).map(({ id }) => id)).toEqual([member.id])
    await member.leave()
    await vi.advanceTimersByTimeAsync(5_000 - 1)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1)
    await expectClosed(room.id, closed)
  })
  it("keeps a room's lapse through a meta write", async () => {
    const room = await Room.create('meta-keeps-lapse', { emptyTimeout: 60_000 })
    const { expiresAt } = (await getRoomBackend().readHead(room.id))!
    expect(expiresAt).toBeGreaterThan(Date.now())
    await vi.advanceTimersByTimeAsync(10_000)
    await Room.setMeta(room.id, { topic: 'kept' })
    expect((await getRoomBackend().readHead(room.id))?.expiresAt).toBe(expiresAt)
  })
  it("closes a room departureTimeout after the leave that left it unheld, though another server's member left it last", async () => {
    const room = (await Room.create('last-leave-elsewhere', { departureTimeout: 60_000 })) as ServerRoom
    const closed = await observe(room.id)
    const member = await room.join()
    // Another server's member joins, and leaves 10 s after this server's; that server is gone before its timer fires.
    const other = await anotherServer()
    const elsewhere = await other.Room.join(room.id)
    await member.leave()
    await vi.advanceTimersByTimeAsync(10_000)
    await elsewhere.leave()
    await other.crash()
    // This server's heartbeat finds the room unheld, closing 60 s after that last leave.
    await vi.advanceTimersByTimeAsync(60_000 - 1)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(1)
    await expectClosed(room.id, closed)
  })
  it('waits on when its close timer fires early: another server left the room unheld later than this one armed it for', async () => {
    const room = (await Room.create('close-fires-early', { emptyTimeout: 10_000 })) as ServerRoom
    const closed = await observe(room.id)
    // A page on another server, 4 s in: held, then unheld again, closing 4 s after this server's timer fires.
    const other = await anotherServer()
    await vi.advanceTimersByTimeAsync(4_000)
    const page = (await other.Room.get(room.id)) as ServerRoom
    const { stub } = serve(page)
    await (page._replica as unknown as { _views: Map<RoomStubChannel, Promise<void>> })._views.get(stub)
    stub._onPeerClose()
    const holds = async () => {
      const read = await driver.readCells(room.id, room._incarnation.inc, { keys: [HOLDS_CELL_KEY] })
      return 'cells' in read ? JSON.parse(decoder.decode(read.cells.get(HOLDS_CELL_KEY)!)).value : null
    }
    await vi.advanceTimersByTimeAsync(0)
    expect(await holds()).toBe(0)
    await other.crash()
    await vi.advanceTimersByTimeAsync(6_000)
    expect(await isListed(room.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(4_000)
    await expectClosed(room.id, closed)
  })
  it('keeps a room that never closes once joined open under a first join that lands while its new expiry is set', async () => {
    const readCells = driver.readCells.bind(driver)
    let holdReads = 0
    let joined: Promise<unknown> = Promise.resolve()
    vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
      const read = await readCells(roomId, inc, selector)
      // The first read of the hold record is the one that sets the new room's expiry: a join commits after it.
      if ('keys' in selector && selector.keys.join() === HOLD_RECORD_KEYS && ++holdReads === 1) {
        joined = Room.join(roomId)
        await joined
        await vi.advanceTimersByTimeAsync(0) // the join's own expiry write
      }
      return read
    })
    const creating = Room.create('first-join-races-expiry', { emptyTimeout: 1_000, departureTimeout: Infinity })
    await vi.advanceTimersByTimeAsync(100) // a compare-exchange the join's write raced waits out its backoff
    await creating
    const { id } = (await joined) as { id: string }
    await vi.advanceTimersByTimeAsync(60_000)
    expect((await getRoomBackend().readHead('first-join-races-expiry'))?.state).toBe('open')
    expect((await Room.getParticipants('first-join-races-expiry')).map((member) => member.id)).toEqual([id])
  })
  it('lets no hidden participant hold a room: alone it closes at emptyTimeout, after players at departureTimeout', async () => {
    const alone = await Room.create('hidden-alone')
    const aloneClosed = await observe(alone.id)
    await alone.join({ hidden: true, identity: 'authority' })
    const played = await Room.create('hidden-after-players')
    const playedClosed = await observe(played.id)
    const authority = await played.join({ hidden: true, identity: 'authority' })
    const authorityLeft = vi.fn()
    authority.onLeave(authorityLeft)
    await visit(played)
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS)
    await expectClosed(played.id, playedClosed)
    expect(authorityLeft).toHaveBeenCalledWith({ type: 'closed' })
    expect(await isListed(alone.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(ROOM_EMPTY_TIMEOUT_MS - ROOM_DEPARTURE_TIMEOUT_MS)
    await expectClosed(alone.id, aloneClosed)
  })
  it('keeps a room held again before its timeout, and closes it once nothing holds it again', async () => {
    const room = await Room.create('held-again')
    const closed = await observe(room.id)
    await visit(room)
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS - 5_000)
    const back = await room.join()
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS)
    expect(await isListed(room.id)).toBe(true)
    await back.leave()
    await vi.advanceTimersByTimeAsync(ROOM_DEPARTURE_TIMEOUT_MS)
    await expectClosed(room.id, closed)
  })
  it('keeps a room whose timeout is Infinity open until Room.close()', async () => {
    const left = await Room.create('never-after-leave', { departureTimeout: Infinity })
    const unjoined = await Room.getOrCreate('never-unjoined', { emptyTimeout: Infinity })
    await visit(left)
    await vi.advanceTimersByTimeAsync(60 * 60_000)
    expect(await isListed(left.id)).toBe(true)
    expect(await isListed(unjoined.id)).toBe(true)
  })
  it("takes its timeouts from config.room, which a room's own options override", async () => {
    config.room = { departureTimeout: 1_000 }
    const byConfig = await Room.create('config-timeout')
    const byConfigClosed = await observe(byConfig.id)
    const own = await Room.create('own-timeout', { departureTimeout: 5_000 })
    const ownClosed = await observe(own.id)
    await visit(byConfig)
    await visit(own)
    await vi.advanceTimersByTimeAsync(1_000)
    await expectClosed(byConfig.id, byConfigClosed)
    expect(await isListed(own.id)).toBe(true)
    await vi.advanceTimersByTimeAsync(4_000)
    await expectClosed(own.id, ownClosed)
  })
  it('accepts a timeout of 0 to 2^31 - 1 ms or Infinity, and refuses anything else as a usage error', async () => {
    for (const valid of [0, 2 ** 31 - 1, Infinity]) {
      await Room.create(`valid-${valid}`, { emptyTimeout: valid, departureTimeout: valid })
    }
    for (const invalid of [-1, 1.5, 2 ** 31, Number.NaN, '1000']) {
      const message = 'should be a non-negative safe integer of milliseconds, at most 2147483647'
      await expect(Room.create('invalid', { emptyTimeout: invalid as number })).rejects.toThrow(
        `\`options.emptyTimeout\` ${message}`,
      )
      await expect(Room.getOrCreate('invalid', { departureTimeout: invalid as number })).rejects.toThrow(
        `\`options.departureTimeout\` ${message}`,
      )
    }
  })
})
