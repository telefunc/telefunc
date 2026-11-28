// What a call is told when its connection to the store drops, on the oldest and the newest Redis and Valkey Telefunc
// supports, for each call the guide's table names: `Room.create()`, `Broadcast.publish()`, `Room.publish()`,
// `Room.setMeta()` and `Room.close()`.
// - Dropped before the call's first command: it didn't run, and the store is as before.
// - Dropped while the call's first write waits at the store, which holds every write (`CLIENT PAUSE ... WRITE`): it may
//   have run, as Telefunc can't tell whether the store ran it. Here the store didn't.
import type { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { changedKeys, endReached, reached, runs, snapshot, STORES } from '../test/stores.js'
import type { RedisCommand } from './commands.js'
import { RedisConnection } from './RedisConnection.js'

afterAll(() => endReached())
afterEach(() => {
  vi.restoreAllMocks()
})

type Call = { name: string; before?: (id: string) => Promise<unknown>; call: (id: string) => Promise<unknown> }

const CALLS: readonly Call[] = [
  { name: 'Room.create()', call: (id) => Room.create(id) },
  { name: 'Broadcast.publish()', call: async (id) => Broadcast.publish(id, 'hello') },
  { name: 'Room.publish()', before: (id) => Room.create(id), call: (id) => Room.publish(id, 'hello') },
  { name: 'Room.setMeta()', before: (id) => Room.create(id), call: (id) => Room.setMeta(id, { topic: 'new' }) },
  { name: 'Room.close()', before: (id) => Room.create(id), call: (id) => Room.close(id) },
]

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return {
    name: error.name,
    code: 'code' in error ? String(error.code) : 'none',
    outcome: 'outcome' in error ? String(error.outcome) : 'none',
  }
}

/** Whether the store holds a client's command, as it holds a write while its writes are paused. */
async function holdsACommand(admin: Redis): Promise<boolean> {
  const info = String(await admin.call('INFO', 'clients'))
  return Number(/blocked_clients:(\d+)/.exec(info)?.[1]) > 0
}

/** Runs the backend's commands as they come, but the first one that `drops` picks: before it is sent, `drop` runs. */
function droppingAt(
  drops: (command: { writes: boolean }) => boolean,
  drop: (connection: RedisConnection) => Promise<unknown>,
) {
  const { run } = RedisConnection.prototype
  let armed = true
  vi.spyOn(RedisConnection.prototype, 'run').mockImplementation(async function <Input, Output>(
    this: RedisConnection,
    command: RedisCommand<Input, Output>,
    input: Input,
  ): Promise<Output> {
    if (!armed || !drops(command)) return await run.bind(this)(command, input)
    armed = false
    await drop(this)
    return await run.bind(this)(command, input)
  })
}

describe.runIf(runs)('a call whose connection to the store drops', () => {
  for (const [i, store] of STORES.entries()) {
    for (const [c, { name, before, call }] of CALLS.entries()) {
      it(`${store.name}: ${name} dropped before its first command didn't run`, async () => {
        const admin = await reached(store)
        const id = `dropped-first-${i}-${c}`
        await before?.(id)
        const held = await snapshot(admin)
        droppingAt(
          () => true,
          async (connection) => connection.client.disconnect(true),
        )
        const refused = await call(id).then(() => 'resolved', told)
        vi.restoreAllMocks()
        const changed = changedKeys(held, await snapshot(admin))
        admin.disconnect()

        expect({ refused, changed }).toEqual({
          refused: { name: 'BackendError', code: 'unreachable', outcome: 'not-run' },
          changed: [],
        })
      }, 60_000)

      it(`${store.name}: ${name} dropped while its first write waits at the store may have run`, async () => {
        const admin = await reached(store)
        const id = `dropped-write-${i}-${c}`
        await before?.(id)
        const held = await snapshot(admin)
        let heldAtTheStore = false
        // The write is sent while the store holds every write; the store drops the app's connections while it holds it.
        droppingAt(
          (command) => command.writes,
          async () => {
            await admin.call('CLIENT', 'PAUSE', '30000', 'WRITE')
          },
        )
        const calling = call(id).then(() => 'resolved', told)
        for (let tries = 0; tries < 2_000 && !heldAtTheStore; tries++) heldAtTheStore = await holdsACommand(admin)
        await admin.call('CLIENT', 'KILL', 'USER', 'telefunc')
        await admin.call('CLIENT', 'UNPAUSE')
        const refused = await calling
        vi.restoreAllMocks()
        const changed = changedKeys(held, await snapshot(admin))
        admin.disconnect()

        expect({ heldAtTheStore, refused, changed }).toEqual({
          heldAtTheStore: true,
          refused: { name: 'BackendError', code: 'unreachable', outcome: 'may-have-run' },
          changed: [],
        })
      }, 60_000)
    }
  }
})
