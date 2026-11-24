import { expect, it } from 'vitest'
import type { TelefuncExtensionNamespace } from 'telefunc/__internal'
import { openRoomAuthority, type CloudflareRoomAuthorityStub } from './objects.js'
import type { OperationWire } from './RoomAuthority.js'
import { ROOM_CLOUDFLARE_EXTENSION } from '../server/constants.js'
import type { OperationArgs, OperationValue } from '../server/store/roomOperations.js'

it("calls a room authority's method through the Durable Object, by its name, with its arguments", async () => {
  const calls: unknown[][] = []
  const namespace: TelefuncExtensionNamespace = {
    idFromName: (name) => name,
    idFromString: (id) => id,
    get: () => ({
      telefuncDurableObjectCall: async (...call) => (calls.push(call), { kind: 'read', cells: new Map() }),
    }),
  }
  const authority = openRoomAuthority(namespace, 'room', 'weur')
  await authority.readCells('inc', { keys: ['k'] })
  expect(calls).toEqual([[ROOM_CLOUDFLARE_EXTENSION, 'readCells', ['inc', { keys: ['k'] }]]])
})

/** Compiled, never run: a call the authority doesn't serve as written fails to type-check. */
function wrongCalls(
  authority: CloudflareRoomAuthorityStub,
  join: OperationArgs['join'],
  leave: OperationArgs['leave'],
): void {
  // @ts-expect-error The authority serves no `readEverything()`.
  void authority.readEverything()
  // @ts-expect-error `readCells()` reads an incarnation by its text.
  void authority.readCells(1, { keys: ['k'] })
  // @ts-expect-error `readHead()` reads a value it returns, not one it is given.
  void authority.readHead('inc')
  // An operation runs by its registered name, with that operation's arguments, and answers that operation's value.
  const left: Promise<OperationWire<OperationValue['leave']>> = authority.runOperation('inc', {
    name: 'leave',
    args: leave,
  })
  // @ts-expect-error Room registers no operation `joinAll`.
  void authority.runOperation('inc', { name: 'joinAll', args: join })
  // @ts-expect-error A join runs with a join's arguments, not a leave's.
  void authority.runOperation('inc', { name: 'join', args: leave })
  // @ts-expect-error A leave answers a leave's value, not a join's.
  const joined: Promise<OperationWire<OperationValue['join']>> = authority.runOperation('inc', {
    name: 'leave',
    args: leave,
  })
  void left
  void joined
}
