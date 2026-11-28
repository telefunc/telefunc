// The store specs' fixtures on a server, on the oldest and the newest Redis and Valkey Telefunc supports: a row's own
// store, each connection role through its own endpoint, and what a forwarder holds, passes and records.
import { connect } from 'node:net'
import { afterAll, describe, expect, it } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { carriers, delivered, standaloneRows } from '../test/endpoints.js'
import { runs, STORES } from '../test/stores.js'

const rows = standaloneRows()
afterAll(() => rows.end())

/** Whether `promise` is still pending once the tasks queued before it ran. */
async function pending(promise: Promise<unknown>): Promise<boolean> {
  const unsettled = Symbol()
  const first = await Promise.race([
    promise.then(
      () => null,
      () => null,
    ),
    new Promise<symbol>((resolve) => setImmediate(resolve, unsettled)),
  ])
  return first === unsettled
}

describe.runIf(runs)("the store specs' fixtures on a server", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a standalone row's roles take their own endpoints, and a forwarder holds, passes and records`, async () => {
      const at = await rows.reached(store)
      const messages = await delivered(`fixtures-${i}`)
      const roles = {
        subscribes: carriers([at.command, at.subscriber], 'subscribe'),
        // A script goes as EVAL on its first use on a socket, then as EVALSHA.
        scripts: carriers([at.command, at.subscriber], 'eval'),
      }

      // A read whose reply is held, then passed one byte, then let through.
      at.command.hold('toClient')
      const [passage] = at.command.passages().filter(({ ended }) => ended === undefined)
      if (passage === undefined) throw new Error('the command client has no open connection')
      const [written, replied] = [passage.toStore.bytes, passage.toClient.bytes]
      const read = Room.get('r-none')
      await at.command.until(() => passage.toStore.bytes > written)
      const heldPending = await pending(read)
      at.command.pass('toClient', 1)
      await at.command.until(() => passage.toClient.bytes === replied + 1)
      const passedPending = await pending(read)
      at.command.flow('toClient')
      // The room isn't there: the store answered.
      const answered = await read.then(
        () => 'resolved',
        (error: unknown) => (error instanceof BackendError ? String(error) : 'answered'),
      )

      // A client that resets its connection, and one that closes it.
      const ends: unknown[] = []
      for (const close of ['reset', 'end'] as const) {
        const client = connect(at.command.port, '127.0.0.1')
        client.on('error', () => {})
        await new Promise<void>((resolve) => client.once('connect', resolve))
        const opened = at.command.passages().at(-1)
        client.write('PING\r\n')
        await at.command.until(() => opened !== undefined && opened.toClient.bytes > 0)
        if (close === 'reset') client.resetAndDestroy()
        else client.end()
        await at.command.until(() => opened?.ended !== undefined)
        ends.push(opened?.ended?.how)
      }
      at.admin.disconnect()

      expect({ messages, roles, heldPending, passedPending, answered, ends }).toEqual({
        messages: ['one'],
        roles: { subscribes: [1], scripts: [0] },
        heldPending: true,
        passedPending: true,
        answered: 'answered',
        ends: ['reset', 'end'],
      })
    }, 120_000)
  }
})
