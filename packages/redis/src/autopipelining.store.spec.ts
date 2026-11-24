// An app client with autopipelining on, on the oldest and the newest Redis and Valkey Telefunc supports: the backend's
// command client writes each of its commands to its socket when it sends it, never a tick later in a batch.
import { Socket } from 'node:net'
import { Redis } from 'ioredis'
import { afterAll, describe, expect, it, vi } from 'vitest'
import { Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { standaloneRows } from '../test/endpoints.js'
import { runs, STORES } from '../test/stores.js'

const rows = standaloneRows({ enableAutoPipelining: true })
afterAll(() => rows.end())

describe.runIf(runs)('an app client with autopipelining on', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: the backend writes each command it sends to its socket on its own`, async () => {
      const { command } = await rows.reached(store)
      // The commands the backend's command client sends, and the writes to its socket, as concurrent calls run.
      const counts = { sent: 0, written: 0 }
      const { sendCommand } = Redis.prototype
      const sends = vi.spyOn(Redis.prototype, 'sendCommand').mockImplementation(function (this: Redis, ...args) {
        if (this.options.port === command.port) counts.sent++
        return sendCommand.apply(this, args)
      })
      const { write } = Socket.prototype
      const writes = vi.spyOn(Socket.prototype, 'write').mockImplementation(function (
        this: Socket,
        ...args: Parameters<Socket['write']>
      ) {
        if (this.remotePort === command.port) counts.written++
        return write.apply(this, args)
      })
      await Promise.all([
        Room.get(`autopipelining-${i}-a`).catch(() => {}),
        Room.get(`autopipelining-${i}-b`).catch(() => {}),
        Broadcast.publish(`autopipelining-${i}`, 'one'),
        Broadcast.publish(`autopipelining-${i}`, 'two'),
      ])
      sends.mockRestore()
      writes.mockRestore()

      expect(counts.sent).toBeGreaterThanOrEqual(4)
      expect(counts.written).toBe(counts.sent)
    }, 60_000)
  }
})
