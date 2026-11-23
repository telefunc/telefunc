// Key prefixes that installRedis() accepts, on a Redis Cluster of the oldest and the newest Redis and Valkey Telefunc
// supports: a server whose backend has the prefix creates a room and lists it, whatever the prefix holds: a `}`, which
// would end a hash tag the prefix is in, a `:` or a space. The room directory's keys share one tag of their own for
// each prefix, and two prefixes that differ only after a `}` have their directories in two slots.
import { execFile } from 'node:child_process'
import { setTimeout as sleep } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { describe, expect, it } from 'vitest'
import { removeStores, runs, STORES, startStore } from '../test/stores.js'
import { directoryChecksKey, directoryIndexKey, directoryTagsKey } from './keys.js'

const PREFIXES = ['app:', '}app:', 'a}b:', 'a}c:', 'my app '] as const

/** What a server whose backend has key prefix `prefix`, on the Cluster at `host`, prints of its `Room.create(id)` and of
 *  whether `Room.list()` then lists it: a process of its own, on the built packages. */
async function createdAt(host: string, prefix: string, id: string): Promise<{ created: string; listed: string }> {
  const server = `
import { Cluster } from 'ioredis'
import { Room } from '@telefunc/room'
import { installRedis } from '@telefunc/redis'
const [host, prefix, id] = process.argv.slice(1)
const cluster = new Cluster([{ host, port: 6379 }], { redisOptions: { username: 'telefunc', password: 'pw' } })
installRedis(cluster, { prefix })
const created = await Room.create(id).then(() => 'created', (error) => 'refused: ' + error.message)
const listed = await Room.list().then((rooms) => String(rooms.some((room) => room.id === id)), (error) => 'refused: ' + error.message)
console.log('told:' + JSON.stringify({ created, listed }))
process.exit(0)
`
  const cwd = fileURLToPath(new URL('..', import.meta.url))
  const { stdout } = await promisify(execFile)(
    process.execPath,
    ['--input-type=module', '-e', server, host, prefix, id],
    {
      cwd,
    },
  )
  const told = stdout.split('\n').find((line) => line.startsWith('told:'))
  if (told === undefined) return { created: stdout, listed: stdout }
  const parsed: unknown = JSON.parse(told.slice('told:'.length))
  return typeof parsed === 'object' && parsed !== null && 'created' in parsed && 'listed' in parsed
    ? { created: String(parsed.created), listed: String(parsed.listed) }
    : { created: told, listed: told }
}

/** The hash tag Redis hashes `key` by: what is between its first `{` and the first `}` after it, when not empty. */
function hashTag(key: string): string | null {
  const open = key.indexOf('{')
  const close = open === -1 ? -1 : key.indexOf('}', open + 1)
  return close > open + 1 ? key.slice(open + 1, close) : null
}

describe.runIf(runs)('key prefixes on a Cluster', () => {
  for (const store of STORES) {
    it(`${store.name}: a server with each prefix creates a room and lists it, its directory under a tag of its own`, async () => {
      removeStores()
      const { host, admin } = await startStore(store, '--cluster-enabled', 'yes')
      // A node that never met another announces no address of its own: CLUSTER SLOTS would name it as ''.
      await admin.call('CONFIG', 'SET', 'cluster-announce-ip', host)
      await admin.call('CLUSTER', 'ADDSLOTSRANGE', '0', '16383')
      for (let tries = 0; !String(await admin.call('CLUSTER', 'INFO')).includes('cluster_state:ok'); tries++) {
        if (tries > 400) throw new Error(`${store.name}: the Cluster never came up`)
        await sleep(25)
      }
      await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
      const slot = async (key: string) => Number(await admin.call('CLUSTER', 'KEYSLOT', key))
      const told: Record<string, { created: string; listed: string }> = {}
      const directory: Record<string, { tag: string | null; slot: number; slots: number }> = {}
      for (const prefix of PREFIXES) {
        told[prefix] = await createdAt(host, prefix, 'r')
        const keys = [directoryIndexKey(prefix), directoryTagsKey(prefix), directoryChecksKey(prefix)]
        const slots = await Promise.all(keys.map(slot))
        directory[prefix] = { tag: hashTag(keys[0] ?? ''), slot: slots[0] ?? -1, slots: new Set(slots).size }
      }
      console.log(JSON.stringify({ store: store.name, told, directory }))
      admin.disconnect()

      const tags = PREFIXES.map((prefix) => directory[prefix]?.tag)
      expect({
        told,
        oneSlotEach: PREFIXES.every((prefix) => directory[prefix]?.slots === 1),
        tagsApart: new Set(tags).size === PREFIXES.length && tags.every((tag) => tag !== null),
        slotsApart: directory['a}b:']?.slot !== directory['a}c:']?.slot,
      }).toEqual({
        told: Object.fromEntries(PREFIXES.map((prefix) => [prefix, { created: 'created', listed: 'true' }])),
        oneSlotEach: true,
        tagsApart: true,
        slotsApart: true,
      })
    }, 120_000)
  }
})
