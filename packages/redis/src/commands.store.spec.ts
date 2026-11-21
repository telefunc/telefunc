// Every write script, on the oldest and the newest Redis and Valkey Telefunc supports: a call that finds a key holding
// another type, or a value Telefunc didn't write, is refused before its first write and says it didn't run. Each store
// runs in Docker; a machine without Docker skips them, as the Docker e2e tests do, and CI runs them.
import { Redis } from 'ioredis'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { BackendError, Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import { REDIS_COMMANDS, type RedisCommand } from './commands.js'
import { installRedis } from './index.js'
import { DEFAULT_PREFIX } from './keys.js'
import { RedisConnection } from './RedisConnection.js'
import { removeStores, runs, STORES, startStore } from '../test/stores.js'

type Value = { type: string; content: string[]; pttl: number }
type Snapshot = Map<string, Value>
type Recorded = {
  name: string
  keys: readonly string[]
  before: Snapshot
  replay: (connection: RedisConnection) => Promise<unknown>
}
type Store = { name: string; admin: Redis; trialClient: Redis }

const hex = (values: Buffer[]) => values.map((value) => value.toString('hex'))

/** Every key, read by its type: unlike DUMP, it restores on any version of either store. */
async function snapshot(admin: Redis): Promise<Snapshot> {
  const shot: Snapshot = new Map()
  for (const key of (await admin.keys('*')).sort()) {
    const type = await admin.type(key)
    let content: string[]
    if (type === 'string') content = hex([(await admin.getBuffer(key)) ?? Buffer.alloc(0)])
    else if (type === 'hash') {
      const fields = Object.entries(await admin.hgetallBuffer(key))
      content = fields.map(([field, value]) => `${Buffer.from(field).toString('hex')}=${value.toString('hex')}`).sort()
    } else if (type === 'set') content = hex(await admin.smembersBuffer(key)).sort()
    else if (type === 'zset') content = hex(await admin.zrangeBuffer(key, 0, -1, 'WITHSCORES'))
    else content = hex(await admin.lrangeBuffer(key, 0, -1))
    shot.set(key, { type, content, pttl: await admin.pttl(key) })
  }
  return shot
}

async function restore(admin: Redis, shot: Snapshot): Promise<void> {
  await admin.flushall()
  for (const [key, { type, content, pttl }] of shot) {
    const bytes = (text: string) => Buffer.from(text, 'hex')
    if (type === 'string') await admin.set(key, bytes(content[0] ?? ''))
    else if (type === 'hash') {
      for (const pair of content) {
        const [field = '', value = ''] = pair.split('=')
        await admin.hset(key, bytes(field), bytes(value))
      }
    } else if (type === 'set') await admin.sadd(key, ...content.map(bytes))
    else if (type === 'zset') {
      for (let i = 0; i < content.length; i += 2)
        await admin.zadd(key, Number(bytes(content[i + 1] ?? '')), bytes(content[i] ?? ''))
    } else await admin.rpush(key, ...content.map(bytes))
    if (pttl > 0) await admin.pexpire(key, pttl)
  }
}

/** The keys whose value differs, or whose TTL came or went or ran down by more than the trial took. */
function changed(before: Snapshot, after: Snapshot, elapsed: number): string[] {
  const diff: string[] = []
  for (const key of new Set([...before.keys(), ...after.keys()])) {
    const a = before.get(key)
    const b = after.get(key)
    if (a === undefined || b === undefined || a.type !== b.type || a.content.join() !== b.content.join()) diff.push(key)
    else if (a.pttl < 0 !== b.pttl < 0 || b.pttl > a.pttl || b.pttl < a.pttl - elapsed - 100) diff.push(`${key} (ttl)`)
  }
  return diff
}

/** The commands the store ran since its stats were reset, by name, its own calls to the stats and to scripts left out. */
async function commandsRun(admin: Redis): Promise<Map<string, number>> {
  const run = new Map<string, number>()
  for (const [, name = '', calls = ''] of (await admin.info('commandstats')).matchAll(
    /^cmdstat_([^:]+):calls=(\d+)/gm,
  )) {
    if (!/^(evalsha|eval|info|config\|.*|script\|.*)$/.test(name)) run.set(name, Number(calls))
  }
  return run
}

async function connected(host: string): Promise<Redis> {
  // Without lazyConnect: connect() rejects on the first refused attempt, while the server may still be starting.
  const redis = new Redis({ host, retryStrategy: (times) => (times > 200 ? null : 50), maxRetriesPerRequest: null })
  redis.on('error', () => {})
  await redis.ping()
  return redis
}

/** The store's container, once its server answers; it's removed after the tests, even when it never answered. */
async function start(store: (typeof STORES)[number]): Promise<Store> {
  const { host, admin } = await startStore(store)
  return { name: store.name, admin, trialClient: await connected(host) }
}

let prefix = ''
const stores: Store[] = []
/** Each store's connection for the trials, made once the recording knows the backend's prefix. */
const trials = new Map<Store, RedisConnection>()
const recorded: Recorded[] = []

/** Every write script the scenario's public calls make, with the store as it was before each. A room id and a payload
 *  hold the refusal's prefix, which Telefunc's guard alone puts at the start of a reply. */
async function record(store: Store): Promise<void> {
  const app = new Redis({ host: store.admin.options.host })
  app.on('error', () => {})
  const run = RedisConnection.prototype.run
  const spy = vi.spyOn(RedisConnection.prototype, 'run').mockImplementation(async function <Input, Output>(
    this: RedisConnection,
    command: RedisCommand<Input, Output>,
    input: Input,
  ): Promise<Output> {
    if (command.writes) {
      prefix = this.prefix
      recorded.push({
        name: command.name,
        keys: command.invoke(this.prefix, input).keys,
        before: await snapshot(store.admin),
        replay: (connection) => run.bind(connection)(command, input),
      })
    }
    return await run.bind(this)(command, input)
  })
  installRedis(app)
  const listening = Broadcast.subscribe('s', () => {})
  await listening.ready
  await Broadcast.publish('s', 'to a subscriber')
  listening()
  await Broadcast.publish('TFNOTRUN WRONGTYPE k', 'TFNOTRUN ERR one')
  await Broadcast.publish('TFNOTRUN WRONGTYPE k', 'TFNOTRUN ERR two')
  const roomId = 'r-TFNOTRUN WRONGTYPE'
  const room = await Room.create(roomId, {})
  Room.guard(room, { anyoneCanJoin: true })
  await Room.setMeta(roomId, { a: '1' })
  await Room.updateMeta(roomId, { b: '2' })
  await Room.publish(roomId, 'TFNOTRUN ERR hello')
  const alice = await Room.join(roomId, { meta: { name: 'alice' }, identity: 'u1' })
  await Room.join(roomId, { meta: { name: 'bob' }, identity: 'u2' })
  await alice.setMeta({ name: 'alice2' })
  await alice.publish('from alice')
  await Room.send(roomId, { identity: 'u2' }, 'dm')
  await Room.removeParticipant(roomId, { identity: 'u2' })
  await alice.leave()
  await Room.close(roomId)
  await Room.create('r2', {})
  await Room.close('r2')
  spy.mockRestore()
  app.disconnect()
}

type Fault = { name: string; plant: (admin: Redis) => Promise<void> }

/** Each key of the call in turn holding a list, a type Telefunc keeps nowhere. */
function anotherType(call: Recorded): Fault[] {
  return [...new Set(call.keys)].map((key) => ({
    name: `${key} holds a list`,
    plant: async (admin) => {
      await admin.del(key)
      await admin.rpush(key, 'x')
    },
  }))
}

/** Each string and each hash of the store before the call in turn holding text that is no JSON, number or order
 *  Telefunc writes; an integer at a counter Redis increments at its largest; a counter cell whose count is no number. */
function corrupted(call: Recorded): Fault[] {
  const faults: Fault[] = []
  for (const [key, { type, content }] of call.before) {
    if (type === 'string') {
      faults.push({
        name: `${key} holds text Telefunc didn't write`,
        plant: async (admin) => void (await admin.set(key, 'x{')),
      })
      if (/^-?\d+$/.test(Buffer.from(content[0] ?? '', 'hex').toString())) {
        faults.push({
          name: `${key} holds the largest integer`,
          plant: async (admin) => void (await admin.set(key, '9223372036854775807')),
        })
      }
    }
    if (type === 'hash') {
      const fields = content.map((pair) => Buffer.from(pair.split('=')[0] ?? '', 'hex'))
      faults.push({
        name: `each field of ${key} holds text Telefunc didn't write`,
        plant: async (admin) => {
          for (const field of fields) await admin.hset(key, field, 'x{')
        },
      })
      faults.push({
        name: `each field of ${key} holds a counter whose count is no number`,
        plant: async (admin) => {
          for (const field of fields) await admin.hset(key, field, '{"value":"x","changedAt":0}')
        },
      })
    }
  }
  return faults
}

/** The call from its store with `fault` planted: it runs, or it is refused before any write and says it didn't run, in
 *  the words given; anything else is a failure, described. */
async function trial(
  store: Store,
  call: Recorded,
  fault: Fault,
): Promise<'ran' | { refused: string } | { failure: string }> {
  await restore(store.admin, call.before)
  await fault.plant(store.admin)
  const listener = new Redis({ host: store.admin.options.host })
  listener.on('error', () => {})
  let published = 0
  listener.on('pmessage', () => published++)
  await listener.psubscribe('*')
  const started = Date.now()
  const base = await snapshot(store.admin)
  const connection = trials.get(store)
  if (connection === undefined) throw new Error(`no trial connection to ${store.name}`)
  const failure = await call.replay(connection).then(
    () => undefined,
    (error: unknown) => error,
  )
  const after = await snapshot(store.admin)
  const diff = changed(base, after, Date.now() - started)
  listener.disconnect()
  if (failure === undefined) return 'ran'
  const told =
    failure instanceof BackendError ? `${failure.code}/${failure.outcome}: ${failure.message}` : String(failure)
  const notRun = failure instanceof BackendError && failure.outcome === 'not-run'
  if (notRun && diff.length === 0 && published === 0) return { refused: failure.message }
  return {
    failure: `${call.name}, ${fault.name}: told ${told}; changed ${diff.slice(0, 3).join(', ') || 'nothing'}, published ${published}`,
  }
}

describe.runIf(runs)('every write script, on a key or value it does not expect', () => {
  beforeAll(async () => {
    for (const store of STORES) stores.push(await start(store))
    const [first] = stores
    if (first === undefined) throw new Error('no store')
    await record(first)
    for (const store of stores) {
      // As installRedis() makes it: the connection adds the client's database to the prefix it's given.
      const connection = new RedisConnection({ redis: store.trialClient, prefix: DEFAULT_PREFIX })
      expect(connection.prefix).toBe(prefix)
      trials.set(store, connection)
    }
  }, 300_000)

  afterAll(() => {
    for (const store of stores) {
      store.admin.disconnect()
      store.trialClient.disconnect()
    }
    removeStores()
  })

  it('the scenario makes every write script', () => {
    const made = new Set(recorded.map((call) => call.name))
    const writing = Object.values(REDIS_COMMANDS).filter((command) => command.writes)
    expect(writing.filter((command) => !made.has(command.name)).map((command) => command.name)).toEqual([])
  })

  for (const { name } of STORES) {
    it(`${name}: each write script's call that Redis doesn't refuse, by the commands and the TYPE calls it makes`, async () => {
      const store = stores.find((candidate) => candidate.name === name)
      const connection = store && trials.get(store)
      if (store === undefined || connection === undefined) throw new Error(`${name} didn't start`)
      // These rows come first, a few seconds after the recording: a close's commit runs only under its lease, which
      // lapses 15 s after it was taken.
      const made: Record<string, string[]> = {}
      for (const call of recorded) {
        await restore(store.admin, call.before)
        await store.admin.config('RESETSTAT')
        await call.replay(connection)
        const run = await commandsRun(store.admin)
        const commands = [...run.values()].reduce((sum, calls) => sum + calls, 0)
        ;(made[call.name] ??= []).push(`${commands} commands, ${run.get('type') ?? 0} TYPE`)
      }
      // A key a call's first command on it reads, or a write that comes first, needs no TYPE: Redis refuses there.
      const commit = '7 commands, 1 TYPE'
      expect(made).toEqual({
        tfBroadcastStart: ['3 commands, 0 TYPE'],
        tfPublish: ['4 commands, 0 TYPE', '5 commands, 0 TYPE', '4 commands, 0 TYPE'],
        tfRoomCellsExchange: ['19 commands, 2 TYPE', '16 commands, 2 TYPE'],
        tfRoomCommit: [commit, commit, commit, commit, '8 commands, 1 TYPE', '8 commands, 1 TYPE', commit, commit],
        tfRoomDirectoryDelete: ['6 commands, 2 TYPE', '6 commands, 2 TYPE'],
        tfRoomDirectoryPut: Array(4).fill('6 commands, 2 TYPE'),
        tfRoomDropIncarnation: ['2 commands, 0 TYPE', '2 commands, 0 TYPE'],
        tfRoomHeadExchange: [
          '20 commands, 3 TYPE',
          '4 commands, 0 TYPE',
          '3 commands, 0 TYPE',
          '4 commands, 0 TYPE',
          '20 commands, 3 TYPE',
          '3 commands, 0 TYPE',
          '4 commands, 0 TYPE',
        ],
        tfRoomOperation: [
          '40 commands, 2 TYPE',
          '37 commands, 2 TYPE',
          '28 commands, 2 TYPE',
          '45 commands, 5 TYPE',
          '48 commands, 5 TYPE',
        ],
      })
    }, 300_000)
  }

  for (const { name } of STORES) {
    it(`${name}: each key in turn holding another type`, async () => {
      const store = stores.find((candidate) => candidate.name === name)
      if (store === undefined) throw new Error(`${name} didn't start`)
      const failures: string[] = []
      let refused = 0
      for (const call of recorded) {
        for (const fault of anotherType(call)) {
          const verdict = await trial(store, call, fault)
          if (verdict === 'ran') continue
          if ('refused' in verdict) refused++
          else failures.push(verdict.failure)
        }
      }
      expect(failures).toEqual([])
      // A fault the store never saw refuses nothing: the trials reached it.
      expect(refused).toBeGreaterThan(0)
    }, 300_000)

    it(`${name}: a publish whose order holds another type is refused before its first write`, async () => {
      const store = stores.find((candidate) => candidate.name === name)
      if (store === undefined) throw new Error(`${name} didn't start`)
      const publish = recorded.find((call) => call.name === REDIS_COMMANDS.publish.name && call.before.size > 0)
      const [order] = publish?.keys ?? []
      if (publish === undefined || order === undefined) throw new Error('the scenario made no publish of a known key')
      const plants: Record<string, (admin: Redis) => Promise<unknown>> = {
        list: (admin) => admin.rpush(order, 'x'),
        hash: (admin) => admin.hset(order, 'x', '1'),
        set: (admin) => admin.sadd(order, 'x'),
        zset: (admin) => admin.zadd(order, 0, 'x'),
      }
      const verdicts: Record<string, unknown> = {}
      for (const [type, plant] of Object.entries(plants)) {
        const fault = {
          name: `${order} holds a ${type}`,
          plant: async (admin: Redis) => {
            await admin.del(order)
            await plant(admin)
          },
        }
        verdicts[type] = await trial(store, publish, fault)
      }
      const refused = {
        refused: expect.stringMatching(
          /^Redis at .+ holds, under Telefunc's prefix, a key of another type than Telefunc keeps there, so Telefunc's Redis backend changed nothing \(WRONGTYPE\): keep that prefix to Telefunc$/,
        ),
      }
      expect(verdicts).toEqual({ list: refused, hash: refused, set: refused, zset: refused })
    }, 300_000)

    it(`${name}: each value in turn not as Telefunc wrote it`, async () => {
      const store = stores.find((candidate) => candidate.name === name)
      if (store === undefined) throw new Error(`${name} didn't start`)
      const failures: string[] = []
      let refused = 0
      for (const call of recorded) {
        for (const fault of corrupted(call)) {
          const verdict = await trial(store, call, fault)
          if (verdict === 'ran') continue
          if ('refused' in verdict) refused++
          else failures.push(verdict.failure)
        }
      }
      expect(failures).toEqual([])
      // A fault the store never saw refuses nothing: the trials reached it.
      expect(refused).toBeGreaterThan(0)
    }, 300_000)
  }
})
