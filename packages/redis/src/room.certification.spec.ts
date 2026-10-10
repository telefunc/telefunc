import { spawn } from 'node:child_process'
import { Cluster, Redis } from 'ioredis'
import { describe, expect, it, onTestFinished } from 'vitest'
import { installRedis } from './index.js'
import { RedisBackend } from './backend.js'
import { disposeBackend } from '../../telefunc/dist/wire-protocol/backend/install.js'
import { Room } from '../../telefunc/dist/wire-protocol/room/server/statics.js'

// Room on a real Redis: REDIS_URL names a standalone server, REDIS_CLUSTER_NODES a Cluster's nodes; each one set runs.
type Target = { name: string; env: Record<string, string>; connect: () => Redis | Cluster }
const targets: Target[] = []
const { REDIS_URL, REDIS_CLUSTER_NODES } = process.env
if (REDIS_URL) targets.push({ name: 'Redis', env: { REDIS_URL }, connect: () => new Redis(REDIS_URL) })
if (REDIS_CLUSTER_NODES) {
  const nodes = REDIS_CLUSTER_NODES.split(',').map((entry) => {
    const separator = entry.lastIndexOf(':')
    return { host: entry.slice(0, separator), port: Number(entry.slice(separator + 1)) }
  })
  targets.push({ name: 'Redis Cluster', env: { NODES: JSON.stringify(nodes) }, connect: () => new Cluster(nodes) })
}
if (targets.length === 0) throw new Error('Room certification on Redis requires REDIS_URL or REDIS_CLUSTER_NODES')

describe.each(targets)('Room on $name', ({ env, connect }) => {
  it('commits an exchange while the cells it expects are as read, whatever else changed, and never conflicts on a count', async () => {
    const backend = new RedisBackend({ redis: ownClient(connect()), prefix: uniquePrefix('cells') })
    const opened = await backend.compareExchangeHead(
      'cells',
      { form: 'absent' },
      { head: { currentInc: 'inc', state: 'open', config: bytes('config') } },
    )
    expect('head' in opened).toBe(true)
    const exchange = (cx: Parameters<RedisBackend['compareExchangeCells']>[2]) =>
      backend.compareExchangeCells('cells', 'inc', cx)
    const created = await exchange({
      expect: [{ key: 'a', version: null }],
      mutations: [{ key: 'a', bytes: bytes('a1') }],
      counts: [{ key: 'n', delta: 0, create: true }],
    })
    expect(created).toEqual({ committed: new Map([['n', { value: 0, changedAt: expect.any(Number) }]]) })
    const read = await backend.readCells('cells', 'inc', { keys: ['a'] })
    if ('staleInc' in read) throw new Error('the room is gone')
    const a = read.versions.get('a')!
    // Another writer: a cell this exchange didn't read, and the counter.
    await exchange({ expect: [{ key: 'b', version: null }], mutations: [{ key: 'b', bytes: bytes('b1') }] })
    expect(await exchange({ expect: [], mutations: [], counts: [{ key: 'n', delta: 1 }] })).toEqual({
      committed: new Map([['n', { value: 1, changedAt: expect.any(Number) }]]),
    })
    const counted = await exchange({
      expect: [{ key: 'a', version: a }],
      mutations: [{ key: 'a', bytes: bytes('a2') }],
      counts: [{ key: 'n', delta: 2 }],
    })
    expect(counted).toEqual({ committed: new Map([['n', { value: 3, changedAt: expect.any(Number) }]]) })
    const n = typeof counted === 'object' ? counted.committed.get('n') : undefined
    // `a` was written since that read.
    expect(
      await exchange({ expect: [{ key: 'a', version: a }], mutations: [], counts: [{ key: 'n', delta: 5 }] }),
    ).toBe('conflict')
    expect(
      await exchange({
        expect: [],
        mutations: [],
        counts: [
          { key: 'n', delta: 0 },
          { key: 'missing', delta: 1 },
        ],
      }),
    ).toEqual({ committed: new Map([['n', n]]) })
    const after = await backend.readCells('cells', 'inc', { keys: ['a', 'n', 'missing'] })
    if ('staleInc' in after) throw new Error('the room is gone')
    expect([...after.cells].map(([key, value]) => [key, Buffer.from(value).toString()])).toEqual([
      ['a', 'a2'],
      ['n', JSON.stringify(n)],
    ])
  })

  it('admits a thousand simultaneous joins from four servers, and counts each as a hold', async () => {
    const prefix = uniquePrefix('join-burst')
    const redis = ownClient(connect())
    installRedis(redis, { prefix })
    onTestFinished(() => disposeBackend())
    const room = await Room.create('join-burst')
    const startAt = Date.now() + 3_000
    const failures = await Promise.all(
      [0, 1, 2, 3].map(() => joinFrom({ ...env, PREFIX: prefix, ROOM: room.id, COUNT: '250', START_AT: `${startAt}` })),
    )
    expect(failures.flat()).toEqual([])
    expect(await Room.getParticipants(room.id)).toHaveLength(1_000)
    const backend = new RedisBackend({ redis, prefix })
    const inc = (await backend.readHead(room.id))!.currentInc!
    const read = await backend.readCells(room.id, inc, { keys: ['holds'] })
    if ('staleInc' in read) throw new Error('the room is gone')
    expect(JSON.parse(Buffer.from(read.cells.get('holds')!).toString())).toMatchObject({ value: 1_000 })
  })
})

/** A server that joins the room COUNT times at START_AT, and reports each join's failure. */
function joinFrom(env: Record<string, string>): Promise<string[]> {
  const server = spawn(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      `import { Cluster, Redis } from 'ioredis'
import { Room } from 'telefunc'
import { installRedis } from ${JSON.stringify(new URL('../dist/index.js', import.meta.url).href)}
const { REDIS_URL, NODES, PREFIX, ROOM, COUNT, START_AT } = process.env
installRedis(NODES ? new Cluster(JSON.parse(NODES)) : new Redis(REDIS_URL), { prefix: PREFIX })
const room = await Room.get(ROOM)
await new Promise((resolve) => setTimeout(resolve, Number(START_AT) - Date.now()))
const joins = Array.from({ length: Number(COUNT) }, (_, i) => room.join({ identity: 'user-' + (i % 50) }))
const outcomes = await Promise.allSettled(joins)
const failures = outcomes.flatMap((outcome) => (outcome.status === 'rejected' ? [String(outcome.reason)] : []))
process.stdout.write(JSON.stringify(failures))
process.exit(0)`,
    ],
    { cwd: new URL('..', import.meta.url), env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'inherit'] },
  )
  let output = ''
  server.stdout.on('data', (chunk) => (output += chunk))
  return new Promise((resolve, reject) =>
    server.on('exit', (code) => (code === 0 ? resolve(JSON.parse(output)) : reject(new Error(`exit ${code}`)))),
  )
}

function ownClient<Client extends Redis | Cluster>(client: Client): Client {
  onTestFinished(async () => void (await client.quit().catch(() => client.disconnect())))
  return client
}

const uniquePrefix = (label: string): string => `redis-room-ci:${process.pid}:${Date.now().toString(36)}:${label}:`

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value)
}
