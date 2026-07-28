// W4-R only: the disposable three-master launcher sets both Redis environment variables, builds the
// public package, and invokes this config. It deliberately runs the same shared/common and Redis-native
// files as vitest.room.config.ts, plus the real Cluster certification schedules.

import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const repoRoot = fileURLToPath(new URL('../../', import.meta.url))
const sentinel = process.env.TELEFUNC_W4R_LAUNCHER_SENTINEL
const realRedis = process.env.TELEFUNC_TEST_REAL_REDIS
const rawNodes = process.env.TELEFUNC_TEST_REDIS_CLUSTER_NODES

if (sentinel === undefined || !/^telefunc-w4r-[a-z0-9-]+$/.test(sentinel)) {
  throw new Error('Redis Cluster certification must be invoked by the owned W4-R launcher')
}
if (realRedis === undefined || realRedis === '' || rawNodes === undefined || rawNodes === '') {
  throw new Error('Redis Cluster certification requires both Redis endpoint variables')
}
const parsedNodes = rawNodes.split(',').map((entry) => {
  const [host, portText] = entry.trim().split(':')
  const port = Number(portText)
  if (host === undefined || host === '' || !Number.isInteger(port) || port <= 0 || port > 65_535) {
    throw new Error(`Redis Cluster certification has invalid node '${entry}'`)
  }
  return { host, port }
})
if (parsedNodes.length < 3) throw new Error('Redis Cluster certification requires at least three parsed nodes')

export default defineConfig({
  test: {
    root: repoRoot,
    include: ['packages/telefunc/wire-protocol/backend/conformance/*.spec.ts', 'packages/redis/src/redis.spec.ts'],
    setupFiles: ['packages/redis/src/room/register.ts'],
    fileParallelism: false,
    testTimeout: 180_000,
    hookTimeout: 60_000,
  },
})
