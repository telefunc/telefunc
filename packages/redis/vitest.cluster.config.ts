// W4-R only: the disposable three-master launcher sets both Redis environment variables, builds the
// public package, and invokes this config. It deliberately runs the same shared/common and Redis-native
// files as vitest.room.config.ts, plus the real Cluster certification schedules.

import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const repoRoot = fileURLToPath(new URL('../../', import.meta.url))

export default defineConfig({
  test: {
    root: repoRoot,
    include: ['packages/telefunc/wire-protocol/backend/conformance/*.spec.ts', 'packages/redis/src/room/*.spec.ts'],
    setupFiles: ['packages/redis/src/room/register.ts'],
    fileParallelism: false,
    testTimeout: 180_000,
    hookTimeout: 60_000,
  },
})
