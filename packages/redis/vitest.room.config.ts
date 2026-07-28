// Gated real-Redis realization lane. Core's backend-neutral contract is certified once in Room's compact
// spec; this config runs only Redis-specific behavior against a real standalone server.
//
//   TELEFUNC_TEST_REAL_REDIS=redis://127.0.0.1:6379 \
//     pnpm exec vitest run --config packages/redis/vitest.room.config.ts
//
// With the env unset the live cases are explicitly skipped — never fake green.

import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const repoRoot = fileURLToPath(new URL('../../', import.meta.url))

export default defineConfig({
  test: {
    root: repoRoot,
    include: ['packages/redis/src/redis.spec.ts'],
    fileParallelism: false,
    testTimeout: 20_000,
    hookTimeout: 20_000,
  },
})
