// Gated real-Redis conformance lane. Runs the SHARED scenario modules (memory + Redis, via the
// register.ts setupFile that appends the Redis backend to `installedBackends`) plus the Redis-specific
// stable-read / barrier-forced scenarios. Kept OUT of the default `vitest run` (root vitest.config.ts)
// so the memory-only suite there is unchanged; this one is invoked explicitly:
//
//   TELEFUNC_TEST_REAL_REDIS=redis://127.0.0.1:6379 \
//     pnpm exec vitest run --config packages/redis/vitest.room.config.ts
//
// With the env unset the setupFile registers nothing and the shared suite runs memory-only (an explicit
// skip report — never fake green).

import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const repoRoot = fileURLToPath(new URL('../../', import.meta.url))

export default defineConfig({
  test: {
    root: repoRoot,
    include: ['packages/telefunc/wire-protocol/backend/conformance/*.spec.ts', 'packages/redis/src/redis.spec.ts'],
    setupFiles: ['packages/redis/src/room/register.ts'],
    // One shared Redis: run files sequentially so connection churn and key namespaces stay predictable.
    fileParallelism: false,
    testTimeout: 20_000,
    hookTimeout: 20_000,
  },
})
