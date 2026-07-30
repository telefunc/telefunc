import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const repoRoot = fileURLToPath(new URL('../../../../', import.meta.url))

export default defineConfig({
  test: {
    root: repoRoot,
    include: ['packages/redis/test/cluster-ci/certification.ts'],
    testTimeout: 180_000,
    hookTimeout: 60_000,
  },
})
