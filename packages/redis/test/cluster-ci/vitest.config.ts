import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    root: fileURLToPath(new URL('../../../../', import.meta.url)),
    include: ['packages/redis/test/cluster-ci/certification.spec.ts'],
    testTimeout: 180_000,
    hookTimeout: 60_000,
  },
})
