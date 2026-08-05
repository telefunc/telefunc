import { configDefaults, defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/**/*.spec.ts'],
    exclude: [...configDefaults.exclude, 'packages/redis/src/room/cluster.certification.spec.ts'],
  },
})
