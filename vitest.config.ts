import { configDefaults, defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/**/*.spec.ts'],
    exclude: [...configDefaults.exclude, 'packages/redis/src/cluster.certification.spec.ts'],
    // The Room handle-ownership tests force real garbage collection.
    poolOptions: { forks: { execArgv: ['--expose-gc'] } },
  },
})
