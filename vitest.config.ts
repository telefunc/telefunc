import { configDefaults, defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/**/*.spec.ts'],
    exclude: [...configDefaults.exclude, 'packages/redis/src/cluster.certification.spec.ts'],
    // Specs that force real garbage collection need it.
    poolOptions: { forks: { execArgv: ['--expose-gc'] } },
  },
})
