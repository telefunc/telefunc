import { configDefaults, defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // **/*.test.ts => @brillout/test-e2e
    include: ['packages/**/*.spec.ts', 'test/playground-stream/*.spec.ts'],
    // They need Docker: `pnpm run test:redis-stores`.
    exclude: [...configDefaults.exclude, 'packages/redis/src/*.store.spec.ts'],
    // Specs that force real garbage collection need it.
    poolOptions: { forks: { execArgv: ['--expose-gc'] } },
  },
})
