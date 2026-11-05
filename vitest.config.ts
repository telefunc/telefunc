import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // **/*.test.ts => @brillout/test-e2e
    include: ['packages/**/*.spec.ts'],
    // Specs that force real garbage collection need it.
    poolOptions: { forks: { execArgv: ['--expose-gc'] } },
  },
})
