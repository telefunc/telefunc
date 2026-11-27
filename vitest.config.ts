import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // **/*.test.ts => @brillout/test-e2e
    include: ['packages/**/*.spec.ts', 'test/playground-stream/*.spec.ts'],
  },
})
