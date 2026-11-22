import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // **/*.test.ts => @brillout/test-e2e
    include: ['packages/**/*.spec.ts', 'test/playground-stream/*.spec.ts'],
    // The package as built, for the pages of packages/telefunc/serve's specs.
    globalSetup: ['packages/telefunc/serve/page.build.fixture.ts'],
  },
})
