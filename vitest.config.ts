import { configDefaults, defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // **/*.test.ts => @brillout/test-e2e
    include: ['packages/**/*.spec.ts'],
    // The Cloudflare Room conformance lane needs the workerd tooling (Miniflare) and runs under its own
    // config (vitest.cloudflare.config.ts). Keep it out of the default memory-lane run.
    exclude: [...configDefaults.exclude, '**/backend/conformance/cloudflare/**'],
  },
})
