import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/@cloudflare-room/behavior.spec.ts'],
    testTimeout: 20_000,
    hookTimeout: 40_000,
  },
})
