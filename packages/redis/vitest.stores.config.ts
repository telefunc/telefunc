import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

// Every write script against each Redis and Valkey version Telefunc supports, each in Docker: `pnpm run test:redis-stores`.
export default defineConfig({
  test: {
    dir: fileURLToPath(new URL('src', import.meta.url)),
    include: ['*.store.spec.ts'],
  },
})
