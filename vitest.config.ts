import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // **/*.test.ts => @brillout/test-e2e
    include: ['packages/**/*.spec.ts'],
    // `--expose-gc` so `global.gc` is ALWAYS available: @telefunc/drizzle reclaims an abandoned live
    // handle's graph token + subscription ref through a FinalizationRegistry, and the control for that has
    // to be able to force a collection. Gating that control on `typeof global.gc` instead would let it skip
    // silently on any run that forgot the flag — a check that never runs reads exactly like one that passed.
    pool: 'forks',
    poolOptions: { forks: { execArgv: ['--expose-gc'] } },
  },
})
