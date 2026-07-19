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
    // Bounded on purpose, because `forks` + an unbounded worker count oversubscribes this suite.
    //
    // Dozens of these spec files stand up a real PGlite — a WASM PostgreSQL, memory-hungry and slow to
    // initialise. Vitest's default is to scale forks with the core count, so on a 32-core machine ~31 of
    // them race to instantiate PGlite at once. Nothing deadlocks; everything just gets slower, until a
    // fixture that takes ~1.2s alone blows the 5s default timeout. The result is a suite that fails with
    // TIMEOUTS in files unrelated to whatever you changed, and fails in DIFFERENT files each run.
    //
    // Measured on this suite (32 cores, 73 spec files) rather than guessed:
    //   unbounded (~31)  RED — 1-2 timeouts per run, moving between files
    //   16 workers       green 4/4 runs, 28.6s
    //   12 workers       green,          28.0s
    //    8 workers       green 4/4 runs, 27.6s  ← chosen
    //    4 workers       green,          42.1s
    // 8 is both the fastest and the furthest from the cliff: capping costs NOTHING in wall clock (unbounded
    // was 27.8s when it passed at all), because past ~8 these processes are contending, not working.
    //
    // This is not a CI workaround — GitHub-hosted runners have 2-4 cores and would pick ~3 workers anyway,
    // so CI never saw the failure. It is here so that a full-suite run means the same thing on a developer's
    // many-core machine as it does in CI, without anyone having to know to pass `--maxWorkers` by hand.
    maxWorkers: 8,
  },
})
