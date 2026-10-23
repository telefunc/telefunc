import os from 'node:os'
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
    // WHY THIS EXPRESSION, and not a bare 8. Read from the installed vitest rather than inferred from the
    // option's name (`createForksPool`, dist/chunks/coverage.DL5VHqXY.js:2609-2612):
    //
    //   numCpus      = os.availableParallelism()
    //   threadsCount = watch ? max(floor(numCpus / 2), 1) : max(numCpus - 1, 1)
    //   maxThreads   = poolOptions.maxForks ?? config.maxWorkers ?? threadsCount
    //
    // Two things follow, and both were got wrong once each. `maxWorkers` is the pool size OUTRIGHT — the
    // first defined value wins and `threadsCount` is never consulted — so a bare `maxWorkers: 8` does not
    // mean "at most 8"; on a smaller machine it means "8 whether or not this machine has 8". And the default
    // it replaces is CORES MINUS ONE, not the core count, so a bound that merely avoids exceeding the raw
    // core count still overrides that default upward on a small machine: on 4 vCPUs the default is 3, and
    // `min(8, 4)` would be 4. The bound has to reproduce the default's headroom, not just its ceiling.
    //
    // Hence the SMALLER of the measured ceiling and what vitest would itself have picked:
    //   32 cores → min(8, 31) = 8   (this box; the measurement above stands)
    //    4 vCPUs → min(8,  3) = 3   (a GitHub runner — what it actually ran before this file bounded anything)
    //    2 vCPUs → min(8,  1) = 1
    //
    // The bare 8 shipped and turned the Ubuntu / Node 23 job red: 10 tests failing on the dot at 5000ms, all
    // in the PGlite-heavy cluster, 355s of test time inside 68s of wall clock (run 29688205697) — the same
    // oversubscription signature this bound exists to prevent, created on CI by the fix for it.
    //
    // KNOWN LIMIT, stated rather than papered over: this mirrors the NON-WATCH default, which is what `vitest
    // run` and CI use. Watch mode's default is lower (cores/2), and a config value cannot see watch mode, so
    // on a small machine in watch this bound can sit above what vitest would have chosen — 3 against 2 on 4
    // cores. That is an interactive path, not a gate.
    maxWorkers: Math.min(8, Math.max(os.availableParallelism() - 1, 1)),
  },
})
