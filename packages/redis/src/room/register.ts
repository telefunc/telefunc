// vitest setupFile for the gated real-Redis conformance lane (vitest.room.config.ts). It appends the
// Redis backend to the SAME `installedBackends` registry the shared scenario modules iterate — the
// harness comment's "Redis and Cloudflare fixtures append themselves here", realized without editing any
// W2a file. Both this module and the spec modules resolve the harness by its source path, so they share
// one registry instance and the shared scenarios run identically against memory AND Redis.
//
// GATED: when TELEFUNC_TEST_REAL_REDIS is unset the Redis backend is NOT registered and the suite runs
// memory-only (an explicit skip report, never fake green).

import { installedBackends } from '../../../telefunc/wire-protocol/backend/conformance/harness.js'
import { makeRedisHarness } from './fixture.js'

const url = process.env.TELEFUNC_TEST_REAL_REDIS

if (url !== undefined && url !== '') {
  if (!installedBackends.some((harness) => harness.name === 'redis')) {
    installedBackends.push(makeRedisHarness(url))
  }
} else {
  console.warn(
    '[room conformance] TELEFUNC_TEST_REAL_REDIS is not set — the Redis backend is NOT registered; the ' +
      'shared conformance suite runs memory-only. Point it at a throwaway Redis to run the gated live lane.',
  )
}
