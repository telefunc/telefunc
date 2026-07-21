// vitest setup file for the Cloudflare conformance lane. It runs before each conformance test module is
// evaluated, so swapping the backend registry here is what makes the shared scenario modules (which
// iterate `installedBackends` at import time) target workerd instead of memory. The memory lane is the
// default `vitest run`; this lane is `vitest run --config vitest.cloudflare.config.ts`.

import { afterAll } from 'vitest'
import { installedBackends } from '../harness.js'
import { cloudflareHarness, disposeSharedMiniflare } from './fixture.js'

// Run the identical suite against the Cloudflare backend ONLY (the memory reference has its own default
// run); the parity report compares the two matrices.
installedBackends.length = 0
installedBackends.push(cloudflareHarness)

afterAll(async () => {
  await disposeSharedMiniflare()
})
