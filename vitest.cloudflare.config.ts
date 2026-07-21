import { defineConfig } from 'vitest/config'

// The Cloudflare local-workerd conformance lane: the SAME conformance spec modules as the memory
// reference, plus the CF-specific scenarios, run with `installedBackends` swapped to the Cloudflare
// backend (see the register.ts setup file). Kept out of the default `vitest run` because it needs the
// workerd tooling (Miniflare) and is heavier.
export default defineConfig({
  test: {
    include: ['packages/telefunc/wire-protocol/backend/conformance/**/*.spec.ts'],
    setupFiles: ['./packages/telefunc/wire-protocol/backend/conformance/cloudflare/register.ts'],
    // Miniflare boots a workerd instance; give hooks room and keep files serial to bound the number of
    // concurrent runtimes (and avoid port-orphan contention on Windows).
    testTimeout: 20_000,
    hookTimeout: 40_000,
    fileParallelism: false,
    pool: 'forks',
  },
})
