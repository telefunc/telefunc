export default {
  ci: {
    jobs: getCiJobs(),
  },
  tolerateError,
  // The dockerized e2e suite (`.test-docker.*`) hits Caddy at https://localhost:8443 with a
  // cert from Caddy's auto-generated local CA — Chromium has no reason to trust that CA, so
  // we tell it to skip cert validation. Same flag is harmless for non-docker tests that
  // already speak HTTP or have a real cert chain.
  chromiumLaunchOptions: {
    args: ['--ignore-certificate-errors'],
  },
}

function getCiJobs() {
  const ubuntu = {
    os: 'ubuntu-latest',
    node_version: '23',
  }
  const win = {
    os: 'windows-latest',
    node_version: '23',
  }
  const setups = [ubuntu, win]
  const setupModern = [ubuntu]

  return [
    // Vitest
    {
      name: 'Vitest',
      command: 'pnpm run test:units',
      setups: [ubuntu],
    },

    // TypeScript
    {
      name: 'TypeScript',
      command: 'pnpm run test:types',
      setups: [ubuntu],
    },

    // @brillout/test-e2e
    {
      name: 'Vite',
      setups,
    },
    {
      name: 'React Native',
      setups,
    },
    {
      name: 'Cloudflare',
      setups: setupModern,
    },
    {
      name: 'Next.js',
      setups,
    },
    {
      name: 'SvelteKit',
      setups,
    },
    {
      name: 'https://telefunc.com',
      setups: setupModern,
    },
  ]
}

function tolerateError({ logSource, logText }) {
  return (
    // TO-DO/eventually: move everything to this array
    [
      // [22:41:29.864][\examples\next][npm run dev][stderr] Watchpack Error (initial scan): Error: EINVAL: invalid argument, lstat 'D:\DumpStack.log.tmp'
      'Watchpack Error (initial scan)',
      // Error: [DocPress][Warning] prop `text` is deprecated
      'prop `text` is deprecated',

      // [18:54:59.547][/docs/.test-preview.test.ts][pnpm run preview][stderr] warnings when minifying css:
      // Warning: G] Transforming this CSS nesting syntax is not supported in the configured target environment ("chrome87", "edge88", "es2020", "firefox78", "safari14") [unsupported-css-nesting]
      'CSS nesting syntax is not supported in the configured target environment',

      // [11:03:16.814][/docs/.test-dev.test.ts][pnpm run dev][stderr] Cannot optimize dependency: @brillout/docpress/renderer/onRenderClient, present in 'optimizeDeps.include'
      'Cannot optimize dependency: @brillout/docpress/renderer/onRenderClient',

      // [21:29:57.330][/docs/.test-dev.test.ts][pnpm run dev][stderr] Cannot optimize dependency: @brillout/docpress/Layout, present in 'optimizeDeps.include'
      'Cannot optimize dependency: @brillout/docpress/Layout',
    ].some((t) => logText.includes(t)) ||
    isRollupEmptyChunkWarning() ||
    isSveltekitTypesGenWarning() ||
    isCJSVikeWarning() ||
    isCJSViteWarning() ||
    isVikeDeprecatedDesignWarning() ||
    isNextJsEslintWarning()
  )

  function isRollupEmptyChunkWarning() {
    return logSource === 'stderr' && logText.includes('Generated an empty chunk: "hooks"')
  }

  function isSveltekitTypesGenWarning() {
    return logSource === 'stderr' && logText.includes('Cannot find base config file "./.svelte-kit/tsconfig.json"')
  }

  function isCJSVikeWarning() {
    return (
      logSource === 'stderr' &&
      logText.includes('We recommend setting ') &&
      logText.includes('/package.json#type to "module", see https://vike.dev/CJS')
    )
  }
  function isCJSViteWarning() {
    return (
      logSource === 'stderr' &&
      logText.includes(
        "The CJS build of Vite's Node API is deprecated. See https://vitejs.dev/guide/troubleshooting.html#vite-cjs-node-api-deprecated for more details.",
      )
    )
  }

  function isVikeDeprecatedDesignWarning() {
    return (
      logSource === 'stderr' &&
      (logText.includes(
        // Old warning
        'You are using the old deprecated design, update to the new V1 design, see https://vike.dev/migration/v1-design',
      ) ||
        logText.includes(
          // New warning
          "You are using Vike's deprecated design. Update to the new V1 design, see https://vike.dev/migration/v1-design for how to migrate.",
        ))
    )
  }

  function isNextJsEslintWarning() {
    return (
      logSource === 'stderr' &&
      logText.includes(
        "DeprecationWarning: 'originalKeywordKind' has been deprecated since v5.0.0 and will no longer be usable after v5.2.0. Use 'identifierToKeywordKind(identifier)' instead.",
      )
    )
  }
}
