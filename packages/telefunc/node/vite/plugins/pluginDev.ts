export { pluginDev }

import type { Plugin, ResolvedConfig } from 'vite'
import { getPackageNodeModulesDirectory } from '../../../utils/requireResolve.js'
import { addTelefuncMiddleware } from '../shared/addTelefuncMiddleware.js'
import { apply } from '../shared/apply.js'

function pluginDev(): Plugin[] {
  let isCloudflarePlugin = false
  return [
    {
      name: 'telefunc:pluginDev',
      apply: apply('dev'),
      config: {
        handler() {
          return {
            optimizeDeps: {
              include: [
                // Vite pre-bundler doesn't discover 'telefunc/client' because it doesn't transform `.telefunc.js` imports => Vite will discover the 'telefunc/client' dependency only after Telefunc transforms `.telefunc.js` imports into the thin HTTP client using 'telefunc/client'
                'telefunc/client',
              ],
            },
          }
        },
      },
      configResolved: {
        async handler(config) {
          isCloudflarePlugin = config.plugins.some((p) => p.name === 'vite-plugin-cloudflare')
          Object.values(config.environments).forEach((env) => fixOptimizeDeps(env.optimizeDeps))
          await determineFsAllowList(config)
        },
      },
    },
    {
      name: 'telefunc:pluginDev:serverMiddleware',
      apply: apply('dev', { skipMiddlewareMode: true }),
      // Ensure that SvelteKit's configureServer() has precedence, see https://github.com/telefunc/telefunc/pull/54
      enforce: 'post',
      configureServer: {
        handler(server) {
          return async () => {
            addTelefuncMiddleware(server.middlewares)
            if (server.httpServer && !isCloudflarePlugin) {
              // Dynamic import to avoid loading crossws at build time
              // Skip for @cloudflare/vite-plugin: it runs the worker in its own
              // environment with a custom WS handler — registering the Node.js
              // upgrade listener would steal WS upgrades from Cloudflare's handler.
              const { Telefunc } = await import('../../../serve/node.js')
              new Telefunc().installWebSocket(server.httpServer)
            }
          }
        },
      },
    },
  ]
}

// - Vike adds @brillout/json-serializer, @brillout/import and @brillout/vite-plugin-server-entry to optimizeDeps.exclude
// - We need to remove them from optimizeDeps.exclude to avoid:
//   ```
//   10:41:35 AM [vite] Internal server error: Failed to resolve import "@brillout/json-serializer/parse" from "node_modules/.vite/deps/chunk-HMXEIHOJ.js?v=9404be11". Does the file exist?
//   ```
//   ```
//   Error: Cannot find module '@brillout/vite-plugin-server-entry/runtime' imported from '/my-app/node_modules/.vite/deps_ssr/telefunc-CIJh5jn0.js'
//   ```
// - We can't add @brillout/json-serializer to optimizeDeps.include because Vite complains:
//   ```
//   Failed to resolve dependency: @brillout/json-serializer/parse, present in 'optimizeDeps.include'
//   Failed to resolve dependency: @brillout/json-serializer/stringify, present in 'optimizeDeps.include'
//   ```
function fixOptimizeDeps(optimizeDeps: { exclude?: string[] }) {
  const deps = ['@brillout/json-serializer', '@brillout/import', '@brillout/vite-plugin-server-entry']
  optimizeDeps.exclude = optimizeDeps.exclude?.filter((entry) => !deps.some((dep) => entry.startsWith(dep)))
}

async function determineFsAllowList(config: ResolvedConfig) {
  const fsAllow = config.server.fs.allow

  // Add node_modules/telefunc/
  const packageNodeModulesDirectory = getPackageNodeModulesDirectory()
  fsAllow.push(packageNodeModulesDirectory)
}
