export { pluginReplaceConstantsNonRunnableDev }
export { DYNAMIC_IMPORT }

import { isDevCheck } from '../../../../utils/isDev.js'
import { isRunnableDevEnvironment } from '../../../../utils/isRunnableDevEnvironment.js'
import { getMagicString } from '../../../shared/getMagicString.js'
import type { Plugin } from 'vite'

// - We cannot use [`define`](https://vite.dev/config/shared-options.html#define) because we don't have access to `this.environment` and therefore we cannot call `isRunnableDevEnvironment(this.environment)` inside a configEnvironment() hook.
// - We cannot use [`filter.id`](https://rolldown.rs/plugins/hook-filters) because Vite's optimizeDeps bundles packages (e.g. `vike` or `telefunc`) into node_modules/.vite/deps_ssr/chunk-WBC5FHD7.js

const IS_NON_RUNNABLE_DEV = 'globalThis.__TELEFUNC__IS_NON_RUNNABLE_DEV'
const DYNAMIC_IMPORT = '__TELEFUNC__DYNAMIC_IMPORT'

declare global {
  var __TELEFUNC__IS_NON_RUNNABLE_DEV: undefined | true
  var __TELEFUNC__DYNAMIC_IMPORT: (module: `virtual:${string}`) => Promise<Record<string, unknown>>
}
function pluginReplaceConstantsNonRunnableDev(): Plugin[] {
  return [
    {
      name: 'telefunc:pluginReplaceConstantsNonRunnableDev:IS_NON_RUNNABLE_DEV',
      apply: (_, configEnv) => isDevCheck(configEnv),
      transform: {
        filter: {
          code: {
            include: IS_NON_RUNNABLE_DEV,
          },
        },
        handler(code, id) {
          if (isRunnableDevEnvironment(this.environment)) return
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          magicString.replaceAll(IS_NON_RUNNABLE_DEV, JSON.stringify(true))
          return getMagicStringResult()
        },
      },
    },
    {
      name: 'telefunc:pluginReplaceConstantsNonRunnableDev:DYNAMIC_IMPORT',
      apply: (_, configEnv) => isDevCheck(configEnv),
      transform: {
        filter: {
          code: {
            include: DYNAMIC_IMPORT,
          },
        },
        handler(code, id) {
          if (isRunnableDevEnvironment(this.environment)) return
          const { magicString, getMagicStringResult } = getMagicString(code, id)
          magicString.replaceAll(DYNAMIC_IMPORT, 'import')
          return getMagicStringResult()
        },
      },
    },
  ]
}
