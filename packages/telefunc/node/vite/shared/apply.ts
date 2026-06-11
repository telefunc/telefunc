export { apply }

import type { UserConfig } from 'vite'
import { assert } from '../../../utils/assert.js'

function apply(when: 'dev' | 'preview', { skipMiddlewareMode }: { skipMiddlewareMode?: true } = {}) {
  return (config: UserConfig, { command, mode }: { command: string; mode: string }): boolean => {
    assert(command)
    assert(mode)

    if (when === 'dev') {
      if (skipMiddlewareMode === true && config?.server?.middlewareMode) {
        return false
      }
      return command === 'serve' && mode === 'development'
    }
    assert(skipMiddlewareMode === undefined)

    if (when === 'preview') {
      return command === 'serve' && mode === 'production'
    }

    assert(false)
  }
}
