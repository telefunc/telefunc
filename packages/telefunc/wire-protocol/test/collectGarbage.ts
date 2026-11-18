export { collectGarbage }

import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'

/** Collects what nothing reaches, without the `--expose-gc` flag on the test run. */
async function collectGarbage(): Promise<void> {
  setFlagsFromString('--expose-gc')
  const gc: unknown = runInNewContext('gc')
  if (typeof gc !== 'function') throw new Error('V8 exposes no gc()')
  for (let turn = 0; turn < 4; turn++) {
    await new Promise((resolve) => setImmediate(resolve))
    gc()
  }
}
