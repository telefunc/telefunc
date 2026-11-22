import { expect, onTestFinished, test, vi } from 'vitest'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { onBug } from '../node/server/runTelefunc/onBug.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

// node.uploadBody.spec.ts's refusals, in production: Telefunc reads `NODE_ENV` once, so they have a file of their own.

vi.hoisted(() => vi.stubEnv('NODE_ENV', 'production'))
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}/requestBody.telefunc.js`]
config.disableNamingConvention = true

const telefunc = new Telefunc()
const bugs: unknown[] = []
onBug((err) => void bugs.push(err))

test('a binary call without a body is refused as malformed: answered 400, nothing printed, with no bug (Telefunc given a Fetch Request)', async () => {
  const printed: string[] = []
  const error = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    printed.push(String(args[0]))
  })
  onTestFinished(() => {
    error.mockRestore()
    vi.unstubAllEnvs()
  })
  const request = new Request(`http://127.0.0.1/_telefunc?wire=${WIRE_VERSION}`, {
    method: 'POST',
    headers: { 'x-telefunc-request': 'bin' },
  })
  const response = await telefunc.serve({ request })
  expect({ status: response?.status, printed, bugs }).toEqual({ status: 400, printed: [], bugs: [] })
})
