import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

/** What 0.3.1 stores under the process-wide keys whose names the head's files also have. */
const SHAPES_031 = () => ({
  'serverConfig.ts': { stream: {}, channel: {}, broadcast: {}, extensions: [] },
  'node/server/runTelefunc/parseHttpRequest.ts': { gcRegistry: { register() {} } },
  'client/remoteTelefunctionCall/serializeTelefunctionArguments.ts': { gcRegistry: { register() {} } },
})

// The head's module graph, transformed while the file is collected, outside any row's deadline: each row then evaluates
// it fresh, after `vi.resetModules()`, at the cost of a cached import.
await import('./telefunc.js')

const FILE = '/twoVersions.telefunc.ts'
const globals = globalThis as { _telefunc?: Record<string, unknown> }
let before: Record<string, unknown> | undefined

beforeEach(() => {
  before = globals._telefunc
  globals._telefunc = {}
  vi.resetModules()
})
afterEach(() => {
  globals._telefunc = before
})

/** The head's server, its modules loaded fresh, with one telefunction that takes a function. */
async function loadHead() {
  vi.doMock('./runTelefunc/loadTelefuncFiles.js', () => ({
    loadTelefuncFiles: async () => ({
      telefuncFilesLoaded: { [FILE]: { onCallback: async (_callback: unknown) => 42 } },
      telefuncFilesAll: [FILE],
    }),
  }))
  const { serve } = await import('./telefunc.js')
  const { config } = await import('./serverConfig.js')
  const { onBug } = await import('./runTelefunc/onBug.js')
  config.disableNamingConvention = true
  // The channel of a function argument holds its request till it closes: with no page, after `connectTtl`.
  config.channel = { connectTtl: 50 }
  const bugs: unknown[] = []
  onBug((err) => bugs.push(err))
  return { serve, config, bugs }
}

/** 0.3.1's `getGlobalObject()`: it takes whatever its key already holds. */
function load031() {
  const all = (globals._telefunc ??= {})
  for (const [key, shape] of Object.entries(SHAPES_031())) all[key] ||= shape
  return all
}

describe('two versions in one process: each keeps its own state', () => {
  test("0.3.1 loaded first: the head's config takes an assignment, and a call with a function argument closes with no bug", async () => {
    load031()
    const { serve, config, bugs } = await loadHead()

    config.telefuncUrl = '/_telefunc'
    expect(config.telefuncUrl).toBe('/_telefunc')

    const channelId = crypto.randomUUID()
    const response = await serve({
      request: new Request('https://example.test/_telefunc', {
        method: 'POST',
        headers: { 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: FILE,
          name: 'onCallback',
          args: [`!TelefuncFunction:${JSON.stringify({ channelId })}`],
        }),
      }),
    })
    expect(response.statusCode).toBe(200)
    // The request closes once that channel did, and runs its `onClose()` callbacks, the registry's among them.
    await new Promise((resolve) => setTimeout(resolve, 200))
    expect(bugs).toEqual([])
  })

  test("the head loaded first: 0.3.1 finds none of the head's objects under its keys", async () => {
    const { config } = await loadHead()
    config.telefuncUrl = '/rpc'
    const all = load031()

    for (const [key, shape] of Object.entries(SHAPES_031()))
      expect(Object.keys(all[key] as object)).toEqual(Object.keys(shape))
    expect(config.telefuncUrl).toBe('/rpc')
  })
})
