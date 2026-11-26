import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { createServer } from 'node:http'

/** What 0.3.1 stores under the process-wide keys whose names the head's files also have. */
const SHAPES_031 = () => ({
  'serverConfig.ts': { stream: {}, channel: {}, broadcast: {}, extensions: [] },
  'node/server/runTelefunc/parseHttpRequest.ts': { gcRegistry: { register() {} } },
  'client/remoteTelefunctionCall/serializeTelefunctionArguments.ts': { gcRegistry: { register() {} } },
  'serve/node.ts': { registeredServers: new WeakSet() },
})

// The head's module graph, transformed while the file is collected, outside any row's deadline: each row's set-up then
// evaluates it fresh, after `vi.resetModules()`, at the cost of a cached import (seconds on a slow runner).
await import('./telefunc.js')
await import('../../serve/node.js')

const FILE = '/twoVersions.telefunc.ts'
/** The process's `_telefunc` global, which both versions keep their state in: a fresh object each row. */
let shared: Record<string, unknown> = {}
let before: PropertyDescriptor | undefined

beforeEach(() => {
  before = Object.getOwnPropertyDescriptor(globalThis, '_telefunc')
  shared = {}
  Object.defineProperty(globalThis, '_telefunc', {
    value: shared,
    writable: true,
    configurable: true,
    enumerable: true,
  })
  vi.resetModules()
})
afterEach(() => {
  if (before === undefined) Reflect.deleteProperty(globalThis, '_telefunc')
  else Object.defineProperty(globalThis, '_telefunc', before)
})

/** The head's server, its modules loaded fresh, with one telefunction that takes a function, and its `telefunc/node`. */
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
  const { Telefunc } = await import('../../serve/node.js')
  config.disableNamingConvention = true
  // The channel of a function argument holds its request till it closes: with no page, after `connectTtl`.
  config.channel = { connectTtl: 50 }
  const bugs: unknown[] = []
  onBug((err) => bugs.push(err))
  return { serve, config, bugs, Telefunc }
}

/** 0.3.1's `getGlobalObject()`: it takes whatever its key already holds. */
function load031() {
  for (const [key, shape] of Object.entries(SHAPES_031())) shared[key] ||= shape
  return shared
}

type Head = Awaited<ReturnType<typeof loadHead>>

describe('two versions in one process: each keeps its own state', () => {
  describe('0.3.1 loaded first', () => {
    let head: Head
    beforeEach(async () => {
      load031()
      head = await loadHead()
    })

    test("the head's config takes an assignment, and a call with a function argument closes with no bug", async () => {
      const { serve, config, bugs } = head
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

    test("the head's `installWebSocket()` takes a server", () => {
      const { Telefunc } = head
      expect(() => new Telefunc().installWebSocket(createServer())).not.toThrow()
    })
  })

  describe('the head loaded first', () => {
    let head: Head
    beforeEach(async () => {
      head = await loadHead()
    })

    test("0.3.1 finds none of the head's objects under its keys", () => {
      const { config } = head
      config.telefuncUrl = '/rpc'
      const all = load031()

      for (const [key, shape] of Object.entries(SHAPES_031()))
        expect(Object.keys(all[key] ?? {})).toEqual(Object.keys(shape))
      expect(config.telefuncUrl).toBe('/rpc')
    })
  })
})
