import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { stringify } from '@brillout/json-serializer/stringify'
import { serve } from './telefunc.js'
import { getContext } from './context/getContext.js'
import { decorateTelefunction } from './runTelefunc/decorateTelefunction.js'

// The telefunction below registers itself, as a bundler-less app's do, whatever app a build last wired into this
// checkout's Vite server entry.
vi.mock('./runTelefunc/loadTelefuncFilesUsingVite.js', () => ({ loadTelefuncFilesUsingVite: async () => null }))

const FILE = '/spec/Slow.telefunc.ts'
let signalAbortedAfter: number | null = null
let started = 0

decorateTelefunction(
  async function onSlow(...args: unknown[]) {
    const ms = args[0] as number
    const { signal } = getContext()
    started = performance.now()
    await new Promise<void>((resolve) => {
      const onAbort = () => {
        signalAbortedAfter = performance.now() - started
        clearTimeout(timer)
        resolve()
      }
      const timer = setTimeout(() => {
        signal.removeEventListener('abort', onAbort)
        resolve()
      }, ms)
      signal.addEventListener('abort', onAbort)
    })
  },
  'onSlow',
  FILE,
  '/',
)

// Wired as the /serve page's Express example.
const server = http.createServer(async (req, res) => {
  const httpResponse = await serve({ url: req.url!, method: req.method!, readable: req, headers: req.headers })
  res.statusCode = httpResponse.statusCode
  for (const [name, value] of httpResponse.headers) res.setHeader(name, value)
  await httpResponse.pipe(res)
})
let url = ''
beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/_telefunc`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

const call = (ms: number, init?: RequestInit) =>
  fetch(url, { method: 'POST', body: stringify({ file: FILE, name: 'onSlow', args: [ms] }), ...init })

test('with serve({ readable: req }), context.signal aborts once the client disconnects during a telefunction (#477)', async () => {
  const controller = new AbortController()
  const response = call(4_000, { signal: controller.signal })
  response.catch(() => {})
  await new Promise((resolve) => setTimeout(resolve, 300))
  controller.abort()
  await new Promise((resolve) => setTimeout(resolve, 500))
  expect(signalAbortedAfter).not.toBe(null)
  expect(signalAbortedAfter!).toBeLessThan(1_000)
})

test("with serve({ readable: req }), calls over one keep-alive connection leave no listener on it, and it doesn't abort their signals", async () => {
  const warnings: Error[] = []
  const onWarning = (warning: Error) => void warnings.push(warning)
  process.on('warning', onWarning)
  const sockets = new Set<import('node:net').Socket>()
  server.on('connection', (socket) => sockets.add(socket))
  try {
    for (let n = 0; n < 20; n++) {
      signalAbortedAfter = null
      expect((await call(0)).status).toBe(200)
      // Its signal aborts as it completes, not before.
      expect(signalAbortedAfter).toBe(null)
    }
    expect(sockets.size).toBe(1)
    const [socket] = sockets
    expect(socket!.listenerCount('close')).toBeLessThan(5)
    expect(warnings.filter((warning) => warning.name === 'MaxListenersExceededWarning')).toEqual([])
  } finally {
    process.off('warning', onWarning)
  }
})
