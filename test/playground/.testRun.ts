export { testRun }

import { page, test, expect, expectLog, run, getServerUrl, autoRetry, fetchHtml } from '@brillout/test-e2e'
import { testCounter } from '../utils'
import { readFileSync } from 'node:fs'
import { posix } from 'node:path'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  run(cmd, {
    serverIsReadyMessage: (log) =>
      log.includes('Listening on') ||
      log.includes('Server running at') ||
      (log.includes('Local:') && log.includes('http://localhost:3000')),
  })

  const isDev = cmd === 'pnpm run dev'

  test('hello', async () => {
    {
      const html = await fetchHtml('/')
      expect(html).toContain('Loading...')
      expect(html).not.toContain('Eva')
    }
    {
      page.goto(`${getServerUrl()}/`)
      // `autoRetry` to ensure that async JavaScript has loaded & executed
      await autoRetry(async () => {
        expect(await page.textContent('body')).toContain('Welcome Eva')
      })
      expect(await page.textContent('body')).not.toContain('Loading')
    }
  })

  test('counter', async () => {
    await testCounter()
  })

  test("error responses don't accumulate Content-Type headers", async () => {
    for (let i = 0; i < 3; i++) {
      const resp = await fetch(`${getServerUrl()}/_telefunc`, {
        method: 'POST',
        headers: { 'x-telefunc-request': 'txt' },
        body: 'not-json',
      })
      expect(resp.status).toBe(400)
      expect(resp.headers.get('content-type')).toBe('text/plain')
    }
    if (isDev) {
      expectLog("Telefunc request body couldn't be parsed", { filter: (log) => log.logSource === 'stderr' })
    }
  })

  test('a form POST without the `x-telefunc-request` header is rejected', async () => {
    for (const action of ['/_telefunc', '/_telefunc?_telefunc=txt']) {
      await page.goto(`${getServerUrl()}/`)
      const responsePromise = page.waitForResponse(
        (resp) => resp.url() === `${getServerUrl()}${action}` && resp.request().isNavigationRequest(),
      )
      await page.evaluate((action) => {
        const form = document.createElement('form')
        form.method = 'POST'
        form.enctype = 'text/plain'
        form.action = action
        const input = document.createElement('input')
        input.type = 'hidden'
        input.name = '{"file":"/pages/index/Hello.telefunc.ts","name":"onLoad","args":[{"name":"Jon"}],"ignored":"'
        input.value = '"}'
        form.appendChild(input)
        document.body.appendChild(form)
        form.submit()
      }, action)
      const resp = await responsePromise
      expect(resp.request().headers()['x-telefunc-request']).toBe(undefined)
      expect(resp.status()).toBe(400)
      expect(await resp.text()).toBe('Malformed Telefunc Request')
    }
    expectLog('the server responded with a status of 400', { filter: (log) => log.logSource === 'Browser Error' })
    if (isDev) {
      expectLog('The `x-telefunc-request` header of the HTTP request is missing', {
        filter: (log) => log.logSource === 'stderr',
      })
    }
  })

  if (!isDev) {
    test('a plain call loads no script and a stream-only call loads only streams code', async () => {
      const { onDemand, streams } = onDemandCode('/pages/index')
      // The page makes the plain call during hydration.
      const requested: string[] = []
      const onRequest = (request: { url(): string }) => {
        const { pathname } = new URL(request.url())
        if (pathname.endsWith('.js') || pathname === '/_telefunc') requested.push(pathname)
      }
      page.on('request', onRequest)
      try {
        await page.goto(`${getServerUrl()}/`)
        await autoRetry(async () => {
          expect(await page.textContent('body')).toContain('Welcome Eva')
        })
        expect(requested.filter((script) => onDemand.has(script)).join(' ')).toBe('')
        expect(requested.slice(requested.indexOf('/_telefunc')).join(' ')).toBe('/_telefunc')

        await page.getByRole('button', { name: 'Read stream' }).click()
        await autoRetry(async () => {
          expect(await page.textContent('output')).toBe('hello stream')
        })
        expect([...new Set(requested.filter((script) => onDemand.has(script)))].sort()).deep.equal(streams)
      } finally {
        page.off('request', onRequest)
      }
    })

    test('shield() generation', async () => {
      {
        const resp = await makeTelefuncHttpRequest('Jon')
        expect(resp.status).toBe(200)
        const { ret } = await resp.json()
        expect(ret.message).toBe('Welcome Jon')
      }
      {
        const resp = await makeTelefuncHttpRequest(1337)
        expect(resp.status).toBe(422)
        expect(await resp.text()).toBe('Shield Validation Error')
        // [14:10:31.724][/.test-preview.test.ts][pnpm run preview][stderr] Shield Validation Error: the arguments passed to the telefunction onLoad() (/pages/index/Hello.telefunc.ts) have the wrong type. Arguments: `[{"name":1337}]`. Wrong type: [root] > [tuple: element 0] > [object: value of key `name`] is `number` but should be `string`.
        expectLog('Shield Validation Error', {
          filter: (log) =>
            log.logSource === 'stderr' && log.logText.includes('onLoad()') && log.logText.includes('Hello.telefunc.ts'),
        })
      }
    })
  }
}

async function makeTelefuncHttpRequest(name: string | number) {
  const resp = await fetch(`${getServerUrl()}/_telefunc`, {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({
      file: '/pages/index/Hello.telefunc.ts',
      name: 'onLoad',
      args: [{ name }],
    }),
  })
  return resp
}

type Chunk = { file: string; imports?: string[]; dynamicImports?: string[] }

/** The scripts of channels, broadcasts and streams that Telefunc's client imports when a call needs them, by the page's
 *  client build (Vike's manifest): the page's own code imports none of them. */
function onDemandCode(pageId: string) {
  const manifest: Record<string, Chunk> = JSON.parse(
    readFileSync(new URL('./dist/nested/assets.json', import.meta.url), 'utf8'),
  )
  const chunk = (key: string) => {
    const found = manifest[key]
    if (!found) throw new Error(`The manifest has no chunk ${key}`)
    return found
  }
  const closure = (keys: string[], into = new Set<string>()) => {
    for (const key of keys) {
      const { file, imports = [] } = chunk(key)
      if (into.has(`/${file}`)) continue
      into.add(`/${file}`)
      closure(imports, into)
    }
    return into
  }
  const entry = `virtual:vike:page-entry:client:${pageId}`
  const scripts = closure([entry])
  const features = chunk(entry).dynamicImports ?? []
  expect(
    features
      .map((key) => key.replace(/.*\/wire-protocol\/values\/client\//, ''))
      .sort()
      .join(' '),
  ).toBe('broadcasts.js channels.js streams.js')
  const streamEntry = features.find((key) => key.endsWith('/wire-protocol/values/client/streams.js'))
  if (!streamEntry) throw new Error('The manifest has no streams chunk')
  const streamScripts = closure([streamEntry])
  for (const feature of features) {
    const file = `/${chunk(feature).file}`
    expect(scripts.has(file)).toBe(false)
    if (feature !== streamEntry) expect(streamScripts.has(file)).toBe(false)
  }
  const modulesByChunk: Record<string, string[]> = JSON.parse(
    readFileSync(new URL('./dist/nested/client/chunk-modules.json', import.meta.url), 'utf8'),
  )
  const channelAndBroadcastModules = ['ClientChannel', 'ClientConnection', 'ClientBroadcast']
  expectExcludedModules(scripts, modulesByChunk, [
    'FrameDemuxer',
    'StreamReader',
    'SseStreamReader',
    ...channelAndBroadcastModules,
  ])
  expectExcludedModules(streamScripts, modulesByChunk, channelAndBroadcastModules)
  const onDemand = new Set([...closure(features)].filter((script) => !scripts.has(script)))
  const streams = [...streamScripts].filter((script) => onDemand.has(script)).sort()
  return { onDemand, streams }
}

function expectExcludedModules(scripts: Set<string>, modulesByChunk: Record<string, string[]>, moduleNames: string[]) {
  const included: string[] = []
  for (const script of scripts) {
    const modules = modulesByChunk[script]
    if (!modules) throw new Error(`The client build has no module list for ${script}`)
    included.push(...modules.filter((id) => moduleNames.includes(posix.basename(id, '.js'))))
  }
  expect(included.join('\n')).toBe('')
}
