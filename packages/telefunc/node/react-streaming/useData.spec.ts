import { AsyncLocalStorage } from 'node:async_hooks'
import { createElement } from 'react'
import { renderToStream } from 'react-streaming/server'
import { beforeAll, expect, test } from 'vitest'
import { installAsyncContext } from '../server/context/async.js'
import { getContext, provideTelefuncContext } from '../server/context/getContext.js'
import { TelefuncSSR } from './useTelefuncContext.js'
import { useData } from './useData.js'

// As an adapter does where the runtime has AsyncLocalStorage, as Cloudflare's does on a Worker with the flag for it.
beforeAll(() => installAsyncContext(AsyncLocalStorage))

const onProfile = Object.assign(async () => getContext<{ name: string }>().name, {
  _key: '/Profile.telefunc.ts:onProfile',
})
const Profile = () => createElement('p', null, useData(onProfile))

async function html(page: ReturnType<typeof createElement>): Promise<string> {
  const { readable } = await renderToStream(page, { webStream: true, disable: true })
  return await new Response(readable).text()
}

test('in async mode, useData() reads the context <TelefuncSSR context> gives', async () => {
  expect(
    await html(createElement(TelefuncSSR, { context: { name: 'Nora' }, children: createElement(Profile) })),
  ).toContain('<p>Nora</p>')
})

test('in async mode without <TelefuncSSR context>, useData() reads the context provideTelefuncContext() gives', async () => {
  const page = await provideTelefuncContext({ name: 'Ada' }, () => html(createElement(Profile)))
  expect(page).toContain('<p>Ada</p>')
})
