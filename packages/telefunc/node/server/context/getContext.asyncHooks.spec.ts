import '../async_hooks.js'
import { expect, test } from 'vitest'

import { assertTelefunction } from '../runTelefunc/assertTelefunction.js'
import { decorateTelefunction } from '../runTelefunc/decorateTelefunction.js'
import { serve } from '../telefunc.js'
import { getContext, provideTelefuncContext } from './getContext.js'

const telefuncFilePath = '/getContext.telefunc.ts'

/** The user `getContext()` gives the code calling it, or `'throws'`. */
function readUser(): string {
  try {
    return getContext<{ user: string }>().user
  } catch {
    return 'throws'
  }
}

async function onReadAroundAwait() {
  const before = readUser()
  await Promise.resolve()
  return { before, after: readUser() }
}

let userAtClose: string | undefined
async function onReadAtClose() {
  getContext().onClose(() => {
    userAtClose = readUser()
  })
}
assertTelefunction(onReadAtClose, 'onReadAtClose', telefuncFilePath)
decorateTelefunction(onReadAtClose, 'onReadAtClose', telefuncFilePath, '/')

test('provideTelefuncContext(context, fn) gives each of two callers started together its own context before and after an `await`', async () => {
  const returns = await Promise.all([
    provideTelefuncContext({ user: 'A' }, onReadAroundAwait),
    provideTelefuncContext({ user: 'B' }, onReadAroundAwait),
  ])
  expect(returns).toEqual([
    { before: 'A', after: 'A' },
    { before: 'B', after: 'B' },
  ])
})

test('the context provideTelefuncContext(context, fn) provides ends with `fn`', async () => {
  expect(await provideTelefuncContext({ user: 'A' }, onReadAroundAwait)).toEqual({ before: 'A', after: 'A' })
  expect(readUser()).toBe('throws')
})

test('an onClose() callback reads the context of its request, not the one provided around serve()', async () => {
  await provideTelefuncContext({ user: 'outer' }, () =>
    serve({
      request: new Request('http://localhost/_telefunc', {
        method: 'POST',
        headers: { 'x-telefunc-request': 'txt' },
        body: JSON.stringify({ file: telefuncFilePath, name: 'onReadAtClose', args: [] }),
      }),
      context: { user: 'request' },
    }),
  )
  expect(userAtClose).toBe('request')
})

// Last: the context this form provides stays for the code that runs after it, the next test included.
test('provideTelefuncContext(context) provides the context to the code that runs after it', async () => {
  provideTelefuncContext<{ user: string }>({ user: 'A' })
  expect(await onReadAroundAwait()).toEqual({ before: 'A', after: 'A' })
})
