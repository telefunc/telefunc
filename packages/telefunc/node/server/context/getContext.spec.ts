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

async function onReadTwiceAroundAwait() {
  const before = [readUser(), readUser()]
  await Promise.resolve()
  return { before, after: readUser() }
}

const usersAfterYield: Record<string, string> = {}
async function* onReadAfterYield() {
  const { user } = getContext<{ user: string }>()
  yield
  usersAfterYield[user] = readUser()
}

const usersAtClose: Record<string, string> = {}
async function onReadAtClose() {
  const { user, onClose } = getContext<{ user: string }>()
  onClose(() => {
    usersAtClose[user] = readUser()
  })
}

const usersAtCancel: Record<string, string> = {}
async function onReadAtCancel() {
  const { user } = getContext<{ user: string }>()
  return new ReadableStream<Uint8Array<ArrayBuffer>>({
    cancel() {
      usersAtCancel[user] = readUser()
    },
  })
}

const telefunctions: Record<string, unknown> = {
  onReadAroundAwait,
  onReadAfterYield,
  onReadAtClose,
  onReadAtCancel,
}
for (const [telefunctionName, telefunction] of Object.entries(telefunctions)) {
  assertTelefunction(telefunction, telefunctionName, telefuncFilePath)
  decorateTelefunction(telefunction, telefunctionName, telefuncFilePath, '/')
}

function call(telefunctionName: string, user: string) {
  return serve({
    request: new Request('http://localhost/_telefunc', {
      method: 'POST',
      headers: { 'x-telefunc-request': 'txt' },
      body: JSON.stringify({ file: telefuncFilePath, name: telefunctionName, args: [] }),
    }),
    context: { user },
  })
}

async function callForReturn(telefunctionName: string, user: string): Promise<unknown> {
  const httpResponse = await call(telefunctionName, user)
  return JSON.parse(httpResponse.body).ret
}

test('after an `await`, getContext() throws for each of two calls started together', async () => {
  const returns = await Promise.all([callForReturn('onReadAroundAwait', 'A'), callForReturn('onReadAroundAwait', 'B')])
  expect(returns).toEqual([
    { before: 'A', after: 'throws' },
    { before: 'B', after: 'throws' },
  ])
})

test('after a `yield`, getContext() gives each of two generators started together its own context', async () => {
  const httpResponses = await Promise.all([call('onReadAfterYield', 'A'), call('onReadAfterYield', 'B')])
  await Promise.all(httpResponses.map((httpResponse) => httpResponse.getBody()))
  expect(usersAfterYield).toEqual({ A: 'A', B: 'B' })
})

test('an onClose() callback reads the context of its own request, for each of two calls started together', async () => {
  await Promise.all([call('onReadAtClose', 'A'), call('onReadAtClose', 'B')])
  expect(usersAtClose).toEqual({ A: 'A', B: 'B' })
})

test('the `cancel()` of a returned stream reads the context of its own request, for each of two responses cancelled together', async () => {
  const httpResponses = await Promise.all([call('onReadAtCancel', 'A'), call('onReadAtCancel', 'B')])
  await Promise.all(httpResponses.map((httpResponse) => httpResponse.getReadableWebStream().cancel()))
  expect(usersAtCancel).toEqual({ A: 'A', B: 'B' })
})

test('provideTelefuncContext(context) without `fn` throws', () => {
  // @ts-expect-error
  expect(() => provideTelefuncContext({ user: 'A' })).toThrow()
})

test('provideTelefuncContext(context, fn) returns what `fn` returns', () => {
  expect(provideTelefuncContext({ user: 'A' }, readUser)).toBe('A')
  const promise = Promise.resolve(true)
  const returned: Promise<boolean> = provideTelefuncContext({ user: 'A' }, () => promise)
  expect(returned).toBe(promise)
})

test('provideTelefuncContext(context, fn) gives each of two callers started together its own context until the first `await`', async () => {
  const returns = await Promise.all([
    provideTelefuncContext({ user: 'A' }, onReadTwiceAroundAwait),
    provideTelefuncContext({ user: 'B' }, onReadTwiceAroundAwait),
  ])
  expect(returns).toEqual([
    { before: ['A', 'A'], after: 'throws' },
    { before: ['B', 'B'], after: 'throws' },
  ])
})

test('provideTelefuncContext(context, fn) gives the previous context back when `fn` returns or throws', () => {
  const failure = new Error()
  const users = provideTelefuncContext({ user: 'A' }, () => {
    const nested = provideTelefuncContext({ user: 'B' }, readUser)
    const afterReturn = readUser()
    let thrown: unknown
    try {
      provideTelefuncContext({ user: 'B' }, () => {
        throw failure
      })
    } catch (err) {
      thrown = err
    }
    return { nested, afterReturn, thrown, afterThrow: readUser() }
  })
  expect(users).toEqual({ nested: 'B', afterReturn: 'A', thrown: failure, afterThrow: 'A' })
  expect(readUser()).toBe('throws')
})
