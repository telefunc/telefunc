import { afterEach, expect, test, vi } from 'vitest'
import { serializeTelefunctionArguments } from './serializeTelefunctionArguments.js'
import { ClientConnection } from '../../wire-protocol/client/connection.js'

afterEach(() => {
  vi.restoreAllMocks()
})

test("a callback or stream argument's channel gets the call's channel idleTimeout", () => {
  const getOrCreate = vi.spyOn(ClientConnection, 'getOrCreate').mockReturnValue({} as ClientConnection)
  serializeTelefunctionArguments({
    telefuncFilePath: '/upload.telefunc.ts',
    telefunctionName: 'onUpload',
    telefunctionArgs: [() => {}, new ReadableStream()],
    channel: { transports: ['sse'] },
    abortController: new AbortController(),
    extensionRequestTypes: [],
    channelIdleTimeout: 0,
    telefuncUrl: 'http://idle.test/_telefunc',
  })
  expect(getOrCreate.mock.calls.map((call) => call[2].idleTimeout)).toEqual([0, 0])
})

test('a callback argument gets its channel on a page served over plain http, which has no crypto.randomUUID()', () => {
  vi.spyOn(ClientConnection, 'getOrCreate').mockReturnValue({} as ClientConnection)
  vi.spyOn(crypto, 'randomUUID').mockImplementation(() => {
    throw new TypeError('crypto.randomUUID is not a function')
  })
  expect(() =>
    serializeTelefunctionArguments({
      telefuncFilePath: '/upload.telefunc.ts',
      telefunctionName: 'onUpload',
      telefunctionArgs: [() => {}],
      channel: { transports: ['sse'] },
      abortController: new AbortController(),
      extensionRequestTypes: [],
      telefuncUrl: 'http://192.168.1.2:3000/_telefunc',
    }),
  ).not.toThrow()
})
