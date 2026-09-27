import { afterEach, expect, test, vi } from 'vitest'
import { serializeTelefunctionArguments } from './serializeTelefunctionArguments.js'
import { ClientConnection } from '../../wire-protocol/client/connection.js'

afterEach(() => {
  vi.restoreAllMocks()
})

test("a callback argument's channel gets the call's channel idleTimeout", () => {
  const getOrCreate = vi.spyOn(ClientConnection, 'getOrCreate').mockReturnValue({} as ClientConnection)
  serializeTelefunctionArguments({
    telefuncFilePath: '/upload.telefunc.ts',
    telefunctionName: 'onUpload',
    telefunctionArgs: [() => {}],
    channel: { transports: ['sse'] },
    abortController: new AbortController(),
    extensionRequestTypes: [],
    channelIdleTimeout: 0,
    telefuncUrl: 'http://idle.test/_telefunc',
  })
  expect(getOrCreate).toHaveBeenCalledWith(
    expect.any(String),
    expect.anything(),
    expect.objectContaining({ idleTimeout: 0 }),
  )
})
