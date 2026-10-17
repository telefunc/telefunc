import { expect, test } from 'vitest'
import { EventEmitter } from 'node:events'
import { Readable } from 'node:stream'
import { nodeReadableToWebRequest } from './nodeReadableToWebRequest.js'

test.each(['GET', 'HEAD'])('builds a %s request, without a body', async (method) => {
  const { request } = await nodeReadableToWebRequest(Readable.from([]), 'http://localhost/_telefunc', method, {})
  expect(request.method).toBe(method)
  expect(request.body).toBe(null)
})

test('a request it fails to build leaves no listener on its connection', async () => {
  const socket = new EventEmitter()
  const readable = Object.assign(Readable.from([]), { socket })
  await expect(nodeReadableToWebRequest(readable, 'http://[', 'POST', {})).rejects.toThrow()
  expect(socket.listenerCount('close')).toBe(0)
})
