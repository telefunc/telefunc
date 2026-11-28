import { once } from 'node:events'
import { createServer } from 'node:http'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { config as serverConfig } from 'telefunc'
import { Telefunc } from 'telefunc/node'
import { expect, test, vi } from 'vitest'
import { __remoteTelefunctionCall, config as clientConfig } from '../dist/client/index.js'

test('native Fetch can read a retained upload after cancelling an earlier file and receiving a plain response', async () => {
  const telefuncFile = '/node.retainedUpload.telefunc.mjs'
  const root = dirname(fileURLToPath(import.meta.url))
  serverConfig.root = root
  serverConfig.telefuncFiles = [root + telefuncFile]
  serverConfig.disableNamingConvention = true
  serverConfig.channel = { maxMemory: '2m', maxPendingBytes: 2 * 1024 * 1024 }

  const telefunc = new Telefunc()
  const server = createServer(async (req, res) => {
    if (!(await telefunc.serve({ req, res }))) res.writeHead(404).end()
  })
  server.listen(0, '127.0.0.1')
  try {
    await once(server, 'listening')
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('no port')
    const origin = `http://127.0.0.1:${address.port}`
    clientConfig.telefuncUrl = `${origin}/_telefunc`
    vi.stubGlobal('window', { scrollY: 0, fetch, location: { href: origin, origin } })

    const first = new File([new Uint8Array(1536 * 1024).fill(11)], 'first')
    const second = new File([new Uint8Array(768 * 1024).fill(22)], 'second')
    expect(await __remoteTelefunctionCall(telefuncFile, 'onUpload', [first, second])).toBe(true)
    expect(await __remoteTelefunctionCall(telefuncFile, 'onReadRetained', [])).toBe(true)
  } finally {
    vi.unstubAllGlobals()
    const closed = once(server, 'close')
    server.close()
    server.closeAllConnections()
    await closed
  }
})
