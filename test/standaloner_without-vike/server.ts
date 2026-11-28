import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { Readable } from 'node:stream'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/node'

config.disableNamingConvention = true
if (process.env.TELEFUNC_MANUAL_FILES === '1') {
  config.root = resolve('../..', 'manual')
  config.telefuncFiles = [resolve(config.root, 'hello.telefunc.ts')]
  config.telefuncUrl = '/api/_telefunc'
}

const telefunc = new Telefunc()

const server = createServer(async (req, res) => {
  const request = new Request(new URL(req.url ?? '/', 'http://localhost:3000'), {
    method: req.method,
    headers: Array.from(new Headers(req.headers as Record<string, string>).entries()),
    ...(req.method === 'POST' ? { body: Readable.toWeb(req), duplex: 'half' } : {}),
  } as RequestInit)
  const response = await telefunc.serve({ request })
  if (response !== undefined) {
    res.writeHead(response.status, Object.fromEntries(response.headers))
    if (response.body !== null) Readable.fromWeb(response.body).pipe(res)
    else res.end()
    return
  }
  if (req.url === '/status') res.writeHead(200).end('app response')
  else res.writeHead(404).end('Not found')
})
telefunc.installWebSocket(server)
server.listen(3000, () => console.log('Server running at http://localhost:3000'))
