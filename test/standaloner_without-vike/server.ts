import { createServer } from 'node:http'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/node'

config.disableNamingConvention = true

const telefunc = new Telefunc()

createServer(async (req, res) => {
  if (await telefunc.serve({ req, res })) return
  res.statusCode = 404
  res.end('Not found')
}).listen(3000, () => console.log('Server running at http://localhost:3000'))
