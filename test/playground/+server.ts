import { Hono } from 'hono'
import vike from '@vikejs/hono'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/node'

config.shield = true

const tf = new Telefunc()
const app = new Hono()
vike(app)

app.all('/_telefunc', async (c) => {
  const response = await tf.serve({ request: c.req.raw })
  return response ?? c.text('Not found', 404)
})

export default {
  fetch: app.fetch,
  prod: {
    port: Number(process.env.PORT) || 3000,
  },
}
