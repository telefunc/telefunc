import { Hono } from 'hono'
import vike from '@vikejs/hono'

const app = new Hono()
vike(app)

export default {
  fetch: app.fetch,
  port: 3000,
}
