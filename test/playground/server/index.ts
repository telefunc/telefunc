import { apply, serve } from '@photonjs/hono'
import { Hono } from 'hono'
import { config } from 'telefunc'
import { telefunc } from 'telefunc/node'
import { cleanupState, resetCleanupState } from '../cleanup-state'
config.channel.pingInterval = 1000
config.shield = true

const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')

const tf = telefunc()

function startServer() {
  const app = new Hono()

  app.get('/api/cleanup-state', (c) => c.json(cleanupState))
  app.post('/api/cleanup-state/reset', (c) => {
    resetCleanupState()
    return c.json({ ok: true })
  })

  app.post('/api/server-close-trigger', async (c) => {
    const channelId = c.req.query('channelId')
    if (!channelId) return c.json({ ok: false, reason: 'missing channelId' }, 400)
    const store: Map<string, any> | undefined = (globalThis as any)[SERVER_CLOSE_RECONNECT_STORE_KEY]
    const channel = store?.get(channelId)
    if (!channel || channel.isClosed) return c.json({ ok: false, reason: 'channel not found or closed' }, 404)
    cleanupState[`serverClose_${channelId}_ackResult`] = 'pending'
    const ackPromise: Promise<unknown> = channel.send('offline-close', { ack: true })
    const closePromise: Promise<0 | 1> = channel.close({ timeout: 10_000 })
    void ackPromise.then(
      (result) => {
        cleanupState[`serverClose_${channelId}_ackResult`] = String(result)
      },
      () => {
        cleanupState[`serverClose_${channelId}_ackResult`] = 'error'
      },
    )
    void closePromise.then(
      (result: 0 | 1) => {
        cleanupState[`serverClose_${channelId}_closeResult`] = String(result)
      },
      () => {
        cleanupState[`serverClose_${channelId}_closeResult`] = 'error'
      },
    )
    return c.json({ ok: true })
  })

  apply(app)

  app.all('/_telefunc', async (c) => {
    const response = await tf.serve({ request: c.req.raw })
    if (response) return response
    return c.text('Not found', 404)
  })

  return serve(app, {
    port: 3000,
    onCreate(server) {
      // @ts-expect-error srvx types not exposed
      tf.installWebSocket(server.node.server)
    },
  })
}

export default startServer()
