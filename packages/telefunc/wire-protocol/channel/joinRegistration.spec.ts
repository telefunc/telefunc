// A page's join of a channel the server registers only later, on a WebSocket, over a real server: what the joined
// channel and an open neighbour see at each point the registration can come, decided by the exchange's milestones.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { RECONCILE_TIMEOUT_MS } from '../constants.js'
import { ServerChannel } from './server/ServerChannel.js'
import { getChannelMux } from './server/ChannelMux.js'
import { ClientChannel } from './client/ClientChannel.js'
import { AttachExchange } from './client/AttachExchange.js'
import { WireLifecycle } from './client/WireLifecycle.js'

/** Past the exchange's deadline, so a registration can come after it and still within the TTL. */
const CONNECT_TTL_MS = RECONCILE_TIMEOUT_MS + 3_000

serverConfig.channel = { transports: ['ws'], connectTtl: CONNECT_TTL_MS }

let server: Server
let telefuncUrl: string
beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

/** Resolves once `check` holds; a stuck row ends at the harness's timeout. */
async function until(check: () => boolean) {
  while (!check()) await new Promise((resolve) => setTimeout(resolve, 20))
}

test.each(['after the RECONCILED', 'past the exchange deadline, within the TTL', 'never, so the TTL expires'] as const)(
  'a join whose channel the server registers %s: an open neighbour sends throughout, in order, and the joined channel opens or ends at its TTL',
  async (when) => {
    const connection = { telefuncUrl, transports: ['ws' as const], connectionKey: crypto.randomUUID() }
    const lead = new ServerChannel<string, string>()
    getChannelMux().registerChannel(lead)
    const neighbour = new ClientChannel<string, string>({ channelId: lead.id, connection })
    const got: string[] = []
    lead.listen((message) => void got.push(message))
    await Promise.all([lead.opened, neighbour.opened])
    const answered = vi.spyOn(AttachExchange.prototype, 'applyReconciled')
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    const joiningId = crypto.randomUUID()
    const joining = new ClientChannel<string, string>({ channelId: joiningId, connection })
    const outcome = joining.opened.then(
      () => 'opened',
      (err: unknown) => (err instanceof Error ? err.message : String(err)),
    )
    const sent: string[] = []
    const send = (n: number) => {
      sent.push(String(n))
      void neighbour.send(String(n))
    }
    try {
      send(1)
      // The join's RECONCILED left the channel out: the server awaits it.
      await until(() => answered.mock.calls.length > 0)
      send(2)
      if (when === 'past the exchange deadline, within the TTL') {
        await new Promise((resolve) => setTimeout(resolve, RECONCILE_TIMEOUT_MS + 1_000))
        send(3)
      }
      if (when !== 'never, so the TTL expires') {
        const registered = new ServerChannel<string, string>({ id: joiningId })
        getChannelMux().registerChannel(registered)
        await registered.opened
      }
      if (when === 'never, so the TTL expires') expect(await outcome).not.toBe('opened')
      else expect(await outcome).toBe('opened')
      send(4)
      await until(() => got.length === sent.length)
      expect(got).toEqual(sent)
      expect(lost).not.toHaveBeenCalled()
    } finally {
      answered.mockRestore()
      lost.mockRestore()
      neighbour.abort()
      joining.abort()
    }
  },
  CONNECT_TTL_MS + 10_000,
)
