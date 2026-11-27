import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { parse } from '@brillout/json-serializer/parse'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { onBug } from '../../node/server/runTelefunc/onBug.js'
import { ServerChannel, ATTACHABLE_NOW } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { ClientBroadcast } from './client/ClientBroadcast.js'
import { ServerBroadcastEnd } from './server/ServerBroadcastEnd.js'
import { getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['ws'], pingInterval: 60_000 }

let server: Server
let telefuncUrl: string
const serverBugs: unknown[] = []

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
  onBug((err) => serverBugs.push(err))
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

/** A server channel and the page's end of it, both open. */
async function pair() {
  const channel = new ServerChannel<unknown, unknown>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const page = new ClientChannel<unknown, unknown>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['ws'], idleTimeout: 0 },
  })
  await Promise.all([channel.opened, page.opened])
  return [channel, page] as const
}

const messageOf = (err: unknown) => (err instanceof Error ? err.message : String(err))
const ANSWERS = {
  bytes: { answer: () => new Uint8Array([1, 2, 255]), which: 'this `Uint8Array`' },
  'bytes inside an object': {
    answer: () => ({ frame: new Uint8Array([1, 2, 255]) }),
    which: 'the `Uint8Array` at `frame`',
  },
  'a Blob': { answer: () => new Blob(['x']), which: 'this `Blob`' },
}
/** What the end that answers reports, and what the sender's acknowledged send rejects with. */
const ENDS = {
  server: { senderGets: 'Internal Server Error — see server logs' },
  page: { senderGets: 'Internal client channel error — see client logs' },
}

for (const [answering, { senderGets }] of Object.entries(ENDS)) {
  for (const listener of ['listen', 'listenBinary'] as const) {
    describe(`a ${listener}() callback on the ${answering} that answers with`, () => {
      for (const [shape, { answer, which }] of Object.entries(ANSWERS)) {
        test(`${shape} fails as a callback that throws, with an error that says what to answer with`, async () => {
          const [channel, page] = await pair()
          const [answerer, sender] = answering === 'server' ? [channel, page] : [page, channel]
          const pageErrors: unknown[] = []
          const consoleError = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
            if (args[0] === '[telefunc:channel-error]') pageErrors.push(args[1])
          })
          serverBugs.length = 0
          try {
            if (listener === 'listen') answerer.listen(answer)
            else answerer.listenBinary(answer)
            const sent =
              listener === 'listen'
                ? sender.send('ask', { ack: true })
                : sender.sendBinary(new Uint8Array([7]), { ack: true })

            await expect(sent).rejects.toThrow(senderGets)
            const reported = answering === 'server' ? serverBugs : pageErrors
            expect(reported.map(messageOf)).toEqual([
              expect.stringContaining(
                `A listener's answer is serialized as JSON, which turns ${which} into a plain object. Answer with a value JSON holds, or send the bytes with \`sendBinary()\` and answer with a reference to them.`,
              ),
            ])
          } finally {
            consoleError.mockRestore()
            await channel.close({ timeout: 100 })
          }
        })
      }
    })
  }
}

// An add-on's own protocol, as Room's joins, meta writes and messages: what the page sends and what the server answers
// keep their bytes.
test("an add-on's request and publish on a page's broadcast, and their answers, keep the bytes in them", async () => {
  const published: unknown[] = []
  const end = new ServerBroadcastEnd()
  end.setOwner({
    _onPagePublish(serialized) {
      published.push(parse(serialized))
      return { thumbnail: new Uint8Array([11]) }
    },
    _onPagePublishBinary() {},
  })
  getChannelMux().registerChannel(end, ATTACHABLE_NOW)
  const page = new ClientBroadcast({
    channelId: end.id,
    key: 'room',
    connection: { telefuncUrl, transports: ['ws'], idleTimeout: 0 },
  })
  await Promise.all([end.opened, page.opened])
  const received: unknown[] = []
  end.listen((request) => {
    received.push(request)
    return { meta: { avatar: new Uint8Array([9, 10]) } }
  })
  try {
    const answer = await page._sendRequest({ meta: { avatar: new Uint8Array([7, 8]) } })
    expect(received).toEqual([{ meta: { avatar: new Uint8Array([7, 8]) } }])
    expect(answer).toEqual({ meta: { avatar: new Uint8Array([9, 10]) } })
    expect(await page._publishUnreported({ data: new Uint8Array([12]) })).toEqual({ thumbnail: new Uint8Array([11]) })
    expect(published).toEqual([{ data: new Uint8Array([12]) }])
  } finally {
    await end.close({ timeout: 100 })
  }
})
