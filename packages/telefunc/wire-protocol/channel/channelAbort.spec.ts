// An abort ends both ends of the channel whatever its reason: one JSON can't hold goes without it, and the error is
// thrown at the `abort()` call, or reported once where the callback that threw `Abort` ran. Over a real server and page.

import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { onBug } from '../../node/server/runTelefunc/onBug.js'
import { Abort } from '../../node/server/Abort.js'
import { isAbort } from '../../shared/Abort.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'
import { encodeErrorPayload } from '../values/server/response/StreamingResponseBody.js'
import { parse } from '@brillout/json-serializer/parse'

const FILE = '/channelAbort.telefunc.ts'
/** The channel the telefunction returned, whose listener throws `Abort(carried.reason)`. */
const carried = { channel: null as ServerChannel<unknown, unknown> | null, reason: undefined as unknown }
vi.mock('../../node/server/runTelefunc/loadTelefuncFiles.js', () => ({
  loadTelefuncFiles: async () => ({
    telefuncFilesLoaded: {
      [FILE]: {
        onChannel: async () => {
          const channel = new ServerChannel<unknown, unknown>()
          channel.listen(() => {
            throw Abort(carried.reason)
          })
          carried.channel = channel
          return channel
        },
        onStreamBesideChannel: async () => {
          const channel = new ServerChannel<unknown, unknown>()
          carried.channel = channel
          async function* stream() {
            yield 'first'
            throw Abort(carried.reason)
          }
          return { channel, stream: stream() }
        },
      },
    },
    telefuncFilesAll: [FILE],
  }),
}))

serverConfig.channel = { transports: ['ws'], pingInterval: 60_000 }
serverConfig.disableNamingConvention = true

let server: Server
let origin: string
const serverBugs: unknown[] = []

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  origin = `http://127.0.0.1:${address.port}`
  onBug((err) => serverBugs.push(err))
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

/** The page's end of `channel`, open. */
async function pageOf(channel: ServerChannel<unknown, unknown>) {
  const page = new ClientChannel<unknown, unknown>({
    channelId: channel.id,
    connection: { telefuncUrl: `${origin}/_telefunc`, transports: ['ws'], idleTimeout: 0 },
  })
  await Promise.all([channel.opened, page.opened])
  return page
}

/** A server channel and the page's end of it, both open. */
async function pair() {
  const channel = new ServerChannel<unknown, unknown>()
  getChannelMux().registerChannel(channel)
  return [channel, await pageOf(channel)] as const
}

/** An abort's value, or what else `err` is. */
const endOf = (err: unknown) => (isAbort(err) ? { abortValue: err.abortValue } : { notAnAbort: err })

/** What each end's `onClose()` callbacks got, by `endOf()`: `null` where they haven't run. */
function closings(ends: Record<string, ServerChannel<unknown, unknown> | ClientChannel<unknown, unknown>>) {
  const got: Record<string, ReturnType<typeof endOf> | null> = {}
  for (const [name, end] of Object.entries(ends)) {
    got[name] = null
    end.onClose((err) => {
      got[name] = endOf(err)
    })
  }
  return got
}

/** What the page reports and the server's `onBug()` gets, while `run` runs. */
async function reported(run: () => Promise<void>) {
  const page: unknown[] = []
  const consoleError = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    if (args[0] === '[telefunc:channel-error]') page.push(args[1])
  })
  serverBugs.length = 0
  try {
    await run()
  } finally {
    consoleError.mockRestore()
  }
  return { page: page.map(messageOf), server: serverBugs.map(messageOf) }
}
const messageOf = (err: unknown) => (err instanceof Error ? err.message : String(err))
/** Until both ends ran their `onClose()` callbacks. */
const settled = (got: Record<string, unknown>) => vi.waitFor(() => expect(Object.values(got)).not.toContain(null))

const REASONS = {
  'a reason JSON holds': { reason: { code: 429 }, refusal: null, arrives: { code: 429 } },
  'bytes in the reason': {
    reason: { frame: new Uint8Array([1, 2, 255]) },
    refusal:
      'An abort reason is serialized as JSON, which turns the `Uint8Array` at `frame` into a plain object. Abort with a plain value, such as a string or a code.',
    arrives: undefined,
  },
  'a function in the reason': {
    reason: { retry() {} },
    refusal: 'cannot serialize retry because it',
    arrives: undefined,
  },
}

for (const aborting of ['server', 'page'] as const) {
  describe(`abort(reason) called on the ${aborting}`, () => {
    for (const [name, { reason, refusal, arrives }] of Object.entries(REASONS)) {
      test(`with ${name}: both ends end${refusal ? ' without it, and the call throws once they did' : ' with it'}`, async () => {
        const [channel, page] = await pair()
        const [aborter, other] = aborting === 'server' ? [channel, page] : [page, channel]
        const got = closings({ channel, page })
        other.listen(() => new Promise(() => {}))
        const pending = aborter.send('waits', { ack: true })

        const report = await reported(async () => {
          if (refusal) expect(() => aborter.abort(reason)).toThrow(refusal)
          else aborter.abort(reason)
          await settled(got)
        })

        expect(got).toEqual({ channel: { abortValue: arrives }, page: { abortValue: arrives } })
        expect(aborter.isClosed).toBe(true)
        expect(other.isClosed).toBe(true)
        expect(endOf(await pending.catch((err: unknown) => err))).toEqual({ abortValue: arrives })
        expect(report).toEqual({ page: [], server: [] })
      })
    }

    test('a getter in the reason that aborts the channel: it ends once, with the reason that ended it', async () => {
      const [channel, page] = await pair()
      const aborter = aborting === 'server' ? channel : page
      const got = closings({ channel, page })
      aborter.abort({
        get first() {
          aborter.abort('from the getter')
          return 1
        },
      })
      await settled(got)
      expect(got).toEqual({ channel: { abortValue: 'from the getter' }, page: { abortValue: 'from the getter' } })
    })
  })

  describe(`Abort(reason) thrown by a listener on the ${aborting}, answering an acknowledged send`, () => {
    for (const [name, { reason, refusal, arrives }] of Object.entries(REASONS)) {
      test(`with ${name}: both ends end${refusal ? ' without it, the error is reported once where it ran,' : ' with it,'} and the send rejects as aborted`, async () => {
        const [channel, page] = await pair()
        const [aborter, sender] = aborting === 'server' ? [channel, page] : [page, channel]
        const got = closings({ channel, page })
        aborter.listen(() => {
          throw Abort(reason)
        })
        let rejection: unknown
        const report = await reported(async () => {
          rejection = await sender.send('ask', { ack: true }).catch((err: unknown) => err)
          await settled(got)
        })

        expect(endOf(rejection)).toEqual({ abortValue: arrives })
        expect(got).toEqual({ channel: { abortValue: arrives }, page: { abortValue: arrives } })
        expect(report).toEqual({
          page: refusal && aborting === 'page' ? [expect.stringContaining(refusal)] : [],
          server: refusal && aborting === 'server' ? [expect.stringContaining(refusal)] : [],
        })
      })
    }
  })
}

describe('Abort(reason) thrown by a listener on a channel a telefunction returned, which aborts its response', () => {
  for (const [name, { reason, refusal, arrives }] of Object.entries(REASONS)) {
    test(`with ${name}: both ends end${refusal ? ' without it, and the error is reported once,' : ' with it,'} and the page's send rejects as aborted`, async () => {
      carried.reason = reason
      const response = await fetch(`${origin}/_telefunc`, {
        method: 'POST',
        headers: { 'x-telefunc-request': 'txt' },
        body: JSON.stringify({ file: FILE, name: 'onChannel', args: [] }),
      })
      expect(response.status).toBe(200)
      await response.text()
      const channel = carried.channel
      if (!channel) throw new Error('the telefunction made no channel')
      const page = await pageOf(channel)
      const got = closings({ channel, page })

      let rejection: unknown
      const report = await reported(async () => {
        rejection = await page.send('ask', { ack: true }).catch((err: unknown) => err)
        await settled(got)
      })

      expect(endOf(rejection)).toEqual({ abortValue: arrives })
      expect(got).toEqual({ channel: { abortValue: arrives }, page: { abortValue: arrives } })
      expect(report).toEqual({ page: [], server: refusal ? [expect.stringContaining(refusal)] : [] })
    })
  }
})

describe('Abort(reason) thrown by a stream a telefunction returned', () => {
  for (const [name, { reason, refusal, arrives }] of Object.entries(REASONS)) {
    test(`with ${name}: its page gets ${refusal ? 'a bug, and the error is reported' : 'the reason'}`, async () => {
      const report = await reported(async () => {
        const payload = encodeErrorPayload(Abort(reason), { telefunctionName: 'onStream', telefuncFilePath: FILE })
        expect(parse(new TextDecoder().decode(payload))).toEqual(
          refusal ? { type: 'bug' } : { type: 'abort', abortValue: arrives },
        )
      })
      expect(report).toEqual({ page: [], server: refusal ? [expect.stringContaining(refusal)] : [] })
    })

    test(`with ${name}: a channel beside it in the response ends ${refusal ? 'without it, and the error is reported once' : 'with it'}`, async () => {
      carried.reason = reason
      let channel: ServerChannel<unknown, unknown> | null = null
      let got: ReturnType<typeof closings> = {}
      const report = await reported(async () => {
        const response = await fetch(`${origin}/_telefunc`, {
          method: 'POST',
          headers: { 'x-telefunc-request': 'txt' },
          body: JSON.stringify({ file: FILE, name: 'onStreamBesideChannel', args: [] }),
        })
        expect(response.status).toBe(200)
        channel = carried.channel
        if (!channel) throw new Error('the telefunction made no channel')
        got = closings({ channel })
        await response.arrayBuffer()
        await settled(got)
      })
      expect(got).toEqual({ channel: { abortValue: arrives } })
      expect(report).toEqual({ page: [], server: refusal ? [expect.stringContaining(refusal)] : [] })
    })
  }
})
