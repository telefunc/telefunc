import { afterEach, expect, test, vi } from 'vitest'
import '../../../../node/server/async_hooks.js'
import { ChannelMux, getChannelMux } from '../../mux.js'
import { ServerChannel } from '../../channel.js'
import { handleSseChannelRequest } from '../../sse.js'
import { ClientChannel } from '../../../client/channel.js'
import { config as clientConfig } from '../../../../client/clientConfig.js'
import { CloudflareRoomSessionManager } from './room/subscription.js'
import { withCloudflareSession, type CloudflareSession } from './session.js'

const incarnation = (): CloudflareSession => ({
  room: new CloudflareRoomSessionManager('session'),
  broadcast: undefined as never,
  mux: new ChannelMux(),
})

test("a session Durable Object's channels end with it: a reset's next incarnation finds none of them", () => {
  const channel = new ServerChannel()
  const [before, after] = [incarnation(), incarnation()]
  withCloudflareSession(before, () => getChannelMux().registerChannel(channel))
  expect(withCloudflareSession(before, () => getChannelMux().hasChannels())).toBe(true)
  // A client reconnecting after the reset reaches the next incarnation, whose subscriptions are empty too.
  expect(withCloudflareSession(after, () => getChannelMux().hasChannels())).toBe(false)
  channel.abort()
})

afterEach(() => {
  delete clientConfig.fetch
})

/** A fetch that runs Telefunc's SSE handler inside `session`, as a session DO serves its client. */
function sseFetchIn(session: CloudflareSession): typeof fetch {
  return (async (url: string | URL | Request, init?: RequestInit) => {
    const response = await withCloudflareSession(session, () =>
      handleSseChannelRequest(new Request(url, init as RequestInit)),
    )
    if (!response) return new Response(null, { status: 404 })
    const headers = new Headers(response.headers)
    headers.set('content-type', response.contentType)
    return new Response(response.body as BodyInit, { status: response.statusCode, headers })
  }) as typeof fetch
}

test("a session Durable Object's SSE clients reach its own channels, whichever DO served SSE first", async () => {
  const [first, second] = [incarnation(), incarnation()]
  await withCloudflareSession(first, () => handleSseChannelRequest(new Request('http://sse.test/_telefunc')))
  const server = new ServerChannel()
  withCloudflareSession(second, () => getChannelMux().registerChannel(server))
  clientConfig.fetch = sseFetchIn(second)
  const opened = vi.fn()
  server.onOpen(opened)
  const client = new ClientChannel({
    channelId: server.id,
    transports: ['sse'],
    telefuncUrl: 'http://sse.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  await vi.waitFor(() => expect(opened).toHaveBeenCalled())
  client.abort()
  server.abort()
})
