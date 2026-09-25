import { expect, test } from 'vitest'
import '../../../../node/server/async_hooks.js'
import { ChannelMux, getChannelMux } from '../../mux.js'
import { ServerChannel } from '../../channel.js'
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
