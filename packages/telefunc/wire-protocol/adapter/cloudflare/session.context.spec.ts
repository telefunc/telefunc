import { AsyncLocalStorage } from 'node:async_hooks'
import { beforeAll, describe, expect, it } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { installAsyncContext } from '../../../node/server/context/async.js'
import { restoreContext } from '../../../node/server/context/context.js'
import { getContext, PROVIDED_CONTEXT } from '../../../node/server/context/getContext.js'
import { ChannelMux } from '../../channel/server/ChannelMux.js'
import { ServerChannel } from '../../channel/server/ServerChannel.js'
import { TAG } from '../../wire/tags.js'
import type { CloudflareBroadcastMember } from './broadcast.js'
import { withCloudflareSession } from './session.js'

beforeAll(() => installAsyncContext(AsyncLocalStorage))

describe("a Channel's listener in a Cloudflare session", () => {
  // Its messages come from its own page alone, so it keeps the context the request it runs in has: none here.
  it('reads the context of the scope it runs in', () => {
    const session = { broadcast: {} as CloudflareBroadcastMember, mux: new ChannelMux() }
    const channel = new ServerChannel<string, string>()
    const read: unknown[] = []
    channel.listen(() => {
      try {
        read.push(Object.keys(getContext()))
      } catch (error) {
        read.push(error instanceof Error ? error.message : error)
      }
    })
    const text = stringify('hello')
    withCloudflareSession(session, () =>
      channel._dispatchFrame({ tag: TAG.TEXT, index: 1, seq: 1, text, bytes: text.length }),
    )
    expect(read).toEqual([['onClose', 'signal']])
  })

  it("whose page asks for a reply reads its request's context", async () => {
    const session = { broadcast: {} as CloudflareBroadcastMember, mux: new ChannelMux() }
    const channel = new ServerChannel<string, string>()
    const read: unknown[] = []
    channel.listen(() => void read.push(getContext<{ user: string }>().user))
    const text = stringify('hello')
    withCloudflareSession(session, () =>
      restoreContext({ [PROVIDED_CONTEXT]: { user: 'alice' } }, () =>
        channel._dispatchFrame({ tag: TAG.TEXT_ACK_REQ, index: 1, seq: 1, text, bytes: text.length }),
      ),
    )
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(read).toEqual(['alice'])
  })
})
