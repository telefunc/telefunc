import { afterEach, expect, it } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { IndexedPeer } from './IndexedPeer.js'
import { publishBody } from './PublishBody.js'
import { disposeBackend } from '../../backend/install.js'

// A file of its own: `config.channel` is set before the first channel exists.
const MAX_PENDING_BYTES = 2 * 1024 * 1024
config.channel = { maxPendingBytes: MAX_PENDING_BYTES, maxMemory: '512m' }

afterEach(() => disposeBackend())

// A lossy publish a plain one of its stream followed is never replaced, so it counts toward the bound as a plain one does:
// where the bound can't be kept, the page fell behind and its channel ends.
it('holds no more than maxPendingBytes for a page that is away, lossy publishes a plain one followed included', () => {
  const end = awayEnd()
  let seq = 0
  for (let group = 0; group < 120; group++) {
    for (let i = 0; i < 5; i++) {
      end.deliver(publishBody(new Uint8Array(32 * 1024), { seq: ++seq, timestamp: seq }, 'cursor', true))
    }
    end.deliver(publishBody('"plain"', { seq: ++seq, timestamp: seq }, 'cursor', false))
  }

  expect(end.heldBytes).toBeLessThanOrEqual(MAX_PENDING_BYTES)
  expect(end._phase).toBe('ended')
})

/** An end whose page attached once and is away. */
function awayEnd(): ServerBroadcastEnd {
  const end = new ServerBroadcastEnd()
  end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
  end._registerChannel()
  const peer = new IndexedPeer({
    sender: {
      send() {},
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => -1,
      deliveredBefore: () => 0,
      terminate() {},
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      queue: {
        waitingPublishes: () => Number.POSITIVE_INFINITY,
        dropWaitingPublishes() {},
        dropWaitingFromPublish() {},
        drainRate: () => 0,
      },
    },
    index: 7,
    replay: end._replayBuffer!,
  })
  end._attachPeer(peer)
  end._onPeerDisconnect(peer, 60_000)
  return end
}
