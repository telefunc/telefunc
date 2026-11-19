export { pageRoom }

import type { ClientBroadcast } from 'telefunc/__internal'
import { ClientRoom } from '../client/room.js'
import type { RoomStubRequest } from '../shared/protocol.js'

/** A page's room of `members`, each with its join time, on a channel that answers a join as `me` and each meta write
 *  as the server does, at the next seq, and sends nothing else; the requests it was sent; and what ends the channel's
 *  wire. */
function pageRoom(members: Record<string, number> = {}) {
  const closes: Array<() => void> = []
  const requests: RoomStubRequest[] = []
  let metaSeq = 0
  const end = {
    isClosed: false,
    isConnected: true,
    send: async (req: RoomStubRequest) => {
      requests.push(req)
      if (req.__r === 'req-join') return { ok: true, value: { id: 'me', joinedAt: 0 } }
      if (req.__r === 'req-set-meta') return { ok: true, value: { seq: ++metaSeq, meta: req.meta } }
      if (req.__r === 'req-update-meta') return { ok: true, value: { seq: ++metaSeq, attrs: req.attrs } }
      throw new Error(`Unexpected request ${req.__r}`)
    },
    _subscribeLocal: () => () => {},
    _setSubscribed: () => {},
    _setWantParts: () => {},
    onClose: (callback: () => void) => {
      closes.push(callback)
      return () => {}
    },
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
  }
  const room = new ClientRoom({
    end: end as unknown as ClientBroadcast,
    snapshot: {
      channelId: 'channel',
      roomId: crypto.randomUUID(),
      meta: [],
      metaSeq: 0,
      closed: false,
      members: Object.entries(members).map(([id, joinedAt]) => ({
        id,
        meta: {},
        metaSeq: 0,
        identity: null,
        joinedAt,
      })),
    },
  })
  return { room, requests, endWire: () => closes.forEach((close) => close()) }
}
