export { pageRoom }

import type { ChannelPublishInfo } from 'telefunc/__internal'
import { ClientRoom } from '../client/room.js'
import type { RoomStubRequest } from '../shared/protocol.js'

/** A page's room of `members`, each with its join time, on a channel that answers a join as `me` and each meta write
 *  as the server does, at the next seq, and sends nothing else; the requests it was sent; what ends the channel's
 *  wire; and what hands the room an event as the server sends one. `answers` puts a value of its own in the place of a
 *  join's or a meta write's answer, and answers a publish with one. */
function pageRoom(
  members: Record<string, number> = {},
  answers: { join?: unknown; metaWrite?: unknown; publish?: unknown } = {},
) {
  const closes: Array<() => void> = []
  let onText: ((envelope: unknown, info: ChannelPublishInfo) => void) | null = null
  let seq = 0
  const requests: RoomStubRequest[] = []
  let metaSeq = 0
  const end = {
    isClosed: false,
    isConnected: true,
    send: async (req: RoomStubRequest) => {
      requests.push(req)
      if (req.__r === 'req-join')
        return { ok: true, value: 'join' in answers ? answers.join : { id: 'me', joinedAt: 0 } }
      if ('metaWrite' in answers && (req.__r === 'req-set-meta' || req.__r === 'req-update-meta'))
        return { ok: true, value: answers.metaWrite }
      if (req.__r === 'req-set-meta') return { ok: true, value: { seq: ++metaSeq, meta: req.meta } }
      if (req.__r === 'req-update-meta') return { ok: true, value: { seq: ++metaSeq, attrs: req.attrs } }
      throw new Error(`Unexpected request ${req.__r}`)
    },
    _subscribeLocal: (kind: string, callback: (envelope: unknown, info: ChannelPublishInfo) => void) => {
      if (kind === 'text') onText = callback
      return () => {}
    },
    _sendReply: () => {
      throw new Error('Unexpected DM reply')
    },
    _publishUnreported: async () => {
      if (!('publish' in answers)) throw new Error('Unexpected publish')
      return { ok: true, value: answers.publish }
    },
    _publishBinaryUnreported: async () => {
      throw new Error('Unexpected publish')
    },
    _confirmSoon: () => {},
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
    end,
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
  const deliver = (envelope: object) => onText?.(envelope, { key: 'room', seq: ++seq, timestamp: 0 })
  return { room, requests, endWire: () => closes.forEach((close) => close()), deliver }
}
