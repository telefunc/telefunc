export { pageRoom }

import type { ChannelPublishInfo } from 'telefunc/__internal'
import { ClientRoom } from '../client/room.js'
import type { ClientRoomEvent, ClientRoomHandleOptions } from '../client/ClientRoomHandle.js'
import type { PageRequest } from '../client/stubChannel.js'

/** A page's room of `members`, with a channel that answers joins and metadata writes, and takes injected events. */
function pageRoom(members: Record<string, number> = {}) {
  const closes: Array<(err: Error | null) => void> = []
  let onText: ((notice: ClientRoomEvent, info: ChannelPublishInfo) => void) | null = null
  let seq = 0
  let metaSeq = 0
  const end = {
    isClosed: false,
    isConnected: true,
    _sendRequest: async (req: PageRequest) => {
      if (req.__r === 'req-join') return { ok: true, value: { id: 'me', joinedAt: 0 } }
      if (req.__r === 'req-set-meta') return { ok: true, value: { seq: ++metaSeq, meta: req.meta } }
      if (req.__r === 'req-update-meta') return { ok: true, value: { seq: ++metaSeq, attrs: req.attrs } }
      throw new Error(`Unexpected request ${req.__r}`)
    },
    // It takes core's callback of any kind, and keeps the text one, which takes any notice, for `deliver()`.
    _subscribeLocal: (kind: string, callback: (notice: unknown, info: ChannelPublishInfo) => void) => {
      if (kind === 'text') onText = callback
      return () => {}
    },
    _sendReply: () => {
      throw new Error('Unexpected DM reply')
    },
    _publishUnreported: unexpectedPublish,
    _publishBinaryUnreported: unexpectedPublish,
    _confirmSoon: () => {},
    _setSubscribed: () => {},
    _setWantParts: () => {},
    onOpen: () => () => {},
    onClose: (callback: (err: Error | null) => void) => {
      closes.push(callback)
      return () => {}
    },
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
    close: async () => null,
  }
  async function unexpectedPublish() {
    throw new Error('Unexpected publish')
  }
  const room = new ClientRoom({
    end: end as unknown as ClientRoomHandleOptions['end'],
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
  const deliver = (notice: ClientRoomEvent) => onText?.(notice, { key: 'room', seq: ++seq, timestamp: 0 })
  return { room, endWire: () => closes.forEach((close) => close(null)), deliver }
}
