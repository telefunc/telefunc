export { pageRoom, loneMember }

import type { ChannelPublishInfo } from 'telefunc/__internal'
import { ClientRoom } from '../client/room.js'
import { LoneMember } from '../client/LoneMember.js'
import type { ClientRoomEvent } from '../client/ClientRoomHandle.js'
import type { ParticipantStubNotice, RoomStubRequest } from '../shared/protocol.js'

/** What a stub's channel answers in place of the server's answer: a join's, a meta write's, a publish's. */
type Answers = { join?: unknown; metaWrite?: unknown; publish?: unknown }

/** A page's room of `members`, each with its join time, on `stubChannel()`; the requests it was sent; what ends the
 *  channel's wire; and what hands the room an event as the server sends one. */
function pageRoom(members: Record<string, number> = {}, answers: Answers = {}) {
  const { end, requests, endWire, deliver } = stubChannel<ClientRoomEvent>(answers)
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
  return { room, requests, endWire, deliver }
}

/** A participant a telefunction returned alone, on `stubChannel()` to its stub, and what hands it a notice as the stub
 *  sends one. */
function loneMember(answers: Answers = {}) {
  const { end, deliver } = stubChannel<ParticipantStubNotice>(answers)
  const metadata = {
    channelId: 'channel',
    roomId: crypto.randomUUID(),
    id: crypto.randomUUID(),
    meta: {},
    selfDelivery: false,
    identity: null,
  }
  return { me: new LoneMember({ end, metadata }).participant, deliver }
}

/** A channel to a Room stub, relaying `Notice`, that answers a join as `me` and each meta write as the server does, at the next seq,
 *  and sends nothing else. `answers` puts a value of its own in the place of a join's or a meta write's answer, and
 *  answers a publish with one. */
function stubChannel<Notice>(answers: Answers) {
  const closes: Array<() => void> = []
  let onText: ((notice: Notice, info: ChannelPublishInfo) => void) | null = null
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
    // It takes core's callback of any kind, and keeps the text one, which takes any notice, for `deliver()`.
    _subscribeLocal: (kind: string, callback: (notice: unknown, info: ChannelPublishInfo) => void) => {
      if (kind === 'text') onText = callback
      return () => {}
    },
    _sendReply: () => {
      throw new Error('Unexpected DM reply')
    },
    _publishUnreported: () => publish(),
    _publishBinaryUnreported: () => publish(),
    _confirmSoon: () => {},
    _setSubscribed: () => {},
    _setWantParts: () => {},
    onOpen: () => () => {},
    onClose: (callback: () => void) => {
      closes.push(callback)
      return () => {}
    },
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
    close: async (): Promise<1> => 1,
  }
  async function publish() {
    if (!('publish' in answers)) throw new Error('Unexpected publish')
    return { ok: true, value: answers.publish }
  }
  const deliver = (notice: Notice) => onText?.(notice, { key: 'room', seq: ++seq, timestamp: 0 })
  return { end, requests, endWire: () => closes.forEach((close) => close()), deliver }
}
