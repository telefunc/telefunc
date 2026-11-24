// The page's side of a Room stub's channel: the channel a room's page and a lone participant share, each acked
// request with the reader of its answer, and the route of a DM. Every answer arrives as `unknown` and meets one reader
// here, which builds its value as it reads; a malformed one rejects its own request.
export { request, deliverDm }
export type { AckedChannel, RoomChannel, PageRequest }

import type { ChannelPublishAck, ClientBroadcast } from 'telefunc/__internal'
import type {
  RoomResult,
  ParticipantStubRequest,
  MemberMetaUpdate,
  RoomStubRequest,
  InboxMessage,
} from '../shared/protocol.js'
import { CLIENT_ROOM_BUG_MESSAGE, publishAck, roomResultValue } from '../shared/errors.js'
import { fieldsOf, memberMetaUpdateOf, numberOf, textOf } from '../shared/readers.js'
import type { ClientParticipant } from './ClientParticipant.js'

/** A DM to `participant`, delivered; one whose sender awaits its answer is answered on `end`. A reply that can't be
 *  sent is reported, and the sender gets an error instead. */
function deliverDm(end: Pick<ClientBroadcast, '_sendReply'>, participant: ClientParticipant, msg: InboxMessage): void {
  const { ackId } = msg
  if (ackId === undefined) return participant._deliverMessage(msg)
  const declare = (answer: RoomResult): RoomStubRequest => ({ __r: 'dm-reply', ackId, reply: answer })
  end._sendReply(
    participant._deliverMessageAck(msg).then(declare),
    declare({ ok: false, kind: 'error', code: 'INTERNAL', message: CLIENT_ROOM_BUG_MESSAGE }),
  )
}

/** A request a page sends on a room's or a participant's channel and awaits the answer of. A DM's reply is sent by
 *  `deliverDm()`, and nothing answers it. */
type PageRequest = Exclude<RoomStubRequest | ParticipantStubRequest, { __r: 'dm-reply' }>

/** A channel a page's requests go out on, each answered. */
type AckedChannel = {
  send(req: PageRequest, opts: { ack: true }): Promise<unknown>
}

/** A page's channel to a Room stub, a room's or a lone participant's: it relays `Notice` on its text lane and takes
 *  `Publish` as a text publish. The stub answers a publish with a `RoomResult`, which core leaves unknown, so the
 *  answer is read with `roomResultValue()`. */
type RoomChannel<Notice, Publish> = AckedChannel &
  Pick<
    ClientBroadcast<Notice, Publish>,
    | '_publishUnreported'
    | '_publishBinaryUnreported'
    | '_sendReply'
    | '_subscribeLocal'
    | '_setSubscribed'
    | '_setWantParts'
    | '_confirmSoon'
    | 'onOpen'
    | 'onClose'
    | 'onDisconnect'
    | 'onReconnect'
    | 'isConnected'
    | 'close'
  >

/** What the server answers each kind of request with: a join's member, a meta write's update, a publish's place. */
type RequestAnswers = {
  'req-join': { id: string; joinedAt: number }
  'req-set-meta': MemberMetaUpdate
  'req-update-meta': MemberMetaUpdate
  'req-publish': ChannelPublishAck
  'req-leave': unknown
  'req-dm': unknown
  'req-private': unknown
}

/** Reads each kind's answer as the channel hands it over, which no type carries across the wire. */
const ANSWERS: { [Kind in PageRequest['__r']]: (value: unknown) => RequestAnswers[Kind] } = {
  'req-join': joinedMember,
  'req-set-meta': memberMetaUpdate,
  'req-update-meta': memberMetaUpdate,
  'req-publish': publishAck,
  'req-leave': (value) => value,
  'req-dm': (value) => value,
  'req-private': (value) => value,
}

/** An acked request, as the answer its kind gets, or the error it failed with. */
async function request<Kind extends PageRequest['__r']>(
  end: AckedChannel,
  req: PageRequest & { __r: Kind },
): Promise<RequestAnswers[Kind]> {
  const answer = ANSWERS[req.__r]
  return answer(await roomResultValue(end.send(req, { ack: true })))
}

/** A join's answer: the member it made. */
function joinedMember(value: unknown): { id: string; joinedAt: number } {
  const fields = fieldsOf(value, "A join's answer")
  return {
    id: textOf(fields.id, "A joined member's id"),
    joinedAt: numberOf(fields.joinedAt, "A joined member's time"),
  }
}

/** A meta write's answer: the member's meta at the write's seq, whole or as the write merged it
 *  (`RoomWrites.writeMetaInOrder()`). */
function memberMetaUpdate(value: unknown): MemberMetaUpdate {
  return memberMetaUpdateOf(fieldsOf(value, "A meta write's answer"))
}
