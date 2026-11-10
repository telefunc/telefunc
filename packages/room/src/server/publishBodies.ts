export { unorderedEventBody, orderedBody, trackFrameBody, MemberMessageBodies }

import { stringify } from '@brillout/json-serializer/stringify'
import { publishBody, type OrderingInfo, type PublishBody } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import type { BinaryFrame } from '../shared/binary.js'
import type {
  ParticipantStubNotice,
  RoomCtrlEnvelope,
  RoomDemandEvent,
  RoomDmEnvelope,
  RoomJoinedEvent,
  RoomRosterEvent,
} from '../shared/protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import { encodeLaneKey } from '../backend/laneKey.js'
assertIsNotBrowser()

/** An event an instance originates, outside any lane's order, which a page never misses. */
function unorderedEventBody(
  event: RoomCtrlEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent | ParticipantStubNotice,
): PublishBody {
  return publishBody(stringify(event), { seq: 0, timestamp: Date.now() }, null)
}

/** A message, an announcement or an event as its lane carried it, which a page never misses: its room's `info.seq` stays
 *  whole. */
function orderedBody(serialized: string, info: OrderingInfo): PublishBody {
  return publishBody(serialized, info, null)
}

/** A track's frame as its lane carried it, on its track's domain (see telefunc's `TAG.DOMAIN`), so that no other track's
 *  loss delays it. One published with `lossy: true` gives way to a newer one of its track that no wire has taken. */
function trackFrameBody(frame: BinaryFrame, framed: Uint8Array, info: OrderingInfo): PublishBody {
  const stream = encodeLaneKey({ kind: 'binary', member: frame.from, track: frame.track })
  return publishBody(framed, info, frame.lossy ? stream : null, stream)
}

/** A member's message as its lane carried it, stamped, and without the stamp, each made once for every client. */
class MemberMessageBodies {
  private readonly serialized: string
  private readonly info: OrderingInfo
  /** What `serialized` encodes. */
  private readonly envelope: RoomDataEnvelope | RoomDmEnvelope
  private _stamped: PublishBody | null = null
  private _unstamped: PublishBody | null = null

  constructor({
    serialized,
    info,
    envelope,
  }: {
    serialized: string
    info: OrderingInfo
    envelope: RoomDataEnvelope | RoomDmEnvelope
  }) {
    this.serialized = serialized
    this.info = info
    this.envelope = envelope
  }

  stamped(): PublishBody {
    return (this._stamped ??= orderedBody(this.serialized, this.info))
  }

  unstamped(): PublishBody {
    return (this._unstamped ??= orderedBody(stringify(withoutStamp(this.envelope)), this.info))
  }
}

/** `envelope` without its sender's stamp. */
function withoutStamp({
  fromMeta,
  fromMetaSeq,
  fromIdentity,
  fromDisconnected,
  ...unstamped
}: RoomDataEnvelope | RoomDmEnvelope) {
  return unstamped
}
