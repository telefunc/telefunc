export { PublishBody, publishBody }
export type { ShedLevel }

import { encode } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { admitPublish } from './channelMemory.js'

/** What a page that falls behind what the server holds for it may miss of a publish: `never`, or, at `behind`, the
 *  oldest of its `stream`, of which a shed keeps the newest (see `ServerBroadcastEnd.shed`). */
type ShedLevel = { level: 'never' } | { level: 'behind'; stream: string }

/** A publish's frame, which every subscriber on this server holds and sends behind a head of its own. */
class PublishBody {
  readonly frame: Uint8Array<ArrayBuffer>
  readonly shed: ShedLevel
  /** The holders of this body, which the ledger counts once. */
  refs = 0

  constructor(frame: Uint8Array<ArrayBuffer>, shed: ShedLevel) {
    this.frame = frame
    this.shed = shed
  }
}

/** The body of a publish at `info`, admitted before anyone holds it: one per delivery, whoever it goes to. */
function publishBody(payload: string | Uint8Array, info: WirePublishInfo, shed: ShedLevel): PublishBody {
  const frame = typeof payload === 'string' ? encode.publish(payload, info) : encode.publishBinary(payload, info)
  const body = new PublishBody(frame, shed)
  admitPublish(body)
  return body
}
