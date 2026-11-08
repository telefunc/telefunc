export { PublishBody, publishBody }

import { encode } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { admitPublish } from './channelMemory.js'

/** A publish's frame, which every subscriber on this server holds and sends behind a head of its own. */
class PublishBody {
  readonly frame: Uint8Array<ArrayBuffer>
  /** The stream within which a newer body takes this one's place while no wire has it; null if its page may not miss it. */
  readonly lossyStream: string | null
  /** The holders of this body, which the ledger counts once. */
  refs = 0

  constructor(frame: Uint8Array<ArrayBuffer>, lossyStream: string | null) {
    this.frame = frame
    this.lossyStream = lossyStream
  }
}

/** The body of a publish at `info`, admitted before anyone holds it: one per delivery, whoever it goes to. */
function publishBody(payload: string | Uint8Array, info: WirePublishInfo, lossyStream: string | null): PublishBody {
  const frame = typeof payload === 'string' ? encode.publish(payload, info) : encode.publishBinary(payload, info)
  const body = new PublishBody(frame, lossyStream)
  admitPublish(body)
  return body
}
