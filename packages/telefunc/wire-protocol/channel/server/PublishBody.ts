export { PublishBody, publishBody }

import { encode } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { admitPublish } from './channelMemory.js'

/** A publish's frame, which every subscriber on this server holds and sends behind a head of its own. */
class PublishBody {
  readonly frame: Uint8Array<ArrayBuffer>
  /** Whether a page that falls behind may miss it (see `BroadcastEndOwner._onPublishesMissed`). */
  readonly lossy: boolean
  /** The holders of this body, which the ledger counts once. */
  refs = 0

  constructor(frame: Uint8Array<ArrayBuffer>, lossy: boolean) {
    this.frame = frame
    this.lossy = lossy
  }
}

/** The body of a publish at `info`, admitted before anyone holds it: one per delivery, whoever it goes to. */
function publishBody(payload: string | Uint8Array, info: WirePublishInfo, { lossy }: { lossy: boolean }): PublishBody {
  const frame = typeof payload === 'string' ? encode.publish(payload, info) : encode.publishBinary(payload, info)
  const body = new PublishBody(frame, lossy)
  admitPublish(body)
  return body
}
