export { PublishBody, publishBody }

import { hasProp } from '../../../utils/hasProp.js'
import { encode } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { admitPublish } from './channelMemory.js'

const PUBLISH_BODY_BRAND = Symbol.for('telefunc.PublishBody')

/** A publish's frame, which every subscriber on this server holds and sends behind a head of its own. */
class PublishBody {
  readonly [PUBLISH_BODY_BRAND] = true
  readonly frame: Uint8Array<ArrayBuffer>
  /** The stream it is ordered in with the other publishes of its source, such as a member's track; null for one that has
   *  none. */
  readonly stream: string | null
  /** Whether a newer lossy body of its `stream` may take its place. */
  readonly lossy: boolean
  /** The holders of this body, which the ledger counts once. */
  refs = 0

  constructor(frame: Uint8Array<ArrayBuffer>, stream: string | null, lossy: boolean) {
    this.frame = frame
    this.stream = stream
    this.lossy = lossy
  }

  static isPublishBody(value: unknown): value is PublishBody {
    return hasProp(value, PUBLISH_BODY_BRAND)
  }
}

/** The body of a publish at `info`, admitted before anyone holds it: one per delivery, whoever it goes to. */
function publishBody(
  payload: string | Uint8Array,
  info: WirePublishInfo,
  stream: string | null,
  lossy: boolean,
): PublishBody {
  const frame = typeof payload === 'string' ? encode.publish(payload, info) : encode.publishBinary(payload, info)
  const body = new PublishBody(frame, stream, lossy)
  admitPublish(body)
  return body
}
