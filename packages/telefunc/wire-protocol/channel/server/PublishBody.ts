export { PublishBody, publishBody }

import { encode } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { admitPublish } from './channelMemory.js'

/** A publish's frame, which every subscriber on this server holds and sends behind a head of its own. */
class PublishBody {
  readonly frame: Uint8Array<ArrayBuffer>
  /** The stream within which a newer body takes this one's place while no wire has it; null if its page may not miss it. */
  readonly lossyStream: string | null
  /** The stream whose publishes reach a page on a channel of their own, its domain (see `TAG.DOMAIN`), so that no other
   *  stream's loss delays them; null for those that travel on their subscriber's channel. */
  readonly domain: string | null
  /** Of a lossy stream: it needs every publish of its stream since the newest one without `delta`, which begins its
   *  group, so it goes to a page right after those or not at all. */
  readonly delta: boolean
  /** The holders of this body, which the ledger counts once. */
  refs = 0

  constructor(frame: Uint8Array<ArrayBuffer>, lossyStream: string | null, domain: string | null, delta: boolean) {
    this.frame = frame
    this.lossyStream = lossyStream
    this.domain = domain
    this.delta = delta
  }
}

/** The body of a publish at `info`, admitted before anyone holds it: one per delivery, whoever it goes to. */
function publishBody(
  payload: string | Uint8Array,
  info: WirePublishInfo,
  lossyStream: string | null,
  domain: string | null = null,
  delta = false,
): PublishBody {
  const frame = typeof payload === 'string' ? encode.publish(payload, info) : encode.publishBinary(payload, info)
  const body = new PublishBody(frame, lossyStream, domain, delta)
  admitPublish(body)
  return body
}
