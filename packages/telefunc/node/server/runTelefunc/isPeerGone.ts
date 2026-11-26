export { isPeerGone }

import { hasProp } from '../../../utils/hasProp.js'
import { StreamTruncatedError } from '../../../wire-protocol/wire/BodyReader.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { StreamAbortedError } from '../../../utils/StreamAbortedError.js'

/** Whether `err` says the peer left: mid-upload, mid-channel, before the response, or let go of by the server for taking
 *  nothing of its event stream. Not a bug, nothing to log.
 *  Node's own errors (`ERR_STREAM_PREMATURE_CLOSE`, and `ERR_STREAM_UNABLE_TO_PIPE` for a streamed body) can only be
 *  duck-typed. */
function isPeerGone(err: unknown): boolean {
  return (
    err instanceof StreamTruncatedError ||
    err instanceof StreamAbortedError ||
    (err instanceof NetworkError && err.isChannel) ||
    (hasProp(err, 'code') && (err.code === 'ERR_STREAM_PREMATURE_CLOSE' || err.code === 'ERR_STREAM_UNABLE_TO_PIPE'))
  )
}
