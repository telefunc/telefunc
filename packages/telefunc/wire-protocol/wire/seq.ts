export { seqNear, seqThrough }

import { SEQ_SPAN } from './header.js'

/** The seq with low bits `bits` nearest `expected` (RFC 9000, A.3): no wire or replay holds 2^31 frames. */
function seqNear(bits: number, expected: number): number {
  const seq = expected - (expected >>> 0) + bits
  if (seq <= expected - SEQ_SPAN / 2) return seq + SEQ_SPAN
  if (seq > expected + SEQ_SPAN / 2 && seq >= SEQ_SPAN) return seq - SEQ_SPAN
  return seq
}

/** The latest seq with low bits `bits` through `sent`: a peer never lacks 2^32 frames of what was sent. */
function seqThrough(bits: number, sent: number): number {
  const seq = sent - (sent >>> 0) + bits
  return seq > sent && seq >= SEQ_SPAN ? seq - SEQ_SPAN : seq
}
