export { assertHeadNextWellFormed }

import type { HeadNext } from './contract.js'
import { assert } from '../../../utils/assert.js'

/** The supervisor's check on every head write; drivers only compare-exchange. */
function assertHeadNextWellFormed(next: HeadNext): void {
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    assert(head.closeLease !== undefined, 'head CX: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    assert(
      Number.isFinite(durationMs) && durationMs > 0,
      `head CX: close lease durationMs ${durationMs} must be finite and positive`,
    )
  } else {
    assert(head.closeLease === undefined, `head CX: a '${head.state}' head must not carry a close lease`)
  }
  assert(
    ttlMs === undefined || head.state === 'closed',
    `head CX: ttlMs is only valid for a 'closed' tombstone, got '${head.state}'`,
  )
  assert(
    head.state !== 'closed' || head.currentInc === null,
    'head CX: a closed tombstone must clear currentInc to null',
  )
  assert(
    head.state === 'closed' || head.currentInc !== null,
    `head CX: a '${head.state}' head must name an incarnation`,
  )
}
