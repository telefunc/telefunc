// Client networking for a live match: the fog-filtered binary state stream in, player commands
// out. Presence/chat/lobby wiring lives in the store; this module is only the match's hot lanes.

export { worldBuffer, startStream, stopStream, sendCommand, netStats }

import type { LocalParticipant } from 'telefunc'
import { BLUE, RED, type Team } from '../shared/constants'
import { decodeFrame } from '../shared/protocol'
import type { Command, MatchAuthority, PlayerMeta } from '../shared/types'
import { WorldBuffer } from './engine/world-buffer'

const worldBuffer = new WorldBuffer()
const netStats = { frames: 0, keyframes: 0, bytes: 0, tick: 0 }

let me: LocalParticipant<PlayerMeta> | null = null
let authorityId: string | null = null
let unsub: (() => void) | null = null

/** Begin consuming the match stream. `authority` is the room's **hidden authority** participant
 *  (`join({ hidden: true })`), handed to us by `onEnterMatch` (findings 1+2) — a first-class,
 *  non-presence handle to the server's simulation, so no roster scan is needed. We subscribe to just
 *  our team's track; enemy positions outside our vision never cross the wire (source-selective
 *  binary — fog enforced upstream, not hidden client-side). The authority retains its keyframes, so
 *  this subscribe is seeded with a baseline before any live frame (finding #5). */
function startStream(authority: MatchAuthority, self: LocalParticipant<PlayerMeta>, team: Team): void {
  stopStream()
  me = self
  authorityId = authority.id
  worldBuffer.reset()
  netStats.frames = 0
  netStats.keyframes = 0
  netStats.bytes = 0

  const track = team === RED ? 'state:red' : team === BLUE ? 'state:blue' : 'state:full'
  unsub = authority.subscribeBinary(
    (bytes) => {
      const frame = decodeFrame(bytes)
      worldBuffer.applyFrame(frame, performance.now())
      netStats.frames++
      netStats.bytes += bytes.byteLength
      netStats.tick = frame.tick
      if (frame.keyframe) netStats.keyframes++
    },
    { track },
  )
}

function stopStream(): void {
  unsub?.()
  unsub = null
  me = null
  authorityId = null
}

/** Issue a command to the match authority.
 *
 * The client→server lane is `me.send(authority.id, cmd)` into the authority's `listen()` — a
 * private, ordered lane to the hidden sim participant (findings 1+2 adopted). `send()` resolves with
 * a delivery receipt `{ seq, timestamp }` (finding 2b adopted), so a command *is* acked; we still
 * fire-and-forget here because orders are absolute (goto/attack this id) and thus idempotently
 * re-issuable — a dropped one just gets re-sent by the next order. The receipt is there if we ever
 * want per-order confirmation or lag estimation.
 */
async function sendCommand(cmd: Command): Promise<void> {
  if (!me || !authorityId) return
  try {
    await me.send(authorityId, cmd)
  } catch {
    // A command lost to a teardown/rejection is non-fatal: orders are absolute and re-issuable.
  }
}
