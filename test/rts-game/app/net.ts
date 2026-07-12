// Client networking for a live match: the fog-filtered binary state stream in, player commands
// out. Presence/chat/lobby wiring lives in the store; this module is only the match's hot lanes.

export { worldBuffer, startStream, stopStream, sendCommand, netStats }

import type { LocalParticipant, RemoteParticipant } from 'telefunc'
import { BLUE, RED, type Team } from '../shared/constants'
import { decodeFrame } from '../shared/protocol'
import type { Command, PlayerMeta } from '../shared/types'
import { WorldBuffer } from './engine/world-buffer'

const worldBuffer = new WorldBuffer()
const netStats = { frames: 0, keyframes: 0, bytes: 0, tick: 0 }

let me: LocalParticipant<PlayerMeta> | null = null
let serverId: string | null = null
let unsub: (() => void) | null = null

/** Begin consuming the match stream. `seat` is the room's **server seat** (`room.server`) — a
 *  first-class, non-presence handle to the server's simulation (findings 1+2 adopted in 51b4613),
 *  so no roster scan is needed. We subscribe to just our team's track; enemy positions outside our
 *  vision never cross the wire (source-selective binary — fog enforced upstream, not hidden
 *  client-side). */
function startStream(seat: RemoteParticipant<PlayerMeta>, self: LocalParticipant<PlayerMeta>, team: Team): void {
  stopStream()
  me = self
  serverId = seat.id
  worldBuffer.reset()
  netStats.frames = 0
  netStats.keyframes = 0
  netStats.bytes = 0

  const track = team === RED ? 'state:red' : team === BLUE ? 'state:blue' : 'state:full'
  unsub = seat.subscribeBinary(
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
  serverId = null
}

/** Issue a command to the server seat.
 *
 * The client→server lane is `me.send(room.server.id, cmd)` into the seat's `listen()` — a private,
 * ordered lane to the authority (findings 1+2 adopted). `send()` now resolves with a delivery
 * receipt `{ seq, timestamp }` (finding 2b adopted), so a command *is* acked; we still fire-and-
 * forget here because orders are absolute (goto/attack this id) and thus idempotently re-issuable —
 * a dropped one just gets re-sent by the next order. The receipt is there if we ever want
 * per-order confirmation or lag estimation.
 */
async function sendCommand(cmd: Command): Promise<void> {
  if (!me || !serverId) return
  try {
    await me.send(serverId, cmd)
  } catch {
    // A command lost to a teardown/rejection is non-fatal: orders are absolute and re-issuable.
  }
}
