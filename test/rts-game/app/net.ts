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
let authorityId: string | null = null
let unsub: (() => void) | null = null

/** Begin consuming the match stream. `authority` is the server's simulation participant, found in
 *  the roster by `meta.authority` (see app/store.ts) — there is no first-class server/host handle,
 *  so the client discovers the authority as a member and both subscribes to and commands it.
 *  We subscribe to just our team's track; the enemy positions outside our vision never cross the
 *  wire (source-selective binary — the fog is enforced upstream, not hidden client-side). */
function startStream(authority: RemoteParticipant<PlayerMeta>, self: LocalParticipant<PlayerMeta>, team: Team): void {
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

/** Issue a command to the authority.
 *
 * ── FINDING (commands ride `send`, against its contract) ─────────────────────────────────────
 * This is the client→server lane, and the only fit is `me.send(authorityId, cmd)`. `send` is
 * documented as at-most-once peer signaling — "delivered to a live participant at most once, never
 * retried or stored" — yet an RTS needs its orders to arrive reliably and in order. We lean on the
 * live WebSocket underneath being ordered + reliable (which it is), but the API makes no such
 * promise, and there is no ack, no sequence number, and no binary form for a compact order batch.
 * Orders are made resend-safe by being absolute (goto/attack this id), so a dropped one is
 * re-issuable — but that is the app hardening around a lane that isn't meant for this.
 */
async function sendCommand(cmd: Command): Promise<void> {
  if (!me || !authorityId) return
  try {
    await me.send(authorityId, cmd)
  } catch {
    // A command lost to a teardown/rejection is non-fatal: orders are absolute and re-issuable.
  }
}
