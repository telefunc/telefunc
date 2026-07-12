export { Match }

import { Room, type LocalParticipant } from 'telefunc'
import { NEUTRAL, TICK_HZ, TICK_MS, KEYFRAME_TICKS, type Team } from '../../shared/constants'
import { encodeFrame } from '../../shared/protocol'
import type { Command, MatchRoom, PlayerMeta } from '../../shared/types'
import { applyCommand } from './commands'
import { seedWorld } from './mapgen'
import { MatchSnapshots, TRACK } from './snapshot'
import { World } from './world'

/** A running match: it owns the authoritative `World`, the tick loop, and the room's delivery
 *  lanes. It takes the room's **server seat** (`join({ server: true })`, surfaced as `room.server`)
 *  to broadcast binary state and receive commands.
 *
 * ── ✔ ADOPTED (findings 1 + 2: the server seat) ───────────────────────────────────────────────
 * Round 1 modelled the server as a synthetic "authority" participant — the one workaround that
 * unlocked server-authored binary broadcast + a command inbox, at the cost of the seat leaking
 * into every roster/`count` (filtered by a `meta.authority` flag), an anti-spoof guard, a
 * `meta.authority` roster scan on the client, and a hand-rolled real-player count for teardown.
 * `51b4613` landed the **server seat**: a full participant (publish, `listen`, `send`, `onDemand`)
 * that is *excluded from presence* and reachable as `room.server`. Adopting it deleted all of the
 * above — the join is now `{ server: true }`, clients read `room.server` directly, and real
 * `onEmpty` drives teardown (server/matches.ts). `send()` now also returns a delivery receipt.
 */
class Match {
  readonly roomId: string
  private room: MatchRoom
  private world = new World()
  private snaps = new MatchSnapshots()
  private seat: LocalParticipant<PlayerMeta> | null = null
  private timer: ReturnType<typeof setInterval> | null = null
  private playerTeam = new Map<string, Team>() // identity → team, snapshotted server-side at start (trusted)
  private demand = new Map<string, number>() // track → live subscriber count (from onDemand)
  private ended = false
  private disposeTimer: ReturnType<typeof setTimeout> | null = null
  onDispose: (() => void) | null = null

  constructor(roomId: string, room: MatchRoom) {
    this.roomId = roomId
    this.room = room
  }

  get isRunning(): boolean {
    return this.timer !== null
  }

  /** The trusted team of an identity (snapshotted at start) — used to restore a reconnecting
   *  player's team so they resubscribe to the right fog track. */
  teamOf(identity: string): Team | undefined {
    return this.playerTeam.get(identity)
  }

  /** Take the room's server seat — at *room creation*, before any client joins, so every client
   *  sees it in its initial roster (`room.server`). The seat join is roster-only (no control-lane
   *  announce), so a client that joined earlier would never discover a seat that appears later;
   *  seating the server up front sidesteps that. Wires command intake + demand; the sim itself
   *  doesn't run until `begin()`. */
  async takeSeat(): Promise<void> {
    if (this.seat) return
    // A non-presence member (`{ server: true }` → `room.server`), excluded from roster/`count`/
    // `onEmpty`. `selfDelivery: false` — it must never receive its own frames back.
    this.seat = await this.room.join(
      { name: 'server', color: '#8892b0', team: NEUTRAL, ready: true },
      { server: true, selfDelivery: false },
    )

    // Command intake. `from.identity` is server-verified; team comes from the trusted snapshot,
    // never from `from.meta.team` (client-authored display state — see commands.ts). Commands
    // before `begin()` hit an unseeded world (empty `playerTeam`) and are harmless no-ops.
    this.seat.listen((data, from) => {
      if (!from || from.identity === null) return
      const team = this.playerTeam.get(from.identity)
      if (team === undefined) return
      const cmd = data as Command
      if (cmd.t === 'resign') {
        this.world.eliminate(team)
        return
      }
      applyCommand(this.world, team, cmd)
    })

    // Track demand: a track's subscriber count rising means a (re)connected client that needs a
    // keyframe to seed its baseline; the spectator `full` track is only computed while watched.
    this.seat.onDemand((track, count) => {
      const prev = this.demand.get(track ?? '') ?? 0
      if (track && count > prev) this.snaps.forceKeyframe(track as (typeof TRACK)[keyof typeof TRACK])
      this.demand.set(track ?? '', count)
    })
  }

  /** Launch: snapshot the lobby's team choices (trusted, server-side), seed the world, flip the
   *  room to `playing`, and start ticking. The seat is already in place from `takeSeat()`. */
  async begin(teamPlayers: Map<Team, string[]>): Promise<void> {
    if (this.isRunning || this.ended) return
    for (const [team, ids] of teamPlayers) for (const id of ids) this.playerTeam.set(id, team)
    seedWorld(this.world, teamPlayers)
    await this.setPhase('playing')
    this.timer = setInterval(() => this.tick(), TICK_MS)
  }

  private tick(): void {
    if (this.ended) return
    this.world.tick(1 / TICK_HZ)
    const clockSec = Math.floor(this.world.tickCount / TICK_HZ)
    const periodic = this.world.tickCount % KEYFRAME_TICKS === 0

    // Team tracks always broadcast; the spectator `full` track only while it has watchers.
    for (const track of [TRACK.red, TRACK.blue] as const) this.publish(track, periodic, clockSec)
    if ((this.demand.get(TRACK.full) ?? 0) > 0) this.publish(TRACK.full, periodic, clockSec)

    if (this.world.winner !== NEUTRAL) this.finish(this.world.winner)
  }

  private publish(track: (typeof TRACK)[keyof typeof TRACK], periodic: boolean, clockSec: number): void {
    if (!this.seat) return
    const frame = this.snaps.build(this.world, track, periodic, clockSec)
    // `receivers` on the ack is the exact per-publish subscriber count; we drive keyframes off
    // `onDemand` instead, so the ack is fire-and-forget (guarded against teardown races).
    //
    // ── finding #12 (no lossy/latest-only binary) — deferred to the datagram lane (#449) ─────────
    // The lane is reliable + ordered, so a slow client accumulates a state-frame backlog when only
    // the newest frame matters. An app/coalesce "latest-only" can't fix it: you can't evict the
    // transport send-buffer, so it's a false promise on a reliable lane — the honest fix is the
    // unreliable datagram lane scoped in #449. We stay at 10 Hz with small frames to live under it.
    this.seat.publishBinary(encodeFrame(frame), { track, keyFrame: frame.keyframe }).catch(() => {})
  }

  private async finish(winner: Team): Promise<void> {
    if (this.ended) return
    this.ended = true
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    // One last broadcast so every client's frame header carries the winner (deterministic result
    // screen — no reliance on the announce arriving).
    const clockSec = Math.floor(this.world.tickCount / TICK_HZ)
    for (const track of [TRACK.red, TRACK.blue, TRACK.full] as const) {
      if (track === TRACK.full && (this.demand.get(TRACK.full) ?? 0) === 0) continue
      this.publish(track, true, clockSec)
    }
    // `Room.setAttributes`/`announce` are statics (admin ops don't live on the instance), so they
    // take the room id, not the room object.
    await Room.setAttributes(this.roomId, { phase: 'ended', winner }).catch(() => {})
    await Room.announce(this.roomId, { k: 'match-end', winner }).catch(() => {})
    // Keep the room alive briefly so players read the result, then tear down.
    this.disposeTimer = setTimeout(() => this.dispose(), 25_000)
  }

  /** The room emptied of real players. If the match had launched, end it as a no-contest;
   *  if it emptied while still in the lobby, just tear down (seat + room). */
  abandon(): void {
    if (this.ended) return
    if (this.isRunning) this.finish(NEUTRAL)
    else this.dispose()
  }

  private async setPhase(phase: 'playing'): Promise<void> {
    await Room.setAttributes(this.roomId, { phase }).catch(() => {})
  }

  dispose(): void {
    if (this.timer) clearInterval(this.timer)
    if (this.disposeTimer) clearTimeout(this.disposeTimer)
    this.timer = null
    this.seat?.leave().catch(() => {})
    this.seat = null
    this.onDispose?.()
  }
}
