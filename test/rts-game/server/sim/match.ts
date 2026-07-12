export { Match }

import { Room, type LocalParticipant } from 'telefunc'
import { NEUTRAL, TICK_HZ, TICK_MS, KEYFRAME_TICKS, type Team } from '../../shared/constants'
import { encodeFrame } from '../../shared/protocol'
import type { Command, MatchRoom, PlayerMeta } from '../../shared/types'
import { applyCommand } from './commands'
import { seedWorld } from './mapgen'
import { MatchSnapshots, TRACK } from './snapshot'
import { World } from './world'

const AUTHORITY_IDENTITY_PREFIX = 'authority:'

/** A running match: it owns the authoritative `World`, the tick loop, and the room's delivery
 *  lanes. It joins its *own* room as the "authority" participant — the one workaround that
 *  unlocks several findings at once (server-authored binary broadcast + a server command inbox).
 *
 * ── FINDING (no server-authored binary broadcast) ─────────────────────────────────────────────
 * `Room.announce()` is the room-authored broadcast, but it is text/JSON only — there is no
 * `Room.announceBinary()`. A server-authoritative game's whole output is a compact binary state
 * frame, so to broadcast it the server must `join()` its own room and publish as a participant.
 * That synthetic member then leaks into every client's roster (filtered out by `meta.authority`,
 * below) and into presence counts. See README finding "No server-authored binary broadcast".
 *
 * ── FINDING (no server inbox / client→server lane) ────────────────────────────────────────────
 * The server is not a participant with an inbox; the only server-side receive points are guards
 * (policy) and a server-side `subscribe()` (which sees room-wide broadcasts, not private, per-
 * player input). A server-authoritative game needs a *private, low-latency, per-player* command
 * lane to the authority. The only fit is `me.send(authorityId, cmd)` into the authority's
 * `listen()` — but `send` is documented as at-most-once peer signaling with no ordering or
 * backpressure, and it is text-only (no binary command batches). We depend on the live WS's
 * reliability the API doesn't promise. See README finding "No first-class client→server lane".
 */
class Match {
  readonly roomId: string
  private room: MatchRoom
  private world = new World()
  private snaps = new MatchSnapshots()
  private authority: LocalParticipant<PlayerMeta> | null = null
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

  /** Snapshot the lobby's team choices (trusted, server-side), seed the world, join the authority
   *  member, wire command intake + demand, and start ticking. */
  async start(teamPlayers: Map<Team, string[]>): Promise<void> {
    if (this.isRunning || this.ended) return
    for (const [team, ids] of teamPlayers) for (const id of ids) this.playerTeam.set(id, team)
    seedWorld(this.world, teamPlayers)

    // The authority: a room member that exists only to broadcast state and receive commands.
    // `selfDelivery: false` — it must never receive its own frames back.
    this.authority = await this.room.join(
      { name: 'Authority', color: '#8892b0', team: NEUTRAL, ready: true, authority: true },
      { identity: AUTHORITY_IDENTITY_PREFIX + this.roomId, selfDelivery: false },
    )

    // Command intake. `from.identity` is server-verified; team comes from the trusted snapshot,
    // never from `from.meta.team` (client-authored display state — see commands.ts).
    this.authority.listen((data, from) => {
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
    this.authority.onDemand((track, count) => {
      const prev = this.demand.get(track ?? '') ?? 0
      if (track && count > prev) this.snaps.forceKeyframe(track as (typeof TRACK)[keyof typeof TRACK])
      this.demand.set(track ?? '', count)
    })

    await this.setPhase('playing')
    await this.room.getParticipants().catch(() => {}) // load the member view so onLeave fires (see matches.ts)

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
    if (!this.authority) return
    const frame = this.snaps.build(this.world, track, periodic, clockSec)
    // `receivers` on the ack is the exact per-publish subscriber count; we drive keyframes off
    // `onDemand` instead, so the ack is fire-and-forget (guarded against teardown races).
    //
    // ── FINDING (no lossy/latest-only binary delivery) ─────────────────────────────────────────
    // The lane is reliable + ordered, so a slow/backgrounded client accumulates a state-frame
    // backlog (head-of-line blocking) when only the newest frame matters. `publish({ coalesce })`
    // gives *text* this latest-only collapse, but `publishBinary` has no `coalesce` and no
    // "drop stale, keep newest" mode. We stay at 10 Hz with small frames to live under it; a
    // 60 Hz game would need the datagram lane of #449. See README finding #12.
    this.authority.publishBinary(encodeFrame(frame), { track, keyFrame: frame.keyframe }).catch(() => {})
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

  /** A player disconnected for good / the room emptied of real players: end as a no-contest. */
  abandon(): void {
    if (this.ended) return
    this.finish(NEUTRAL)
  }

  private async setPhase(phase: 'playing'): Promise<void> {
    await Room.setAttributes(this.roomId, { phase }).catch(() => {})
  }

  dispose(): void {
    if (this.timer) clearInterval(this.timer)
    if (this.disposeTimer) clearTimeout(this.disposeTimer)
    this.timer = null
    this.authority?.leave().catch(() => {})
    this.authority = null
    this.onDispose?.()
  }
}
