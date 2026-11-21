export { UpgradeStages }
export type { UpgradeStagesOwner }

import { UPGRADE_MAX_STAGED_BYTES, UPGRADE_MAX_STAGED_RECORDS, UPGRADE_STAGE_TTL_MS } from '../../constants.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { assertProtocol } from '../../wire/protocolViolation.js'
import type { PreparePayload } from '../../wire/payloads.js'
import type { Fault, Wire } from './ChannelMux.js'

/** The mux, which ends a probe whose stage is abandoned. */
interface UpgradeStagesOwner {
  endFault(wire: Wire, fault: Fault): void
}

type UpgradeStagesOptions = {
  owner: UpgradeStagesOwner
}

/** A probe WebSocket's stage, from its PREPARE until its barrier commits it. */
type StagedUpgrade = {
  upgradeId: string
  /** The session the upgrade moves off, which its barrier retires. */
  prevSessionId: string
  /** The page's number for the probe, from its PREPARE. */
  attempt: number
  /** The PREPARE's bytes, which count in the staged byte budget. */
  bytes: number
  timer: ReturnType<typeof setTimeout>
  phase: 'staged' | 'committing'
}

/** The SSE→WS upgrades staged on probe WebSockets, one per session, within a record and a byte budget. A stage ends
 *  when its barrier commits it, its TTL passes, a RECONCILE claims its session, or its probe closes. */
class UpgradeStages {
  private readonly owner: UpgradeStagesOwner
  private readonly byProbe = new Map<Wire, StagedUpgrade>()
  private readonly byPrevSession = new Map<string, Wire>()
  private stagedBytes = 0

  constructor({ owner }: UpgradeStagesOptions) {
    this.owner = owner
  }

  get(probe: Wire): Readonly<StagedUpgrade> | undefined {
    return this.byProbe.get(probe)
  }

  /** The probe staged to take over the session `sessionId`. */
  probeFor(sessionId: string): Wire | undefined {
    return this.byPrevSession.get(sessionId)
  }

  /** Stages `probe` for the PREPARE's session; false, staging nothing, where the session's stage is a newer attempt's.
   *  A page stages one attempt at a time, so a newer PREPARE replaces what an earlier attempt left staged. */
  stage(probe: Wire, payload: PreparePayload, bytes: number): boolean {
    const staged = this.byPrevSession.get(payload.sessionId)
    if (staged !== undefined) {
      const stage = this.byProbe.get(staged)
      assertProtocol(stage?.phase === 'staged', 'session already committing')
      if (payload.attempt <= stage.attempt) return false
      this.abandon(staged)
    }
    assertProtocol(this.byProbe.size < UPGRADE_MAX_STAGED_RECORDS, 'staged record budget')
    assertProtocol(this.stagedBytes + bytes <= UPGRADE_MAX_STAGED_BYTES, 'staged byte budget')

    const timer = unrefTimer(setTimeout(() => this.abandon(probe), UPGRADE_STAGE_TTL_MS))
    this.byProbe.set(probe, {
      upgradeId: payload.upgradeId,
      prevSessionId: payload.sessionId,
      attempt: payload.attempt,
      bytes,
      timer,
      phase: 'staged',
    })
    this.byPrevSession.set(payload.sessionId, probe)
    this.stagedBytes += bytes
    return true
  }

  /** Marks the probe's stage committing: from now on a claim of its session is refused and `abandon` is a no-op. */
  commit(probe: Wire): void {
    this.byProbe.get(probe)!.phase = 'committing'
  }

  /** A RECONCILE claims `sessionId`: the probe staged for it is abandoned. A committing barrier already owns the
   *  session, so a claim on it then is refused rather than allowed to abandon the stage under the commit. */
  claim(sessionId: string): void {
    const staleProbe = this.byPrevSession.get(sessionId)
    if (staleProbe === undefined) return
    assertProtocol(this.byProbe.get(staleProbe)?.phase !== 'committing', 'session claimed mid-commit')
    this.abandon(staleProbe)
  }

  /** Forgets the stage and ends the probe holding it, as a loss: it holds no session. No-op once the stage is
   *  committing. */
  abandon(probe: Wire): void {
    if (this.byProbe.get(probe)?.phase !== 'staged') return
    this.clear(probe)
    this.owner.endFault(probe, { scope: 'loss' })
  }

  /** Forgets the stage and leaves the probe wire alone. */
  clear(probe: Wire): void {
    const stage = this.byProbe.get(probe)
    if (!stage) return
    clearTimeout(stage.timer)
    this.byProbe.delete(probe)
    if (this.byPrevSession.get(stage.prevSessionId) === probe) this.byPrevSession.delete(stage.prevSessionId)
    this.stagedBytes -= stage.bytes
  }
}
