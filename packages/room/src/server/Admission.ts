export { Admission }
export type { AdmissionOptions }

import { participantLeftError, roomClosedError } from '../errors.js'
import type { ParticipantMeta } from '../types.js'

type AdmissionOptions = { meta: ParticipantMeta; identity: string | null; hidden: boolean }

/** A join through one handle. Once its holder took the member: its inbox opening (`owned`), its record durable
 *  (`committed`), or given up because the member left, its holder detached or the room closed (`abandoned`). Before
 *  that it is a guard running; once its join applies, a member like any other. */
class Admission {
  readonly id = crypto.randomUUID()
  readonly joinedAt = Date.now()
  readonly meta: ParticipantMeta
  readonly identity: string | null
  readonly hidden: boolean
  private state: 'owned' | 'committed' | 'abandoned' = 'owned'

  constructor({ meta, identity, hidden }: AdmissionOptions) {
    this.meta = meta
    this.identity = identity
    this.hidden = hidden
  }

  /** Its holder's detach leaves the member to the admission, which rolls itself back. */
  get owned(): boolean {
    return this.state === 'owned'
  }

  get abandoned(): boolean {
    return this.state === 'abandoned'
  }

  commit(): void {
    this.state = 'committed'
  }

  abandon(): void {
    this.state = 'abandoned'
  }

  /** Throws once abandoned: the room closed, or the member left. */
  assertLive(room: { roomId: string; closed: boolean }): void {
    if (this.state === 'abandoned') throw room.closed ? roomClosedError(room.roomId) : participantLeftError()
  }
}
