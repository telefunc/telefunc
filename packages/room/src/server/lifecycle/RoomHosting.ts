export { RoomHosting }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { RoomIncarnation } from '../protocol.js'
import { reportRoomError } from '../reportRoomError.js'
import { eachInFlight } from '../store/cellStore.js'
import { holdViews, removeView, renewLease, RoomLease } from '../store/lease.js'
import { evictMember } from '../store/members.js'
import type { RoomStub } from '../RoomStub.js'
assertIsNotBrowser()

/** What this process hosts in a room incarnation under its lease: the members its handles join and the pages' views.
 *  The heartbeat calls `maintain()`, which renews the lease and redoes what failed. */
class RoomHosting {
  readonly lease = new RoomLease()
  private readonly incarnation: RoomIncarnation
  /** Each stub's latest view write, from its attach or a rewrite, to the key it wrote, null if it failed; its removal
   *  comes after it. */
  private readonly views = new Map<RoomStub, Promise<string | null>>()
  /** A view write failed, or the lease hosting the views was lost: `maintain()` writes them again. */
  private viewsLost = false
  /** Members whose eviction failed: the lease would host them for as long as it lives. */
  private readonly unevicted = new Set<string>()

  constructor({ incarnation }: { incarnation: RoomIncarnation }) {
    this.incarnation = incarnation
  }

  /** The stub's page attached: its view is written. */
  hostView(stub: RoomStub): void {
    this.views.set(
      stub,
      this.writeViews([stub]).then(([key]) => key ?? null),
    )
  }

  /** The stub's page detached: its view is removed, after its write. */
  dropView(stub: RoomStub): void {
    const view = this.views.get(stub)
    this.views.delete(stub)
    void view
      ?.then(async (key) => {
        if (key !== null) await removeView(this.incarnation, key)
      })
      .catch(reportRoomError)
  }

  /** `maintain()` evicts the member again, as `disconnected`. */
  evictLater(id: string): void {
    this.unevicted.add(id)
  }

  /** Renews the lease and evicts again whom it failed to; once the lease was lost, or a view write failed, writes every
   *  stub's view again in one write, after each one's earlier write. */
  async maintain(): Promise<void> {
    if (!(await renewLease(this.incarnation, this.lease))) this.viewsLost = true
    await eachInFlight(this.unevicted, (id) => this.evictAgain(id))
    if (!this.viewsLost) return
    this.viewsLost = false
    const stubs = [...this.views.keys()]
    if (stubs.length === 0) return
    const previous = Promise.all(this.views.values())
    const keys = previous.then(() => this.writeViews(stubs))
    stubs.forEach((stub, i) =>
      this.views.set(
        stub,
        keys.then((written) => written[i] ?? null),
      ),
    )
    await keys
  }

  private async evictAgain(id: string): Promise<void> {
    await evictMember(this.incarnation, id, { type: 'disconnected' })
    this.unevicted.delete(id)
  }

  /** The stubs' views' keys, in order; each null and the views lost if the write failed. */
  private async writeViews(stubs: RoomStub[]): Promise<(string | null)[]> {
    try {
      const keys = await holdViews(
        this.incarnation,
        this.lease,
        stubs.map((stub) => stub.end.id),
      )
      return stubs.map((_, i) => keys[i] ?? null)
    } catch (error) {
      this.viewsLost = true
      reportRoomError(error)
      return stubs.map(() => null)
    }
  }
}
