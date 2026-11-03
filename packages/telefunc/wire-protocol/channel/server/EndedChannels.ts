export { EndedChannels }

import type { ServerChannel } from './ServerChannel.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

type EndedChannelsOptions = {
  /** Lets go of a channel no page can need anymore. */
  onRelease: (channel: ServerChannel) => void
}

/** The ended channels whose page may still need them: each stays attachable while its page is attached, and for a
 *  while once it isn't, unless its page lets it go first. */
class EndedChannels {
  /** Each channel's release timer: null while its page is attached. */
  private readonly timers = new Map<ServerChannel, ReturnType<typeof setTimeout> | null>()
  private readonly onRelease: (channel: ServerChannel) => void

  constructor({ onRelease }: EndedChannelsOptions) {
    this.onRelease = onRelease
  }

  has(channel: ServerChannel): boolean {
    return this.timers.has(channel)
  }

  /** Keeps `channel` while its page is attached (`waitMs` null), else for `waitMs`. */
  keep(channel: ServerChannel, waitMs: number | null): void {
    const timer = this.timers.get(channel)
    if (timer) clearTimeout(timer)
    this.timers.set(channel, waitMs === null ? null : unrefTimer(setTimeout(() => this.release(channel), waitMs)))
  }

  /** Lets `channel` go; false if it wasn't kept. */
  release(channel: ServerChannel): boolean {
    if (!this.timers.has(channel)) return false
    const timer = this.timers.get(channel)
    if (timer) clearTimeout(timer)
    this.timers.delete(channel)
    this.onRelease(channel)
    return true
  }
}
