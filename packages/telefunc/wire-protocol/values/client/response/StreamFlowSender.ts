export { StreamFlowSender }

import { getSessionUrl } from '../../../channel/client/getSessionUrl.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../../../wire/requestKind.js'
import { STREAM_FLOW_FOUND, STREAM_FLOW_PAUSE_BYTES } from '../../../constants.js'

const RETRY_MS = 1000
const MAX_RETRIES = 60

type CallContext = {
  telefuncUrl: string
  headers: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
  abortController: AbortController
}

/** Tells the server which of a response's streams to pause or cancel: one request at a time, each with the whole state,
 *  so none can overtake another. */
class StreamFlowSender {
  private readonly unread = new Map<number, number>()
  private readonly paused = new Set<number>()
  private readonly cancelled = new Set<number>()
  private readonly id: string
  private readonly callContext: CallContext
  private sending = false
  private stopped = false
  private acknowledged = ''
  /** The streams the server may hold paused. */
  private mayBeHeld = new Set<number>()
  private readonly done = new Set<number>()
  private retry: { timer: ReturnType<typeof setTimeout>; resume: () => void } | null = null

  constructor(id: string, callContext: CallContext) {
    this.id = id
    this.callContext = callContext
    callContext.abortController.signal.addEventListener('abort', () => this.stop(), { once: true })
  }

  onBuffered(index: number, bytes: number): void {
    const unread = (this.unread.get(index) ?? 0) + bytes
    this.unread.set(index, unread)
    if (unread <= STREAM_FLOW_PAUSE_BYTES || this.paused.has(index)) return
    this.paused.add(index)
    void this.send()
  }

  onRead(index: number, bytes: number): void {
    const unread = this.unread.get(index)! - bytes
    this.unread.set(index, unread)
    if (unread > STREAM_FLOW_PAUSE_BYTES / 2 || !this.paused.delete(index)) return
    void this.send()
  }

  /** Nothing waits for this stream any more, so no pause on it is left to undo. */
  onDone(index: number): void {
    this.done.add(index)
    this.paused.delete(index)
    this.mayBeHeld.delete(index)
  }

  onCancel(index: number): void {
    this.onDone(index)
    this.cancelled.add(index)
    void this.send()
  }

  /** The response ended: nothing is left to ask for. */
  stop(): void {
    this.stopped = true
    if (!this.retry) return
    clearTimeout(this.retry.timer)
    this.retry.resume()
    this.retry = null
  }

  private async send(): Promise<void> {
    if (this.sending) return
    this.sending = true
    let retries = 0
    while (!this.stopped) {
      const paused = [...this.paused]
      const state = JSON.stringify({ id: this.id, paused, cancelled: [...this.cancelled] })
      if (state === this.acknowledged) break
      const found = await this.post(state)
      const stillOpen = paused.filter((index) => !this.done.has(index))
      if (found === null) for (const index of stillOpen) this.mayBeHeld.add(index)
      if (found) {
        this.acknowledged = state
        this.mayBeHeld = new Set(stillOpen)
        retries = 0
      } else if (found === false && this.mayBeHeld.size === 0) {
        // This instance doesn't hold the response and no pause waits to be undone: the page buffers what comes.
        this.stop()
      } else if (++retries > MAX_RETRIES) {
        // A pause that may be held can't be undone, so a read fails rather than waits forever.
        if (this.mayBeHeld.size > 0) this.callContext.abortController.abort()
        this.stop()
      } else {
        // A pause may have reached the server, so its resume must too: the next request may land elsewhere.
        await new Promise<void>((resume) => {
          this.retry = { timer: setTimeout(resume, RETRY_MS), resume }
        })
      }
    }
    this.sending = false
  }

  /** Whether the server holds the response, or `null` when the request may have reached it without an answer. */
  private async post(body: string): Promise<boolean | null> {
    const { telefuncUrl, headers, abortController } = this.callContext
    const fetch = this.callContext.fetch ?? window.fetch
    try {
      const response = await fetch(getMarkedRequestUrl(getSessionUrl(telefuncUrl), REQUEST_KIND.FLOW), {
        method: 'POST',
        body,
        credentials: 'same-origin',
        headers: { 'Content-Type': 'text/plain', [REQUEST_KIND_HEADER]: REQUEST_KIND.FLOW, ...headers },
        signal: abortController.signal,
      })
      return response.ok && (await response.text()) === STREAM_FLOW_FOUND
    } catch {
      return null
    }
  }
}
