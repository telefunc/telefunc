export { StreamFlowSender }

import { getSessionUrl } from '../../../channel/client/getSessionUrl.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../../../wire/requestKind.js'
import { STREAM_FLOW_FOUND, STREAM_FLOW_LEASE_MS, STREAM_FLOW_PAUSE_BYTES } from '../../../constants.js'

const RENEW_MS = STREAM_FLOW_LEASE_MS / 3

type CallContext = {
  telefuncUrl: string
  headers: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
  abortController: AbortController
}

/** Tells the server which of a response's streams to pause or cancel: one request at a time, each with the whole state,
 *  so none can overtake another. The server drops a pause that isn't renewed, so no failure here can hold a stream. */
class StreamFlowSender {
  private readonly unread = new Map<number, number>()
  private readonly paused = new Set<number>()
  private readonly cancelled = new Set<number>()
  private readonly id: string
  private readonly callContext: CallContext
  private sending = false
  private stopped = false
  private acknowledged = ''
  private wake: (() => void) | null = null

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

  onCancel(index: number): void {
    this.paused.delete(index)
    this.cancelled.add(index)
    void this.send()
  }

  /** The response ended, or this instance doesn't hold it and the page buffers what comes: nothing is left to ask for. */
  stop(): void {
    this.stopped = true
    this.wake?.()
  }

  private state(): string {
    return JSON.stringify({ id: this.id, paused: [...this.paused], cancelled: [...this.cancelled] })
  }

  private async send(): Promise<void> {
    this.wake?.()
    if (this.sending) return
    this.sending = true
    while (!this.stopped) {
      const state = this.state()
      if (state === this.acknowledged && this.paused.size === 0) break
      const found = await this.post(state)
      if (found) this.acknowledged = state
      if (found === false) this.stop()
      if (!this.stopped && state === this.state()) await this.untilRenewal()
    }
    this.sending = false
  }

  private untilRenewal(): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, RENEW_MS)
      this.wake = () => {
        clearTimeout(timer)
        resolve()
      }
    })
  }

  /** Whether the server holds the response, or `null` for no answer. The server always answers 200: another status is a proxy's. */
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
      return response.ok ? (await response.text()) === STREAM_FLOW_FOUND : null
    } catch {
      return null
    }
  }
}
