export { StreamFlow, handleStreamFlowRequest }

import { getGlobalObject } from '../../../../utils/getGlobalObject.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import { STREAM_FLOW_FOUND } from '../../../constants.js'

const globalObject = getGlobalObject('wire-protocol/values/server/response/StreamFlow.ts', {
  flows: new Map<string, StreamFlow>(),
})

/** What the page asks of a response's streams, by index. Its id, known only to that response's page, is what lets a
 *  request reach it. Lives as long as the response. */
class StreamFlow {
  paused = new Set<number>()
  readonly cancelled = new Set<number>()
  private next = createDeferred<{ type: 'flowChanged' }>()
  private readonly id: string
  private readonly onCancel: (index: number) => void

  constructor(id: string, onCancel: (index: number) => void) {
    this.id = id
    this.onCancel = onCancel
    globalObject.flows.set(id, this)
  }

  get changed(): Promise<{ type: 'flowChanged' }> {
    return this.next.promise
  }

  close(): void {
    globalObject.flows.delete(this.id)
  }

  apply(paused: number[], cancelled: number[]): void {
    this.paused = new Set(paused)
    for (const index of cancelled) {
      if (this.cancelled.has(index)) continue
      this.cancelled.add(index)
      this.onCancel(index)
    }
    const { resolve } = this.next
    this.next = createDeferred()
    resolve({ type: 'flowChanged' })
  }
}

/** One answer for every id that names no live response, so a request can't tell an expired id from a made-up one.
 *  A 200, as a response often ends before the page's request arrives and the browser logs any other status as an error. */
function handleStreamFlowRequest(text: string) {
  const request = parseFlowRequest(text)
  const flow = request && globalObject.flows.get(request.id)
  flow?.apply(request!.paused, request!.cancelled)
  return {
    statusCode: 200 as const,
    contentType: 'text/plain' as const,
    headers: [],
    body: flow ? STREAM_FLOW_FOUND : 'not found',
  }
}

function parseFlowRequest(text: string): { id: string; paused: number[]; cancelled: number[] } | null {
  try {
    const { id, paused, cancelled } = JSON.parse(text)
    return typeof id === 'string' && isIndexList(paused) && isIndexList(cancelled) ? { id, paused, cancelled } : null
  } catch {
    return null
  }
}

function isIndexList(value: unknown): value is number[] {
  return Array.isArray(value) && value.every((index) => typeof index === 'number')
}
