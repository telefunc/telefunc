// The widest RECONCILE the server's checks admit passes the control-frame cap, on the real Node server over SSE and over
// a WebSocket: every channel the connection may hold, each at the longest id and the widest values its entry may
// carry. The cap counts what the frame's checks admit.

import { expect, test } from 'vitest'
import { decodeU32, encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { WIDEST_ENTRY, WIRE_MAX_CTRL_FRAME_BYTES, type ReconcileOpenEntry } from '../../wire/payloads.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../../wire/requestKind.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ServerBroadcast } from '../server/ServerBroadcast.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import { getWireUrl } from './getSessionUrl.js'
import { framesStart, pair, useRealServer, world } from './realServer.fixture.js'
import { ATTACHABLE_NOW } from '../server/ServerChannel.js'

useRealServer(['sse', 'ws'])

/** 4,096 entries, each at the longest id and the widest values the server's checks admit, under the widest attempt. */
function widestReconcile(): Uint8Array<ArrayBuffer> {
  const open: ReconcileOpenEntry[] = Array.from({ length: 4_096 }, (_, at) => ({
    id: String(at).padStart(256, 'x'),
    ix: 0xffff - at,
    lastSeq: Number.MAX_SAFE_INTEGER,
    initial: true,
    want: { version: 0xffffffff, text: false, binary: false },
    probe: 0xffffffff,
  }))
  return encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 0xffffffff, open })
}

/** The tag of each frame the server's answer carries, to its RECONCILED or its end. */
function tagsTo(tags: number[], frame: Uint8Array<ArrayBuffer>): boolean {
  tags.push(frame[0] ?? -1)
  return frame[0] === TAG.RECONCILED
}

test('the widest RECONCILE the checks admit is reconciled on SSE', async () => {
  const frame = widestReconcile()
  const response = await fetch(getMarkedRequestUrl(getWireUrl(world.telefuncUrl), REQUEST_KIND.SSE), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      Accept: 'text/event-stream',
      [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
    },
    body: new Blob([encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [frame])]),
  })
  const tags: number[] = []
  const body = response.body
  if (body !== null) {
    const parser = new SseEvents()
    const reader = body.getReader()
    let reconciled = false
    while (!reconciled) {
      const { value, done } = await reader.read()
      if (done) break
      parser.push(value, (raw) => void (reconciled = reconciled || tagsTo(tags, raw)))
    }
    await reader.cancel()
  }
  expect([response.status, tags.includes(TAG.RECONCILED), frame.byteLength > 1_600_000]).toEqual([200, true, true])
})

test('the widest RECONCILE the checks admit is reconciled on a WebSocket', async () => {
  const frame = widestReconcile()
  const url = new URL(getWireUrl(world.telefuncUrl))
  url.protocol = 'ws:'
  const socket = new WebSocket(url.href)
  socket.binaryType = 'arraybuffer'
  const tags: number[] = []
  const closed = await new Promise<number | null>((resolve) => {
    socket.onopen = () => socket.send(frame)
    socket.onmessage = (event) => {
      if (!(event.data instanceof ArrayBuffer)) return
      const message = new Uint8Array(event.data)
      for (const each of isOneFrame(message) ? [message] : messageFrames(message)) {
        if (tagsTo(tags, each)) resolve(null)
      }
    }
    socket.onclose = (event) => resolve(event.code)
  })
  socket.close()
  expect([closed, tags.includes(TAG.RECONCILED), decode(frame).tag]).toEqual([null, true, TAG.RECONCILE])
})

test("the cap counts each key a real page's RECONCILE entry carries, a broadcast's want and a probe among them", async () => {
  const keys = new Set<string>()
  world.hooks = [
    {
      kind: 'open',
      body: (bytes) => {
        const at = framesStart(bytes)
        const frame = decode(bytes.subarray(at + 4, at + 4 + decodeU32(bytes.subarray(at, at + 4))))
        if (frame.tag === TAG.RECONCILE) for (const entry of frame.payload.open) for (const key in entry) keys.add(key)
        return [bytes]
      },
    },
  ]
  const connectionKey = crypto.randomUUID()
  const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  getChannelMux().registerChannel(broadcast, ATTACHABLE_NOW)
  const transports: ('sse' | 'ws')[] = ['sse']
  const connection = { telefuncUrl: world.telefuncUrl, transports, idleTimeout: 0, connectionKey }
  const page = new ClientBroadcast<string>({ channelId: broadcast.id, connection })
  page.subscribe(() => {})
  const plain = pair(connectionKey, ['sse'])
  try {
    await Promise.all([page.opened, plain.page.opened])
    expect([[...keys].filter((key) => !(key in WIDEST_ENTRY)), keys.has('want'), keys.has('probe')]).toEqual([
      [],
      true,
      true,
    ])
    expect(widestReconcile().byteLength).toBeLessThanOrEqual(WIRE_MAX_CTRL_FRAME_BYTES)
  } finally {
    page.close()
    plain.page.abort()
  }
})
