export {
  onBenchInfo,
  onBenchChannelEcho,
  onBenchChannelEchoNoAck,
  onBenchChannelBinaryEchoNoAck,
  onBenchChannelServerPush,
  onBenchChannelBinaryEcho,
  onBenchChannelBinaryServerPush,
  onBenchBroadcast,
  onBenchBroadcastServerPush,
  onBenchBroadcastBinary,
  onBenchBroadcastBinaryServerPush,
}

import { Channel, Broadcast } from 'telefunc'

const INSTANCE_ID = process.env.INSTANCE_ID ?? 'local'
const HAS_REDIS = !!process.env.REDIS_URL

async function onBenchInfo() {
  return { instanceId: INSTANCE_ID, hasRedis: HAS_REDIS }
}

type EchoMsg = { seq: number; payload: string }
type EchoAck = { seq: number; recvAt: number; instance: string }

/** Client→server channel: server echoes ack with its receive timestamp + instance id.
 *  Page measures RTT per send. */
async function onBenchChannelEcho() {
  const ch = new Channel<(msg: EchoMsg) => EchoAck, never>()
  ch.listen((msg) => ({ seq: msg.seq, recvAt: Date.now(), instance: INSTANCE_ID }))
  return { channel: ch.client, instance: INSTANCE_ID }
}

/** Fire-and-forget echo: server `send`s each received message back with no per-send ack.
 *  Client fires N sends in a tight loop, then awaits all N echoes — exposes pure throughput
 *  on both legs without the serial ack-RTT pessimization. */
async function onBenchChannelEchoNoAck() {
  const ch = new Channel<(msg: EchoMsg) => void, (msg: EchoMsg) => void>()
  ch.listen((msg) => {
    void ch.send(msg)
  })
  return { channel: ch.client, instance: INSTANCE_ID }
}

/** Binary fire-and-forget echo — same shape as `onBenchChannelEchoNoAck` but on the
 *  binary path. Server echoes the same `Uint8Array` back. */
async function onBenchChannelBinaryEchoNoAck() {
  const ch = new Channel()
  ch.listenBinary((msg) => {
    void ch.sendBinary(msg)
  })
  return { channel: ch.client, instance: INSTANCE_ID }
}

type PushMsg = { seq: number; payload: string; sentAt: number }

/** Server→client channel: server blasts `count` messages of `payloadSize` bytes when the
 *  client sends a `'go'` trigger message. The trigger pattern lets the page open the channel
 *  + wire its listener *before* the burst — channel-attach cost stays out of the timed
 *  window. */
async function onBenchChannelServerPush(count: number, payloadSize: number) {
  const ch = new Channel<(msg: 'go') => void, (msg: PushMsg) => void>()
  const payload = 'x'.repeat(payloadSize)
  ch.listen(() => {
    for (let i = 0; i < count; i++) {
      void ch.send({ seq: i, payload, sentAt: Date.now() })
    }
  })
  return { channel: ch.client, instance: INSTANCE_ID, count }
}

type BinaryAck = { recvAt: number; instance: string }

/** Binary-path channel echo: client sends `Uint8Array`, server returns an ack object so the
 *  page can measure RTT for binary frames. The ack itself is JSON (text path) — the request
 *  side is what we're benchmarking. */
async function onBenchChannelBinaryEcho() {
  const ch = new Channel()
  ch.listenBinary((): BinaryAck => ({ recvAt: Date.now(), instance: INSTANCE_ID }))
  return { channel: ch.client, instance: INSTANCE_ID }
}

/** Server→client binary push: server fires `count` `Uint8Array` frames when the client
 *  sends a `'go'` text trigger. Same pre-open / trigger split as `onBenchChannelServerPush`. */
async function onBenchChannelBinaryServerPush(count: number, payloadSize: number) {
  const ch = new Channel<(msg: 'go') => void, never>()
  const payload = new Uint8Array(payloadSize)
  for (let i = 0; i < payloadSize; i++) payload[i] = i & 0xff
  ch.listen(() => {
    for (let i = 0; i < count; i++) void ch.sendBinary(payload)
  })
  return { channel: ch.client, instance: INSTANCE_ID, count }
}

type BroadcastMsg = { seq: number; payload: string; sentAt: number }

/** Broadcast room: page is both publisher and subscriber. With Redis substrate, traffic
 *  fans out across instances and back. Server-side counter records per-instance receives. */
async function onBenchBroadcast(key: string) {
  const room = new Broadcast<BroadcastMsg>({ key })
  const stats = { count: 0, instance: INSTANCE_ID }
  room.subscribe(() => {
    stats.count++
  })
  return {
    room,
    instance: INSTANCE_ID,
    getStats: async () => ({ ...stats }),
  }
}

/** Server-driven broadcast: server publishes `count` messages on `key`. The `mode` flag
 *  splits the two regimes the perf data needs to disambiguate:
 *  - 'sequential' awaits each `publish()` before the next — 1000 serial round-trips through
 *    the substrate; bounded by RTT × count, not CPU.
 *  - 'parallel'   fires all publishes via `Promise.all` — bounded by Redis Lua throughput +
 *    pipelining (or a tight in-memory loop for the local substrate). */
async function onBenchBroadcastServerPush(
  key: string,
  count: number,
  payloadSize: number,
  mode: 'sequential' | 'parallel',
) {
  const room = new Broadcast<BroadcastMsg>({ key })
  const payload = 'x'.repeat(payloadSize)
  const t0 = Date.now()
  if (mode === 'sequential') {
    for (let i = 0; i < count; i++) {
      await room.publish({ seq: i, payload, sentAt: Date.now() })
    }
  } else {
    const sentAt = Date.now()
    const promises: Array<Promise<unknown>> = []
    for (let i = 0; i < count; i++) {
      promises.push(room.publish({ seq: i, payload, sentAt }))
    }
    await Promise.all(promises)
  }
  return { instance: INSTANCE_ID, elapsedMs: Date.now() - t0, count, mode }
}

/** Binary-path broadcast room: uses `subscribeBinary` to keep payloads as raw bytes through
 *  Redis and the WS frame, skipping the JSON `stringify`/`parse` cost of the text path. */
async function onBenchBroadcastBinary(key: string) {
  const room = new Broadcast({ key })
  const stats = { count: 0, instance: INSTANCE_ID }
  room.subscribeBinary(() => {
    stats.count++
  })
  return {
    room,
    instance: INSTANCE_ID,
    getStats: async () => ({ ...stats }),
  }
}

/** Server-driven binary broadcast: same shape as `onBenchBroadcastServerPush` but publishes
 *  raw `Uint8Array`. Bytes never get UTF-8 encoded — meaningful for >>1KB payloads where the
 *  JSON serializer dominates the text-path bandwidth. */
async function onBenchBroadcastBinaryServerPush(
  key: string,
  count: number,
  payloadSize: number,
  mode: 'sequential' | 'parallel',
) {
  const room = new Broadcast({ key })
  const payload = new Uint8Array(payloadSize)
  // Fill with non-zero bytes so any optimizer can't elide the copy.
  for (let i = 0; i < payloadSize; i++) payload[i] = i & 0xff
  const t0 = Date.now()
  if (mode === 'sequential') {
    for (let i = 0; i < count; i++) await room.publishBinary(payload)
  } else {
    const promises: Array<Promise<unknown>> = []
    for (let i = 0; i < count; i++) promises.push(room.publishBinary(payload))
    await Promise.all(promises)
  }
  return { instance: INSTANCE_ID, elapsedMs: Date.now() - t0, count, mode }
}
