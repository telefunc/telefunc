export {
  onBenchInfo,
  onBenchEchoLatency,
  onBenchBinaryEchoLatency,
  onBenchTextThroughput,
  onBenchBinaryThroughput,
  onBenchServerPush,
  onBenchBinaryServerPush,
  onBenchBroadcastRoom,
  onBenchBroadcastPublish,
}

import { Broadcast, Channel } from 'telefunc'

const INSTANCE_ID = process.env.INSTANCE_ID ?? 'local'

async function onBenchInfo() {
  return {
    instanceId: INSTANCE_ID,
    hasRedis: !!process.env.REDIS_URL,
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
  }
}

type TextMsg = { seq: number; payload: string }
type PushMsg = { seq: number; payload: string; sentAt: number }

/** Sequential ack echo — client sends, server echoes back; measures RTT. */
async function onBenchEchoLatency() {
  const ch = new Channel<(msg: TextMsg) => TextMsg, never>()
  ch.listen((msg) => msg)
  return { channel: ch.client, instance: INSTANCE_ID }
}

/** Binary sequential ack echo — client sends Uint8Array, server returns timing ack. */
async function onBenchBinaryEchoLatency() {
  const ch = new Channel()
  ch.listenBinary((): { recvAt: number } => ({ recvAt: Date.now() }))
  return { channel: ch.client, instance: INSTANCE_ID }
}

type CtrlMsg = { ctrl: 'start' | 'stop' }
type TextInbound = TextMsg | CtrlMsg

/** Pure-direction throughput (text). Two independent measurements over one channel:
 *   - TX: client floods text data; server counts via `getServerReceived` (no echoes).
 *   - RX: client sends `{ctrl:'start'}`; server fires text data fire-and-forget until `{ctrl:'stop'}`.
 *  Avoids the echo round-trip — each direction's rate reflects wire-level one-way capacity. */
async function onBenchTextThroughput(payloadSize: number) {
  let serverReceived = 0
  let pushing = false
  const payload = 'x'.repeat(payloadSize)
  const ch = new Channel<(msg: TextInbound) => void, (msg: TextMsg) => void>()
  // The push loop is spawned OUTSIDE the listen callback. If we awaited it inside
  // the callback, telefunc would defer `_trackConsumption` for the `start` frame
  // until the loop ended — i.e., until `stop` arrived — starving credit refresh.
  const pushLoop = async () => {
    while (pushing) {
      try {
        await ch.send({ seq: 0, payload })
      } catch {
        break
      }
    }
  }
  ch.listen((msg) => {
    if ('ctrl' in msg) {
      if (msg.ctrl === 'start' && !pushing) {
        pushing = true
        void pushLoop()
      } else if (msg.ctrl === 'stop') {
        pushing = false
      }
    } else {
      serverReceived++
    }
  })
  return {
    channel: ch.client,
    instance: INSTANCE_ID,
    getServerReceived: async () => serverReceived,
  }
}

/** Pure-direction throughput (binary). Control via text (`{ctrl:'start'|'stop'}`), data via binary. */
async function onBenchBinaryThroughput(payloadSize: number) {
  let serverReceived = 0
  let pushing = false
  const payload = new Uint8Array(payloadSize).fill(0xab)
  const ch = new Channel<(msg: CtrlMsg) => void, never>()
  // Push loop runs detached — see text variant for rationale.
  const pushLoop = async () => {
    while (pushing) {
      try {
        await ch.sendBinary(payload)
      } catch {
        break
      }
    }
  }
  ch.listen((msg) => {
    if (msg.ctrl === 'start' && !pushing) {
      pushing = true
      void pushLoop()
    } else if (msg.ctrl === 'stop') {
      pushing = false
    }
  })
  ch.listenBinary(() => {
    serverReceived++
  })
  return {
    channel: ch.client,
    instance: INSTANCE_ID,
    getServerReceived: async () => serverReceived,
  }
}

/** Server push: fires `count` text frames after receiving a 'go' trigger. */
async function onBenchServerPush(count: number, payloadSize: number) {
  const ch = new Channel<(msg: 'go') => void, (msg: PushMsg) => void>()
  const payload = 'x'.repeat(payloadSize)
  ch.listen(() => {
    for (let i = 0; i < count; i++) void ch.send({ seq: i, payload, sentAt: Date.now() })
  })
  return { channel: ch.client, instance: INSTANCE_ID, count }
}

/** Binary server push: fires `count` Uint8Array frames after receiving a 'go' trigger. */
async function onBenchBinaryServerPush(count: number, payloadSize: number) {
  const ch = new Channel<(msg: 'go') => void, never>()
  const payload = new Uint8Array(payloadSize).fill(0xab)
  ch.listen(() => {
    for (let i = 0; i < count; i++) void ch.sendBinary(payload)
  })
  return { channel: ch.client, instance: INSTANCE_ID, count }
}

/** Open a broadcast room and subscribe. Returns room + server-side stats closure. */
async function onBenchBroadcastRoom(key: string) {
  const room = new Broadcast<PushMsg>({ key })
  let serverReceived = 0
  room.subscribe(() => {
    serverReceived++
  })
  return {
    room,
    instance: INSTANCE_ID,
    getStats: async () => ({ count: serverReceived, instance: INSTANCE_ID }),
  }
}

/** Publish `count` messages to a broadcast key; returns elapsed time. */
async function onBenchBroadcastPublish(key: string, count: number, payloadSize: number) {
  const room = new Broadcast<PushMsg>({ key })
  const payload = 'x'.repeat(payloadSize)
  const t0 = Date.now()
  const promises: Promise<unknown>[] = []
  for (let i = 0; i < count; i++) promises.push(room.publish({ seq: i, payload, sentAt: Date.now() }))
  await Promise.all(promises)
  return { instance: INSTANCE_ID, elapsedMs: Date.now() - t0, count }
}
