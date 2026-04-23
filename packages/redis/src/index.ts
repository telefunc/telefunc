export { createRedisPubSubAdapter, RedisTransport }
export type { RedisPubSubOptions, RedisPublisher, RedisSubscriber }

import { DefaultPubSubAdapter, type PubSubAdapter, type PubSubTransport } from 'telefunc'

type RedisPublisher = {
  publish(channel: string, message: string | Uint8Array): Promise<number>
  incr(key: string): Promise<number>
}

type RedisSubscriber = {
  subscribe(...channels: string[]): Promise<unknown>
  unsubscribe(...channels: string[]): Promise<unknown>
  on(event: string, listener: (...args: any[]) => void): unknown
  off(event: string, listener: (...args: any[]) => void): unknown
}

type RedisPubSubOptions = {
  publisher: RedisPublisher
  subscriber: RedisSubscriber
  prefix?: string
}

function createRedisPubSubAdapter(options: RedisPubSubOptions): PubSubAdapter {
  return new DefaultPubSubAdapter(new RedisTransport(options))
}

const HEADER_BYTES = 8
const TS_EPOCH = 1577836800000 // 2020-01-01T00:00:00Z

class RedisTransport implements PubSubTransport {
  private readonly pub: RedisPublisher
  private readonly sub: RedisSubscriber
  private readonly prefix: string
  private readonly textCallbacks = new Map<string, (payload: string, info: { seq: number; ts: number }) => void>()
  private readonly binaryCallbacks = new Map<string, (payload: Uint8Array, info: { seq: number; ts: number }) => void>()

  constructor(options: RedisPubSubOptions) {
    this.pub = options.publisher
    this.sub = options.subscriber
    this.prefix = options.prefix ?? 'telefunc:'
    this.sub.on('message', this._onText)
    this.sub.on('messageBuffer', this._onBinary)
  }

  async send(key: string, payload: string) {
    const seq = await this.pub.incr(this.prefix + 'seq:' + key)
    const ts = Date.now()
    await this.pub.publish(this.prefix + 't:' + key, seq + '\n' + ts + '\n' + payload)
    return { seq, ts }
  }

  listen(key: string, onMessage: (payload: string, info: { seq: number; ts: number }) => void) {
    const channel = this.prefix + 't:' + key
    this.textCallbacks.set(key, onMessage)
    this.sub.subscribe(channel)
    return () => {
      this.textCallbacks.delete(key)
      this.sub.unsubscribe(channel)
    }
  }

  async sendBinary(key: string, payload: Uint8Array) {
    const seq = await this.pub.incr(this.prefix + 'seq:' + key)
    const ts = Date.now()
    const envelope = new Uint8Array(HEADER_BYTES + payload.byteLength)
    const view = new DataView(envelope.buffer)
    view.setUint32(0, seq)
    view.setUint32(4, ts - TS_EPOCH)
    envelope.set(payload, HEADER_BYTES)
    await this.pub.publish(this.prefix + 'b:' + key, envelope)
    return { seq, ts }
  }

  listenBinary(key: string, onMessage: (payload: Uint8Array, info: { seq: number; ts: number }) => void) {
    const channel = this.prefix + 'b:' + key
    this.binaryCallbacks.set(key, onMessage)
    this.sub.subscribe(channel)
    return () => {
      this.binaryCallbacks.delete(key)
      this.sub.unsubscribe(channel)
    }
  }

  // ── Internal: route ioredis events to per-key callbacks ──

  private _onText = (channel: string, message: string): void => {
    const tp = this.prefix + 't:'
    if (!channel.startsWith(tp)) return
    const i = message.indexOf('\n')
    const j = message.indexOf('\n', i + 1)
    if (i === -1 || j === -1) return
    this.textCallbacks.get(channel.slice(tp.length))?.(
      message.slice(j + 1),
      { seq: parseInt(message.slice(0, i), 10), ts: parseInt(message.slice(i + 1, j), 10) },
    )
  }

  private _onBinary = (channelBuf: Uint8Array, buf: Uint8Array): void => {
    const channel = new TextDecoder().decode(channelBuf)
    const bp = this.prefix + 'b:'
    if (!channel.startsWith(bp) || buf.byteLength < HEADER_BYTES) return
    const view = new DataView(buf.buffer, buf.byteOffset)
    this.binaryCallbacks.get(channel.slice(bp.length))?.(
      new Uint8Array(buf.buffer, buf.byteOffset + HEADER_BYTES, buf.byteLength - HEADER_BYTES),
      { seq: view.getUint32(0), ts: view.getUint32(4) + TS_EPOCH },
    )
  }
}
