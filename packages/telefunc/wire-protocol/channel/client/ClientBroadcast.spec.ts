import { afterEach, describe, expect, it, vi } from 'vitest'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import { ClientConnection } from './ClientConnection.js'

afterEach(() => {
  vi.restoreAllMocks()
})

describe('ClientBroadcast uplink', () => {
  it('sends a lossy publish after a plain one of its stream that it followed', () => {
    const { page, waiting } = behindUplink()
    page._publishBinaryUnreported(new Uint8Array([1]), 'cursor', true)
    page._publishBinaryUnreported(new Uint8Array([2]), 'cursor', false)
    page._publishBinaryUnreported(new Uint8Array([3]), 'cursor', true)

    expect(waiting()).toEqual([1, 2, 3])
  })

  it("replaces a stream's waiting lossy publish across a plain one of another stream", () => {
    const { page, waiting } = behindUplink()
    page._publishBinaryUnreported(new Uint8Array([1]), 'cursor', true)
    page._publishBinaryUnreported(new Uint8Array([2]), 'drawing', false)
    page._publishBinaryUnreported(new Uint8Array([3]), 'cursor', true)

    expect(waiting()).toEqual([3, 2])
  })
})

/** A page's broadcast on an uplink that keeps every publish waiting, as one that is behind does; the first byte of each
 *  waiting publish, in the order they go. */
function behindUplink() {
  const queue: { seq: number; data: Uint8Array }[] = []
  const connection = {
    sendSequenced(
      _channel: unknown,
      _kind: unknown,
      _encode: unknown,
      data: Uint8Array,
      onQueued?: (seq: number) => void,
    ) {
      const seq = queue.length + 1
      queue.push({ seq, data })
      onQueued?.(seq)
    },
    replaceWaiting(_channel: unknown, seq: number, _encode: unknown, data: Uint8Array) {
      const entry = queue.find((waiting) => waiting.seq === seq)
      if (entry === undefined) return false
      entry.data = data
      return true
    },
  }
  const anyMethod = new Proxy(connection, {
    get: (target, prop) => (prop in target ? target[prop as keyof typeof target] : () => {}),
  })
  vi.spyOn(ClientConnection, 'getOrCreate').mockReturnValue(anyMethod as never)
  const page = new ClientBroadcast({
    channelId: crypto.randomUUID(),
    connection: { transports: [CHANNEL_TRANSPORT.WS], telefuncUrl: 'http://test.local/_telefunc' },
  })
  return { page, waiting: () => queue.map(({ data }) => data[0]) }
}
