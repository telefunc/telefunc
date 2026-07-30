import { describe, expect, test } from 'vitest'

import { CHANNEL_MESSAGE_LIMIT_BYTES, CHANNEL_TRANSPORT } from './constants.js'
import { ClientConnection } from './client/connection.js'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import { StreamReader } from './server/request/StreamReader.js'
import { encodeU32 } from './frame.js'

// The inbound per-message cap (`config.channel.messageLimit`), enforced at every ingress:
// the mux terminates oversized WS frames, StreamReader rejects oversized declared lengths
// before buffering a byte (SSE POSTs, binary-RPC metadata), and the client fails oversized
// sends locally with a usage error instead of letting the server kill its connection.

describe('messageLimit', () => {
  test('the mux terminates a connection that ships an oversized frame', async () => {
    const mux = new ChannelMux()
    let terminated = false
    const transport: ServerTransport<{ id: string }> = {
      getSessionId: () => undefined,
      setSessionId: () => {},
      getConnId: () => null,
      sendNow: () => {},
      terminateConnection: () => {
        terminated = true
      },
    }
    const connection = { id: 'conn' }
    mux.onConnectionOpen(connection, transport)

    await mux.onConnectionRawMessage(connection, new Uint8Array(CHANNEL_MESSAGE_LIMIT_BYTES + 1))
    expect(terminated).toBe(true)
    // Permanent: the client must not resume this session — it's hostile or misconfigured.
    expect(mux.consumePermanentTermination(connection)).toBe(true)
    mux.dispose()
  })

  test('StreamReader rejects an oversized declared length without buffering the body', async () => {
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        // Declares a "gigabyte" frame, then trickles 1-byte chunks: without the declared-length
        // check, readExact(1GB) would pull forever — the immediate rejection is the proof.
        controller.enqueue(encodeU32(1024 * 1024 * 1024))
      },
    })
    const reader = new StreamReader(body, 1024)
    await expect(reader.readLengthPrefixedBytesOrNull()).rejects.toThrow('per-message limit')
  })

  test('StreamReader caps the metadata declaration the same way', async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encodeU32(1024 * 1024 * 1024))
        controller.close()
      },
    })
    const reader = new StreamReader(body, 1024)
    await expect(reader.readMetadata()).rejects.toThrow('per-message limit')
  })

  test('the client fails an oversized send locally, before the wire', () => {
    const channel = {
      id: crypto.randomUUID(),
      isClosed: false,
      _onTransportOpen() {},
      _dispatchFrame() {},
      _onTransportClose() {},
    }
    const fetchImpl = (async () =>
      new Response(new ReadableStream<Uint8Array>({ start() {} }), { status: 200 })) as unknown as typeof fetch
    const connection = ClientConnection.getOrCreate('http://test.local/_telefunc', channel as never, {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })
    expect(() => connection.send(channel as never, 'x'.repeat(CHANNEL_MESSAGE_LIMIT_BYTES + 1))).toThrow(
      'per-message limit',
    )
    connection.unregister(channel as never)
  })
})
