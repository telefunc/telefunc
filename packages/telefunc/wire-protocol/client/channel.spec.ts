import { afterEach, describe, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { ACK_STATUS, TAG, type AckResultStatus } from '../shared-ws.js'
import { ChannelOverflowError } from '../channel-errors.js'
import { getSessionToken } from './session-registry.js'

const broadcasts: ClientBroadcast[] = []
afterEach(() => {
  for (const broadcast of broadcasts.splice(0)) broadcast.abort()
  delete config.fetch
  vi.restoreAllMocks()
})

/** A ClientBroadcast whose wire never opens, so each publish waits for the ack a test hands it. */
function stalledBroadcast(): ClientBroadcast {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const broadcast = new ClientBroadcast({
    channelId: crypto.randomUUID(),
    key: 'client-broadcast',
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://client-broadcast.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  broadcasts.push(broadcast)
  return broadcast
}

function publishThatSettlesWith(status: AckResultStatus, binary: boolean) {
  const broadcast = stalledBroadcast()
  const publishing = binary ? broadcast.publishBinary(new Uint8Array([1])) : broadcast.publish('message')
  const text = status === ACK_STATUS.ABORT ? JSON.stringify('expected') : 'unexpected publish bug'
  broadcast._dispatchFrame({ tag: TAG.ACK_RES, index: 0, seq: 1, ackedSeq: 1, status, text })
  return publishing
}

test("a subscriber that unsubscribes itself doesn't make the next one miss the message", () => {
  const broadcast = stalledBroadcast()
  const seen: string[] = []
  const off = broadcast.subscribe((message) => {
    seen.push(`once:${String(message)}`)
    off()
  })
  broadcast.subscribe((message) => void seen.push(`other:${String(message)}`))
  for (const [seq, text] of [
    [1, 'one'],
    [2, 'two'],
  ] as const)
    broadcast._dispatchFrame({
      tag: TAG.PUBLISH,
      index: 0,
      seq,
      text: JSON.stringify(text),
      info: { seq, timestamp: 1 },
    })
  expect(seen).toEqual(['once:one', 'other:one', 'other:two'])
})

describe.each([
  ['text', false],
  ['binary', true],
] as const)('ClientBroadcast %s', (_name, binary) => {
  test('reports an unexpected publish error through the client bug pipeline', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await expect(publishThatSettlesWith(ACK_STATUS.ERROR, binary)).rejects.toThrow('unexpected publish bug')
    expect(report).toHaveBeenCalledOnce()
    expect(report.mock.calls[0]?.[0]).toBe('[telefunc:channel-error]')
    expect(report.mock.calls[0]?.[1]).toMatchObject({ message: 'unexpected publish bug' })
  })

  test('keeps an expected Abort quiet', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await expect(publishThatSettlesWith(ACK_STATUS.ABORT, binary)).rejects.toMatchObject({ abortValue: 'expected' })
    expect(report).not.toHaveBeenCalled()
  })

  test('keeps a refused publish quiet, as a ChannelOverflowError', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await expect(publishThatSettlesWith(ACK_STATUS.OVERFLOW, binary)).rejects.toBeInstanceOf(ChannelOverflowError)
    expect(report).not.toHaveBeenCalled()
  })

  test('keeps a shield validation failure quiet', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await expect(publishThatSettlesWith(ACK_STATUS.SHIELD_ERROR, binary)).rejects.toMatchObject({
      name: 'ShieldValidationError',
    })
    expect(report).not.toHaveBeenCalled()
  })

  test('reports a rejected subscriber promise through the client bug pipeline', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const broadcast = stalledBroadcast()
    const rejected = () => Promise.reject(new Error('subscriber rejected'))
    const info = { seq: 1, timestamp: 1 }
    if (binary) {
      broadcast.subscribeBinary(rejected)
      broadcast._dispatchFrame({ tag: TAG.PUBLISH_BINARY, index: 0, seq: 1, data: new Uint8Array(), info })
    } else {
      broadcast.subscribe(rejected)
      broadcast._dispatchFrame({ tag: TAG.PUBLISH, index: 0, seq: 1, text: 'null', info })
    }
    await vi.waitFor(() => expect(report).toHaveBeenCalledOnce())
  })
})

test('a channel made before the page has a session token makes one, so the call that carries it presents the same', () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const telefuncUrl = 'http://first-call.test/_telefunc'
  expect(getSessionToken(telefuncUrl)).toBeUndefined()
  broadcasts.push(
    new ClientBroadcast({
      channelId: crypto.randomUUID(),
      key: 'first-call',
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl,
      connectionKey: crypto.randomUUID(),
    }),
  )
  expect(getSessionToken(telefuncUrl)).toEqual(expect.any(String))
})

test('a close request goes out again when its channel re-attaches before the close is acknowledged', () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://close-resend.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const sendCloseRequest = vi.spyOn((channel as any)._connection, 'sendCloseRequest')
  void channel.close({ timeout: 5_000 })
  channel._onTransportOpen(false) // the reconcile of a reconnect: the first request may have died with the old wire
  expect(sendCloseRequest).toHaveBeenCalledTimes(2)
})

test('a close the server acknowledged ends gracefully, though a reconnect then drops the channel', async () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: [CHANNEL_TRANSPORT.SSE],
    telefuncUrl: 'http://close-acked.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const closedWith: unknown[] = []
  channel.onClose((err) => void closedWith.push(err))
  const closing = channel.close({ timeout: 5_000 })
  channel._onTransportCloseAck()
  // An upgrade's RECONCILED, applied in the same turn, no longer lists the channel the server closed.
  channel._onTransportClose(new Error('Channel not acknowledged by server after reconnect'))
  expect(await closing).toBe(0)
  expect(closedWith).toEqual([undefined])
})
