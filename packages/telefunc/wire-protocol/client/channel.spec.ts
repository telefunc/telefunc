import { afterEach, describe, expect, test, vi } from 'vitest'

import { ClientBroadcast } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { ACK_STATUS, TAG, type AckResultStatus } from '../shared-ws.js'
import { ChannelOverflowError } from '../channel-errors.js'

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
