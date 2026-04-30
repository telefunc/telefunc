import { describe, expect, it } from 'vitest'
import {
  decodeProxyEnvelope,
  encodeProxyEnvelope,
  ENVELOPE_KIND,
  InMemoryChannelSubstrate,
  PROXY_DIRECTION,
  type ProxyEnvelope,
} from './substrate.js'

describe('proxy envelope codec', () => {
  it('round-trips an attach envelope with the timeout, ix, and lastSeq intact', () => {
    const envelope: ProxyEnvelope = {
      channelId: 'room:abc',
      fromInstance: 'instance-A',
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 12_345, ix: 7, lastSeq: 42 },
    }

    const decoded = decodeProxyEnvelope(encodeProxyEnvelope(envelope))

    expect(decoded).toEqual(envelope)
  })

  it('round-trips a detach envelope', () => {
    const envelope: ProxyEnvelope = {
      channelId: 'room:abc',
      fromInstance: 'instance-A',
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.DETACH, reason: 'transient' },
    }

    const decoded = decodeProxyEnvelope(encodeProxyEnvelope(envelope))

    expect(decoded).toEqual(envelope)
  })

  it('round-trips a frame envelope, preserving binary bytes', () => {
    const frame = new Uint8Array([0x01, 0x0a, 0x02, 0x0a, 0x0a, 0xff])
    const envelope: ProxyEnvelope = {
      channelId: 'room:bin',
      fromInstance: 'instance-B',
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.FRAME, frame },
    }

    const decoded = decodeProxyEnvelope(encodeProxyEnvelope(envelope))

    expect(decoded.channelId).toBe('room:bin')
    expect(decoded.fromInstance).toBe('instance-B')
    expect(decoded.direction).toBe(PROXY_DIRECTION.TO_PEER)
    expect(decoded.payload.kind).toBe(ENVELOPE_KIND.FRAME)
    if (decoded.payload.kind !== ENVELOPE_KIND.FRAME) throw new Error('Expected frame payload')
    expect(Array.from(decoded.payload.frame)).toEqual(Array.from(frame))
  })
})

describe('InMemoryChannelSubstrate', () => {
  it('locateRemoteHome always resolves null — single-process has no remote', async () => {
    const substrate = new InMemoryChannelSubstrate()
    await substrate.pinChannel('room:foo')

    expect(await substrate.locateRemoteHome('room:foo', 50)).toBe(null)
  })

  it('forward and listen are no-ops — single-process has nothing to forward', async () => {
    const substrate = new InMemoryChannelSubstrate()
    const received: ProxyEnvelope[] = []
    substrate.listen({ onAttach: (env) => received.push(env) })

    await substrate.forward('any-target', {
      channelId: 'room:foo',
      fromInstance: substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 1000, ix: 0, lastSeq: 0 },
    })

    expect(received).toEqual([])
  })
})
