// Focused live-Redis schedules from the W2b independent review. These are intentionally outside the
// shared conformance modules: they exercise Redis-specific shared-connection, Lua-accounting and
// authority-time mechanisms while preserving the same public SPI outcomes.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  accepted,
  bytes,
  cellsOf,
  collector,
  CONTROL,
  deferred,
  enterClosing,
  finalizeClose,
  flush,
  nextId,
  okDeleted,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  settled,
  text,
} from '../../../telefunc/wire-protocol/backend/conformance/scenario.js'
import { REDIS_GENERATION_CAPTURE_TTL_MS } from './backend.js'
import { createRedisFixture, type RedisBackendFixture } from './fixture.js'
import { generationTokensKey, routeCapturesKey } from './layout.js'

const url = process.env.TELEFUNC_TEST_REAL_REDIS

describe.skipIf(url === undefined || url === '')('Redis independent-review race gates', () => {
  let fx: RedisBackendFixture

  beforeEach(async () => {
    fx = await createRedisFixture(url as string)
  })

  afterEach(async () => {
    vi.useRealTimers()
    await fx.dispose()
  })

  it('same-channel subscribers share the in-flight SUBSCRIBE ack before either becomes ready', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const entered = deferred()
    const release = deferred()
    let calls = 0
    fx.setBeforeSubscribe(async () => {
      calls++
      if (calls === 1) {
        entered.resolve()
        await release.promise
      }
    })

    const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await entered.promise
    const second = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    let firstReady = false
    let secondReady = false
    void first.ready.then(() => {
      firstReady = true
    })
    void second.ready.then(() => {
      secondReady = true
    })
    await flush()
    expect(firstReady).toBe(false)
    expect(secondReady).toBe(false)
    expect(calls).toBe(1)

    release.resolve()
    await Promise.all([first.ready, second.ready])
    expect(calls).toBe(1)
    expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-ready'))).receivers).toBe(1)
  })

  it('initial SUBSCRIBE failure rejects ready, retries exactly five times, then closes', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    let attempts = 0
    fx.setBeforeSubscribe(() => {
      attempts++
      throw new Error('forced subscribe failure')
    })

    const states: string[] = []
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    sub.onStateChange((state) => states.push(state))
    await expect(sub.ready).rejects.toThrow('forced subscribe failure')
    await waitFor(() => sub.state() === 'closed')
    expect(attempts).toBe(5)
    expect(states).toEqual(['lost', 'closed'])
  })

  it('subscriber transport loss emits lost, re-SUBSCRIBE ack emits ready, and delivery resumes', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const latch = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await sub.ready
    const states: string[] = []
    sub.onStateChange((state) => states.push(state))

    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await waitFor(() => states.includes('lost') && states.includes('ready'))
    expect(states.slice(0, 2)).toEqual(['lost', 'ready'])
    expect(sub.state()).toBe('ready')

    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-reconnect')))
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['after-reconnect'])
  })

  it('requires a current-connection SUBSCRIBE ack after disconnect during held generation validation', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const firstAckHeld = deferred()
    const releaseFirstValidation = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        firstAckHeld.resolve()
        await releaseFirstValidation.promise
      }
    })

    const latch = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    const states: string[] = []
    sub.onStateChange((state) => states.push(state))
    await firstAckHeld.promise

    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await waitFor(() => states.includes('lost'))
    await waitFor(() => fx.subscriber.status === 'ready')

    releaseFirstValidation.resolve()
    await waitFor(() => sub.state() === 'ready')
    expect(ackCount).toBe(2)
    await waitFor(
      async () => (await fx.redis.pubsub('NUMSUB', `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`))[1] === 1,
    )
    expect(states.slice(0, 2)).toEqual(['lost', 'ready'])

    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-race')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['after-race'])
  })

  it('starts a current-operation SUBSCRIBE when last detach overlaps a same-connection replacement', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    const firstAckHeld = deferred()
    const releaseFirstValidation = deferred()
    const uninstallEntered = deferred()
    const releaseUninstall = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        firstAckHeld.resolve()
        await releaseFirstValidation.promise
      }
    })

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const originalUnsubscribe = subscriber.unsubscribe.bind(fx.subscriber)
    let heldExactLane = false
    subscriber.unsubscribe = (...channels) => {
      const pending = originalUnsubscribe(...channels)
      if (!heldExactLane && channels.length === 1 && channels[0] === channel) {
        heldExactLane = true
        uninstallEntered.resolve()
        return releaseUninstall.promise.then(() => pending)
      }
      return pending
    }

    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await firstAckHeld.promise
    const oldDetach = old.unsubscribe()
    await uninstallEntered.promise

    const latch = collector()
    const replacement = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    releaseUninstall.resolve()
    await oldDetach
    subscriber.unsubscribe = originalUnsubscribe

    await waitFor(() => ackCount === 2)
    await replacement.ready
    releaseFirstValidation.resolve()
    expect(ackCount).toBe(2)
    await waitFor(async () => (await fx.redis.pubsub('NUMSUB', channel))[1] === 1)
    expect(replacement.state()).toBe('ready')
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('replacement')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['replacement'])
  })

  it('keeps a replacement behind cleanup when it attaches before the lane UNSUBSCRIBE is acknowledged', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    const firstAckHeld = deferred()
    const releaseFirstValidation = deferred()
    const uninstallCalled = deferred()
    const releaseUninstall = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        firstAckHeld.resolve()
        await releaseFirstValidation.promise
      }
    })

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const originalUnsubscribe = subscriber.unsubscribe.bind(fx.subscriber)
    let heldExactLane = false
    subscriber.unsubscribe = async (...channels) => {
      if (!heldExactLane && channels.length === 1 && channels[0] === channel) {
        heldExactLane = true
        uninstallCalled.resolve()
        await releaseUninstall.promise
      }
      return originalUnsubscribe(...channels)
    }

    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await firstAckHeld.promise
    const oldDetach = old.unsubscribe()
    await uninstallCalled.promise
    const latch = collector()
    const replacement = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)

    releaseFirstValidation.resolve()
    await flush()
    expect(replacement.state()).toBe('establishing')
    expect(ackCount).toBe(1)
    releaseUninstall.resolve()
    await oldDetach
    subscriber.unsubscribe = originalUnsubscribe

    await replacement.ready
    expect(ackCount).toBe(2)
    await waitFor(async () => (await fx.redis.pubsub('NUMSUB', channel))[1] === 1)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('before-ack')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['before-ack'])
  })

  it('starts the replacement after an acknowledged lane UNSUBSCRIBE whose caller is still held', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    const firstAckHeld = deferred()
    const releaseFirstValidation = deferred()
    const uninstallAcknowledged = deferred()
    const releaseUninstallCaller = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        firstAckHeld.resolve()
        await releaseFirstValidation.promise
      }
    })

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const originalUnsubscribe = subscriber.unsubscribe.bind(fx.subscriber)
    let heldExactLane = false
    subscriber.unsubscribe = async (...channels) => {
      const result = await originalUnsubscribe(...channels)
      if (!heldExactLane && channels.length === 1 && channels[0] === channel) {
        heldExactLane = true
        uninstallAcknowledged.resolve()
        await releaseUninstallCaller.promise
      }
      return result
    }

    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await firstAckHeld.promise
    const oldDetach = old.unsubscribe()
    await uninstallAcknowledged.promise
    const latch = collector()
    const replacement = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)

    releaseFirstValidation.resolve()
    await flush()
    expect(replacement.state()).toBe('establishing')
    releaseUninstallCaller.resolve()
    await oldDetach
    subscriber.unsubscribe = originalUnsubscribe

    await replacement.ready
    expect(ackCount).toBe(2)
    await waitFor(async () => (await fx.redis.pubsub('NUMSUB', channel))[1] === 1)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-ack')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['after-ack'])
  })

  it('multiplexes two late replacements behind one current-operation acknowledgement', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    const firstAckHeld = deferred()
    const releaseFirstValidation = deferred()
    const uninstallCalled = deferred()
    const releaseUninstall = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        firstAckHeld.resolve()
        await releaseFirstValidation.promise
      }
    })

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const originalUnsubscribe = subscriber.unsubscribe.bind(fx.subscriber)
    let heldExactLane = false
    subscriber.unsubscribe = async (...channels) => {
      if (!heldExactLane && channels.length === 1 && channels[0] === channel) {
        heldExactLane = true
        uninstallCalled.resolve()
        await releaseUninstall.promise
      }
      return originalUnsubscribe(...channels)
    }

    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await firstAckHeld.promise
    const oldDetach = old.unsubscribe()
    await uninstallCalled.promise
    const firstLatch = collector()
    const secondLatch = collector()
    const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, firstLatch.receiver)
    const second = fx.backend.subscribeLane(roomId, inc, SEMANTIC, secondLatch.receiver)

    releaseFirstValidation.resolve()
    releaseUninstall.resolve()
    await oldDetach
    subscriber.unsubscribe = originalUnsubscribe
    await Promise.all([first.ready, second.ready])

    expect(ackCount).toBe(2)
    await waitFor(async () => (await fx.redis.pubsub('NUMSUB', channel))[1] === 1)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two-late')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await Promise.all([firstLatch.waitFor(1), secondLatch.waitFor(1)])
    expect(firstLatch.payloads()).toEqual(['two-late'])
    expect(secondLatch.payloads()).toEqual(['two-late'])
  })

  it('does not let failed old cleanup retry uninstall a successor attachment', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    let ackCount = 0
    fx.setAfterSubscribeAck(() => {
      ackCount++
    })
    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await old.ready

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const originalUnsubscribe = subscriber.unsubscribe.bind(fx.subscriber)
    const cleanupCalled = deferred()
    const releaseFailure = deferred()
    let failedExactLane = false
    subscriber.unsubscribe = async (...channels) => {
      if (!failedExactLane && channels.length === 1 && channels[0] === channel) {
        failedExactLane = true
        cleanupCalled.resolve()
        await releaseFailure.promise
        throw new Error('forced old cleanup failure')
      }
      return originalUnsubscribe(...channels)
    }

    const oldDetach = old.unsubscribe()
    await cleanupCalled.promise
    const latch = collector()
    const successor = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    releaseFailure.resolve()
    await expect(oldDetach).rejects.toThrow('forced old cleanup failure')
    subscriber.unsubscribe = originalUnsubscribe

    await successor.ready
    expect(ackCount).toBe(2)
    await waitFor(async () => (await fx.redis.pubsub('NUMSUB', channel))[1] === 1)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-failure')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['after-failure'])
  })

  it('fences detach and replacement attempts across a simultaneous subscriber connection close', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    let ackCount = 0
    fx.setAfterSubscribeAck(() => {
      ackCount++
    })
    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await old.ready

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const originalUnsubscribe = subscriber.unsubscribe.bind(fx.subscriber)
    const cleanupCalled = deferred()
    const releaseCleanup = deferred()
    let heldExactLane = false
    subscriber.unsubscribe = async (...channels) => {
      if (!heldExactLane && channels.length === 1 && channels[0] === channel) {
        heldExactLane = true
        cleanupCalled.resolve()
        await releaseCleanup.promise
      }
      return originalUnsubscribe(...channels)
    }

    const oldDetach = old.unsubscribe()
    await cleanupCalled.promise
    const latch = collector()
    const replacement = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    expect(await settled(replacement.ready)).toBe('rejected')
    await waitFor(() => replacement.state() === 'lost')
    releaseCleanup.resolve()
    await oldDetach
    subscriber.unsubscribe = originalUnsubscribe

    await waitFor(() => replacement.state() === 'ready')
    expect(ackCount).toBe(2)
    await waitFor(async () => (await fx.redis.pubsub('NUMSUB', channel))[1] === 1)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-close')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['after-close'])
  })

  it('reuses a genuinely current operation acknowledgement without duplicate broker SUBSCRIBE or delivery', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const channel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    const ackHeld = deferred()
    const releaseAck = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        ackHeld.resolve()
        await releaseAck.promise
      }
    })

    const firstLatch = collector()
    const secondLatch = collector()
    const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, firstLatch.receiver)
    await ackHeld.promise
    const second = fx.backend.subscribeLane(roomId, inc, SEMANTIC, secondLatch.receiver)
    releaseAck.resolve()
    await Promise.all([first.ready, second.ready])

    expect(ackCount).toBe(1)
    expect((await fx.redis.pubsub('NUMSUB', channel))[1]).toBe(1)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one-ack')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await Promise.all([firstLatch.waitFor(1), secondLatch.waitFor(1)])
    expect(firstLatch.payloads()).toEqual(['one-ack'])
    expect(secondLatch.payloads()).toEqual(['one-ack'])
  })

  it('re-establishes every channel on the replacement shared subscriber connection', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const firstAcksHeld = deferred()
    const releaseFirstValidations = deferred()
    const ackCounts = new Map<string, number>()
    const heldChannels = new Set<string>()
    fx.setAfterSubscribeAck(async (channel) => {
      const count = (ackCounts.get(channel) ?? 0) + 1
      ackCounts.set(channel, count)
      if (count === 1) {
        heldChannels.add(channel)
        if (heldChannels.size === 2) firstAcksHeld.resolve()
        await releaseFirstValidations.promise
      }
    })

    const semanticLatch = collector()
    const controlLatch = collector()
    const semantic = fx.backend.subscribeLane(roomId, inc, SEMANTIC, semanticLatch.receiver)
    const control = fx.backend.subscribeLane(roomId, inc, CONTROL, controlLatch.receiver)
    const semanticStates: string[] = []
    const controlStates: string[] = []
    semantic.onStateChange((state) => semanticStates.push(state))
    control.onStateChange((state) => controlStates.push(state))
    await firstAcksHeld.promise

    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await waitFor(() => semanticStates.includes('lost') && controlStates.includes('lost'))
    await waitFor(() => fx.subscriber.status === 'ready')
    releaseFirstValidations.resolve()
    await waitFor(() => semantic.state() === 'ready' && control.state() === 'ready')

    expect([...ackCounts.values()].sort()).toEqual([2, 2])
    const semanticChannel = `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`
    const controlChannel = `${fx.prefix}room:{${roomId}}:ch:${inc}:control`
    await waitFor(async () => {
      const counts = await fx.redis.pubsub('NUMSUB', semanticChannel, controlChannel)
      return counts[1] === 1 && counts[3] === 1
    })

    const semanticResult = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('semantic')))
    const controlResult = accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('control')))
    expect([semanticResult.receivers, controlResult.receivers]).toEqual([1, 1])
    await Promise.all([semanticResult.delivery, controlResult.delivery])
    await Promise.all([semanticLatch.waitFor(1), controlLatch.waitFor(1)])
    expect(semanticLatch.payloads()).toEqual(['semantic'])
    expect(controlLatch.payloads()).toEqual(['control'])
  })

  it('keeps a late sibling behind the current ack when the old attachment detaches during stale validation', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const oldAckHeld = deferred()
    const currentAckHeld = deferred()
    const releaseOldValidation = deferred()
    const releaseCurrentValidation = deferred()
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      ackCount++
      if (ackCount === 1) {
        oldAckHeld.resolve()
        await releaseOldValidation.promise
      } else if (ackCount === 2) {
        currentAckHeld.resolve()
        await releaseCurrentValidation.promise
      }
    })

    const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await oldAckHeld.promise
    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await currentAckHeld.promise

    const latch = collector()
    const late = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await old.unsubscribe()
    expect(old.state()).toBe('closed')
    releaseOldValidation.resolve()
    await flush()
    expect(late.state()).toBe('establishing')

    releaseCurrentValidation.resolve()
    await late.ready
    expect(ackCount).toBe(2)
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('late-only')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['late-only'])
  })

  it('ignores completions from two stale connections before the third connection ack validates', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const entered = [deferred(), deferred(), deferred()]
    const release = [deferred(), deferred(), deferred()]
    let ackCount = 0
    fx.setAfterSubscribeAck(async () => {
      const index = ackCount++
      entered[index]?.resolve()
      await release[index]?.promise
    })

    const latch = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await entered[0]?.promise
    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await entered[1]?.promise

    const secondConnectionId = await onlyPubSubClientId(fx)
    await fx.redis.client('KILL', 'ID', String(secondConnectionId))
    await entered[2]?.promise
    release[0]?.resolve()
    release[1]?.resolve()
    await flush()
    expect(sub.state()).toBe('lost')

    release[2]?.resolve()
    await waitFor(() => sub.state() === 'ready')
    expect(ackCount).toBe(3)
    await waitFor(
      async () => (await fx.redis.pubsub('NUMSUB', `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`))[1] === 1,
    )
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('third-connection')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['third-connection'])
  })

  it('makes a late attachment await the current re-SUBSCRIBE ack instead of historical readiness', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await first.ready

    const entered = deferred()
    const release = deferred()
    let resubscribeCalls = 0
    fx.setBeforeSubscribe(async () => {
      resubscribeCalls++
      entered.resolve()
      await release.promise
    })
    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await entered.promise

    const late = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    let lateReady = false
    void late.ready.then(() => {
      lateReady = true
    })
    await flush()
    expect(first.state()).toBe('lost')
    expect(late.state()).toBe('establishing')
    expect(lateReady).toBe(false)
    expect(resubscribeCalls).toBe(1)

    release.resolve()
    await late.ready
    await waitFor(() => first.state() === 'ready')
    expect(lateReady).toBe(true)
    expect(resubscribeCalls).toBe(1)
  })

  it('reuses one durable attempt when the first generation-capture response is lost', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const attempts: Array<{ id: string; createdAt: number; token: string }> = []
    fx.setAfterGenerationCapture((info) => {
      attempts.push({ id: info.attemptId, createdAt: info.createdAt, token: info.token })
      if (attempts.length === 1) throw new Error('simulated lost capture response')
    })

    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    expect(await settled(sub.ready)).toBe('rejected')
    await waitFor(() => sub.state() === 'ready')
    expect(attempts).toHaveLength(2)
    expect(attempts[1]).toEqual(attempts[0])
    expect(await fx.backend.countGenerationCapturesForTest(roomId)).toBe(1)
  })

  it('reclaims an abandoned capture, rejects its delayed retry after reuse, and admits a fresh lifecycle', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const abandonedId = nextId('capture')
    const abandonedAt = fx.authorityNow()
    const oldToken = await fx.backend.captureGenerationAttemptForTest(roomId, inc, abandonedId, abandonedAt)
    expect(await fx.backend.countGenerationCapturesForTest(roomId)).toBe(1)

    fx.advanceAuthority(REDIS_GENERATION_CAPTURE_TTL_MS)
    const sweepId = nextId('capture')
    await fx.backend.captureGenerationAttemptForTest(roomId, inc, sweepId, fx.authorityNow())
    expect(await fx.backend.countGenerationCapturesForTest(roomId)).toBe(1)

    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
    await fx.backend.dropGeneration(roomId, inc)
    okHead(
      await fx.backend.compareExchangeHead(
        roomId,
        { expect: { rev: tombstone.rev } },
        { head: { currentInc: inc, state: 'open', config: tombstone.config } },
      ),
    )

    await expect(fx.backend.captureGenerationAttemptForTest(roomId, inc, abandonedId, abandonedAt)).rejects.toThrow(
      /absent or stale/,
    )
    const freshToken = await fx.backend.captureGenerationAttemptForTest(
      roomId,
      inc,
      nextId('capture'),
      fx.authorityNow(),
    )
    expect(freshToken).not.toBe(oldToken)
    const fresh = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await fresh.ready
    expect(fresh.state()).toBe('ready')
  })

  it('touches a capture only after successful exact SUBSCRIBE validation', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    let attemptId = ''
    let initialExpiry = 0
    fx.setAfterGenerationCapture(async (info) => {
      attemptId = info.attemptId
      const raw = await fx.redis.hget(routeCapturesKey(fx.prefix, roomId), attemptId)
      initialExpiry = (JSON.parse(raw as string) as { expiresAt: number }).expiresAt
    })
    fx.setBeforeSubscribe(() => {
      fx.advanceAuthority(1_000)
    })

    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await sub.ready
    const touchedRaw = await fx.redis.hget(routeCapturesKey(fx.prefix, roomId), attemptId)
    const touchedExpiry = (JSON.parse(touchedRaw as string) as { expiresAt: number }).expiresAt
    expect(touchedExpiry).toBe(initialExpiry + 1_000)
  })

  it('does not let a rejected stale SUBSCRIBE validation extend its capture', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    let rejectedAttempt = ''
    fx.setAfterGenerationCapture((info) => {
      rejectedAttempt = info.attemptId
    })
    fx.setBeforeSubscribe(() => {
      fx.advanceAuthority(REDIS_GENERATION_CAPTURE_TTL_MS)
    })

    const stale = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    expect(await settled(stale.ready)).toBe('rejected')
    await waitFor(() => stale.state() === 'closed')
    expect(await fx.redis.hexists(routeCapturesKey(fx.prefix, roomId), rejectedAttempt)).toBe(1)

    fx.setAfterGenerationCapture(null)
    fx.setBeforeSubscribe(null)
    const fresh = fx.backend.subscribeLane(roomId, inc, CONTROL, () => {})
    await fresh.ready
    expect(await fx.redis.hexists(routeCapturesKey(fx.prefix, roomId), rejectedAttempt)).toBe(0)
  })

  it('fences a held pre-SUBSCRIBE lifecycle across drop and legal incarnation reuse', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const entered = deferred()
    const release = deferred()
    fx.setBeforeSubscribe(async () => {
      entered.resolve()
      await release.promise
    })
    const staleLatch = collector()
    const stale = fx.backend.subscribeLane(roomId, inc, SEMANTIC, staleLatch.receiver)
    await entered.promise

    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
    await fx.backend.dropGeneration(roomId, inc)
    okHead(
      await fx.backend.compareExchangeHead(
        roomId,
        { expect: { rev: tombstone.rev } },
        { head: { currentInc: inc, state: 'open', config: tombstone.config } },
      ),
    )
    fx.setBeforeSubscribe(null)
    release.resolve()
    await waitFor(() => stale.state() === 'closed')

    const freshLatch = collector()
    const fresh = fx.backend.subscribeLane(roomId, inc, SEMANTIC, freshLatch.receiver)
    await fresh.ready
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('fresh-only')))
    expect(result.receivers).toBe(1)
    await result.delivery
    await freshLatch.waitFor(1)
    expect(staleLatch.payloads()).toEqual([])
    expect(freshLatch.payloads()).toEqual(['fresh-only'])
  })

  it('broadcasts generation invalidation across backend instances and removes the stale reused-id target', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const staleLatch = collector()
    const stale = fx.backend.subscribeLane(roomId, inc, SEMANTIC, staleLatch.receiver)
    await stale.ready
    const peer = await fx.createPeerBackend()
    try {
      const head = await readHeadOrThrow(peer.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(peer.backend, roomId, head)
      accepted(await peer.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(peer.backend, roomId, closing, leaseId))
      await peer.backend.dropGeneration(roomId, inc)
      okHead(
        await peer.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      )

      await waitFor(() => stale.state() === 'closed')
      await waitFor(
        async () => (await fx.redis.pubsub('NUMSUB', `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`))[1] === 0,
      )
      const withoutFresh = accepted(await peer.backend.commitLane(roomId, inc, SEMANTIC, bytes('not-for-stale')))
      expect(withoutFresh.receivers).toBe(0)
      expect(staleLatch.payloads()).toEqual([])

      const freshLatch = collector()
      const fresh = fx.backend.subscribeLane(roomId, inc, SEMANTIC, freshLatch.receiver)
      await fresh.ready
      const withFresh = accepted(await peer.backend.commitLane(roomId, inc, SEMANTIC, bytes('fresh-peer')))
      expect(withFresh.receivers).toBe(1)
      await withFresh.delivery
      await freshLatch.waitFor(1)
      expect(freshLatch.payloads()).toEqual(['fresh-peer'])
    } finally {
      await peer.dispose()
    }
  })

  it('fences last detach against an in-flight establishment validation', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const entered = deferred()
    const release = deferred()
    fx.setAfterSubscribeAck(async () => {
      entered.resolve()
      await release.promise
    })
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await entered.promise
    await sub.unsubscribe()
    expect(sub.state()).toBe('closed')
    await waitFor(
      async () => (await fx.redis.pubsub('NUMSUB', `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`))[1] === 0,
    )
    release.resolve()
    await flush()
    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('nobody')))
    expect(result.receivers).toBe(0)
  })

  it('terminates a disconnected lifecycle that misses invalidation before same-id reuse', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const stale = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await stale.ready
    const peer = await fx.createPeerBackend()
    const entered = deferred()
    const release = deferred()
    fx.setBeforeSubscribe(async () => {
      entered.resolve()
      await release.promise
    })
    try {
      await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
      await entered.promise
      const head = await readHeadOrThrow(peer.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(peer.backend, roomId, head)
      accepted(await peer.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(peer.backend, roomId, closing, leaseId))
      await peer.backend.dropGeneration(roomId, inc)
      okHead(
        await peer.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      )

      release.resolve()
      await waitFor(() => stale.state() === 'closed')
      expect(accepted(await peer.backend.commitLane(roomId, inc, SEMANTIC, bytes('no-stale'))).receivers).toBe(0)

      fx.setBeforeSubscribe(null)
      const fresh = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
      await fresh.ready
      expect(accepted(await peer.backend.commitLane(roomId, inc, SEMANTIC, bytes('fresh'))).receivers).toBe(1)
    } finally {
      release.resolve()
      await peer.dispose()
    }
  })

  it('closes locally on unsubscribe transport failure and retries the retained exact channel cleanup', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await sub.ready
    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const original = subscriber.unsubscribe.bind(fx.subscriber)
    let failed = false
    subscriber.unsubscribe = (...channels) => {
      if (!failed) {
        failed = true
        return Promise.reject(new Error('forced unsubscribe transport failure'))
      }
      return original(...channels)
    }

    await expect(sub.unsubscribe()).rejects.toThrow('forced unsubscribe transport failure')
    expect(sub.state()).toBe('closed')
    await waitFor(
      async () => (await fx.redis.pubsub('NUMSUB', `${fx.prefix}room:{${roomId}}:ch:${inc}:semantic`))[1] === 0,
    )
    expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-detach'))).receivers).toBe(0)
    subscriber.unsubscribe = original
  })

  it('isolates a throwing state observer from last-detach teardown', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await sub.ready
    sub.onStateChange(() => {
      throw new Error('observer failure')
    })
    await sub.unsubscribe()
    expect(sub.state()).toBe('closed')
    expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-observer'))).receivers).toBe(0)
  })

  it('keeps gens registered through physical drop, blocking same-inc recreation until the final SREM', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const cells = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' }))
    expect(
      await fx.backend.compareExchangeCells(roomId, inc, cells.revision, [{ key: 'old', set: { bytes: bytes('x') } }]),
    ).toBe('committed')
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

    const reachedBoundary = deferred()
    const release = deferred()
    fx.setBeforeDropGenerationUnregister(async () => {
      reachedBoundary.resolve()
      await release.promise
    })
    const dropping = fx.backend.dropGeneration(roomId, inc)
    await reachedBoundary.promise
    try {
      await expect(
        fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      ).rejects.toThrow(/surviving generation state/)
    } finally {
      release.resolve()
      await dropping
    }
    const recreated = okHead(
      await fx.backend.compareExchangeHead(
        roomId,
        { expect: { rev: tombstone.rev } },
        { head: { currentInc: inc, state: 'open', config: tombstone.config } },
      ),
    )
    expect(recreated.currentInc).toBe(inc)
    expect(cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' })).cells.size).toBe(0)
  })

  it('removes durable generation sources last and isolates one failed channel uninstall', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const semantic = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    const control = fx.backend.subscribeLane(roomId, inc, CONTROL, () => {})
    await Promise.all([semantic.ready, control.ready])
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

    const subscriber = fx.subscriber as unknown as {
      unsubscribe: (...channels: string[]) => Promise<number>
    }
    const original = subscriber.unsubscribe.bind(fx.subscriber)
    const calls: string[] = []
    let failedSemantic = false
    subscriber.unsubscribe = (...channels) => {
      calls.push(...channels)
      if (!failedSemantic && channels.some((channel) => channel.endsWith(':semantic'))) {
        failedSemantic = true
        return Promise.reject(new Error('forced one-channel uninstall failure'))
      }
      return original(...channels)
    }

    await expect(fx.backend.dropGeneration(roomId, inc)).rejects.toThrow('forced one-channel uninstall failure')
    expect(calls.some((channel) => channel.endsWith(':semantic'))).toBe(true)
    expect(calls.some((channel) => channel.endsWith(':control'))).toBe(true)
    expect(await fx.backend.listGenerations(roomId)).toContain(inc)
    expect(await fx.redis.hget(generationTokensKey(fx.prefix, roomId), inc)).not.toBeNull()
    expect(semantic.state()).toBe('closed')
    expect(control.state()).toBe('closed')

    subscriber.unsubscribe = original
    await fx.backend.dropGeneration(roomId, inc)
    expect(await fx.backend.listGenerations(roomId)).not.toContain(inc)
    expect(await fx.redis.hget(generationTokensKey(fx.prefix, roomId), inc)).toBeNull()
  })

  it('enforces the aggregate retained payload cap inside concurrent COMMIT records', async () => {
    await fx.dispose()
    fx = await createRedisFixture(url as string, { maxRetainedPayloadBytes: 100 })
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)

    const results = await Promise.allSettled([
      fx.backend.commitLane(roomId, inc, SEMANTIC, new Uint8Array(60), { retain: true }),
      fx.backend.commitLane(roomId, inc, CONTROL, new Uint8Array(60), { retain: true }),
    ])
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1)
    expect(await fx.backend.listRetained(roomId, inc)).toHaveLength(1)

    await fx.backend.deleteRetained(roomId, inc)
    accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, new Uint8Array(100), { retain: true }))
    // Replacement subtracts only the old payload (not its 12-byte frame header), so the total remains 100.
    accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, new Uint8Array(100), { retain: true }))
    await expect(fx.backend.commitLane(roomId, inc, CONTROL, new Uint8Array(1), { retain: true })).rejects.toThrow(
      /retained aggregate/,
    )
  })

  it('uses Redis TIME for production tombstone and logical cell-expiry reads under caller skew', async () => {
    await fx.dispose()
    fx = await createRedisFixture(url as string, { useRedisAuthority: true })

    const cellRoom = nextId('room')
    const { inc: cellInc } = await openRoom(fx.backend, cellRoom)
    const read = cellsOf(await fx.backend.readCells(cellRoom, cellInc, { prefix: '' }))
    expect(
      await fx.backend.compareExchangeCells(cellRoom, cellInc, read.revision, [
        { key: 'ttl', set: { bytes: bytes('live'), ttlMs: 60_000 } },
      ]),
    ).toBe('committed')

    const tombRoom = nextId('room')
    const { inc: tombInc, head } = await openRoom(fx.backend, tombRoom)
    const { head: closing, leaseId } = await enterClosing(fx.backend, tombRoom, head)
    accepted(await fx.backend.commitLane(tombRoom, tombInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    okHead(await finalizeClose(fx.backend, tombRoom, closing, leaseId))

    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(Date.now() + 10 * 365 * 24 * 60 * 60 * 1_000)
    expect(
      text(cellsOf(await fx.backend.readCells(cellRoom, cellInc, { prefix: '' })).cells.get('ttl') as Uint8Array),
    ).toBe('live')
    expect((await fx.backend.readHead(tombRoom))?.head.state).toBe('closed')
  })

  it('keys subscription ownership by room and incarnation, so a same-inc peer room survives drop', async () => {
    const sharedInc = nextId('shared-inc')
    const roomA = nextId('room')
    const roomB = nextId('room')
    const headA = okHead(
      await fx.backend.compareExchangeHead(
        roomA,
        { expect: 'absent' },
        {
          head: { currentInc: sharedInc, state: 'open', config: bytes('a') },
        },
      ),
    )
    okHead(
      await fx.backend.compareExchangeHead(
        roomB,
        { expect: 'absent' },
        {
          head: { currentInc: sharedInc, state: 'open', config: bytes('b') },
        },
      ),
    )
    const latch = collector()
    const peer = fx.backend.subscribeLane(roomB, sharedInc, SEMANTIC, latch.receiver)
    await peer.ready

    const { head: closing, leaseId } = await enterClosing(fx.backend, roomA, headA)
    accepted(await fx.backend.commitLane(roomA, sharedInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    okHead(await finalizeClose(fx.backend, roomA, closing, leaseId))
    await fx.backend.dropGeneration(roomA, sharedInc)

    expect(peer.state()).toBe('ready')
    const result = accepted(await fx.backend.commitLane(roomB, sharedInc, SEMANTIC, bytes('still-live')))
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['still-live'])
  })

  it('applies the selected HeadCx predicate before deleting a closed tombstone', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

    const guarded = await fx.backend.compareExchangeHead(
      roomId,
      { expect: { rev: tombstone.rev, closingLease: leaseId } },
      { delete: true },
    )
    expect(guarded).toHaveProperty('conflict', true)
    expect((await fx.backend.readHead(roomId))?.head.state).toBe('closed')
    okDeleted(await fx.backend.compareExchangeHead(roomId, { expect: { rev: tombstone.rev } }, { delete: true }))
  })

  it('keeps a concurrent newer directory tag under stale compare-delete cleanup', async () => {
    const roomId = nextId('dir')
    await fx.backend.directoryPut(roomId, 'inc-old')
    const reachedApply = deferred()
    const release = deferred()
    fx.setBeforeDirectoryDeleteApply(async () => {
      reachedApply.resolve()
      await release.promise
    })
    const staleDelete = fx.backend.directoryDelete(roomId, 'inc-old')
    await reachedApply.promise
    await fx.backend.directoryPut(roomId, 'inc-new')
    release.resolve()
    await staleDelete
    expect((await fx.backend.directoryList(roomId)).entries).toEqual([{ roomId, incTag: 'inc-new' }])
  })

  it('rejects only the affected delivery when the ratified local PING flush fails', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const latch = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await sub.ready
    const subscriber = fx.subscriber as unknown as { ping: () => Promise<string> }
    const originalPing = subscriber.ping
    subscriber.ping = () => Promise.reject(new Error('forced PING transport failure'))
    const failed = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('accepted')))
    await expect(failed.delivery).rejects.toThrow('forced PING transport failure')
    subscriber.ping = originalPing

    const next = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('next')))
    await next.delivery
    await latch.waitFor(2)
    expect(latch.payloads()).toEqual(['accepted', 'next'])
  })
})

async function onlyPubSubClientId(fx: RedisBackendFixture): Promise<number> {
  const redis = fx.redis as unknown as { client(command: string, ...args: string[]): Promise<unknown> }
  const list = String(await redis.client('LIST', 'TYPE', 'pubsub'))
  const ids = [...list.matchAll(/(?:^|\n)id=(\d+)\b/g)].map((match) => Number(match[1]))
  if (ids.length !== 1) throw new Error(`expected one Redis pub/sub client, found ${ids.length}`)
  return ids[0] as number
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${timeoutMs} ms`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}
