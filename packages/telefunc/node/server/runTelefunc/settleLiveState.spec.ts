import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { settleLiveState } from './settleLiveState.js'
import { executeTelefunction } from './executeTelefunction.js'
import { addLiveSource, takeLiveSources } from '../live/source.js'
import type { LiveSource } from '../live/source.js'
import { Abort } from '../Abort.js'
import { restoreContext } from '../context/context.js'
import { createRequestContext, REQUEST_CONTEXT } from '../context/requestContext.js'
import { getServerConfig } from '../serverConfig.js'
import { _resetTagHubsForTesting } from '../live/tagHub.js'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../../wire-protocol/server/broadcast.js'

let previousAdapter: ReturnType<typeof getBroadcastAdapter>
beforeEach(() => {
  previousAdapter = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter())
  _resetTagHubsForTesting()
})
afterEach(() => _resetBroadcastAdapterForTesting(previousAdapter))

function source(identity: string, dispose: () => void): LiveSource {
  return { identity, subscribe: () => () => {}, dispose }
}

/** Drive executeTelefunction with a body that registers one leftover live source, so settleLiveState
 *  disposes it on whichever settle path the body takes. */
function runBody(body: () => unknown, dispose: () => void) {
  const request = new Request('http://x')
  const requestContext = createRequestContext(request)
  const context = { [REQUEST_CONTEXT]: requestContext }
  const wrapped = async () => {
    addLiveSource(source('leftover', dispose))
    return body()
  }
  return executeTelefunction({
    telefunction: wrapped as never,
    telefunctionName: 'testFn',
    telefuncFilePath: '/test.telefunc.ts',
    telefunctionArgs: [],
    context,
    requestContext,
    request,
    requestExtensions: {},
    serverConfig: { ...getServerConfig(), extensions: [] } as ReturnType<typeof getServerConfig>,
  })
}

describe('settleLiveState (§3.D)', () => {
  it('T1.D3 disposes a never-subscribed leftover; leaves a taken source alone', () =>
    restoreContext({}, async () => {
      const dispose = vi.fn()
      addLiveSource(source('a', dispose))
      await settleLiveState()
      expect(dispose).toHaveBeenCalledTimes(1)
    }).then(() =>
      restoreContext({}, async () => {
        const dispose = vi.fn()
        addLiveSource(source('a', dispose))
        takeLiveSources() // a result hook took ownership
        await settleLiveState()
        expect(dispose).not.toHaveBeenCalled()
      }),
    ))

  it('T1.D4 disposes leftover on the error path', async () => {
    const dispose = vi.fn()
    await runBody(async () => {
      throw new Error('boom')
    }, dispose)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  it('T1.D5 disposes leftover on the abort path', async () => {
    const dispose = vi.fn()
    await runBody(async () => {
      throw Abort('v')
    }, dispose)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  it('T1.D6 disposes leftover on the success path', async () => {
    const dispose = vi.fn()
    await runBody(async () => 'ok', dispose)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  it('T1.D4 a PRIMITIVE body rejection still reaches settle (disposes leftover)', async () => {
    const dispose = vi.fn()
    const res = await runBody(async () => {
      throw 'boom' // primitive — validateTelefunctionError must not escape before settle
    }, dispose)
    expect(dispose).toHaveBeenCalledTimes(1)
    expect(res.telefunctionHasErrored).toBe(true)
  })

  it('T1.D6 a non-async body (sync primitive return) still reaches settle', async () => {
    const dispose = vi.fn()
    const request = new Request('http://x')
    const requestContext = createRequestContext(request)
    const context = { [REQUEST_CONTEXT]: requestContext }
    // A sync telefunction that returns a non-promise — the return-type assertion must not escape.
    const res = await executeTelefunction({
      telefunction: (() => {
        addLiveSource(source('leftover', dispose))
        return 42
      }) as never,
      telefunctionName: 'testFn',
      telefuncFilePath: '/test.telefunc.ts',
      telefunctionArgs: [],
      context,
      requestContext,
      request,
      requestExtensions: {},
      serverConfig: { ...getServerConfig(), extensions: [] } as ReturnType<typeof getServerConfig>,
    })
    expect(dispose).toHaveBeenCalledTimes(1)
    expect(res.telefunctionHasErrored).toBe(true)
  })
})
