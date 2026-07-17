import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { executeTelefunction } from './executeTelefunction.js'
import { Abort } from '../Abort.js'
import { createRequestContext } from '../context/requestContext.js'
import { REQUEST_CONTEXT } from '../context/requestContext.js'
import { getRawContext } from '../context/context.js'
import { getServerConfig } from '../serverConfig.js'
import type { TelefuncServerExtension } from '../extensions.js'
import { invalidateTag } from '../live/tags.js'
import { _resetTagHubsForTesting } from '../live/tagHub.js'
import { parse } from '@brillout/json-serializer/parse'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../../wire-protocol/server/broadcast.js'

let calls: string[]
let previousAdapter: ReturnType<typeof getBroadcastAdapter>

beforeEach(() => {
  calls = []
  previousAdapter = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter())
  _resetTagHubsForTesting()
})
afterEach(() => _resetBroadcastAdapterForTesting(previousAdapter))

type Hooks = { onResult?: (ctx: { result: unknown; data?: Record<string, unknown> }) => unknown }

/** An extension with only the pre-PR `onTelefunctionResult` hook (START/SETTLED were removed). */
function ext(name: string, h: Hooks = {}): TelefuncServerExtension {
  return {
    name,
    hooks: {
      onTelefunctionResult: (ctx) => {
        calls.push(`${name}.result`)
        return h.onResult ? h.onResult(ctx) : ctx.result
      },
    },
  }
}

function run(
  extensions: TelefuncServerExtension[],
  body: () => unknown,
  requestExtensions: Record<string, Record<string, unknown>> = {},
) {
  const request = new Request('http://x')
  const requestContext = createRequestContext(request)
  const context = { [REQUEST_CONTEXT]: requestContext }
  return executeTelefunction({
    telefunction: body as never,
    telefunctionName: 'testFn',
    telefuncFilePath: '/test.telefunc.ts',
    telefunctionArgs: [],
    context,
    requestContext,
    request,
    requestExtensions,
    serverConfig: { ...getServerConfig(), extensions } as ReturnType<typeof getServerConfig>,
  })
}

const okBody = async () => 'r0'

/** Parsed tag batches from an adapter `publish` spy (excludes readiness-barrier frames). */
function tagBatchesFrom(spy: ReturnType<typeof vi.spyOn>): { tags: string[] }[] {
  return spy.mock.calls
    .filter((c) => typeof c[1] === 'string' && (c[1] as string).includes('"tags"'))
    .map((c) => parse(c[1] as string) as { tags: string[] })
}

describe('lifecycle — result transform chain (§3.A)', () => {
  it('result hooks run in registration order and thread the prior result', async () => {
    let e2Received: unknown
    const res = await run(
      [
        ext('E1', { onResult: () => 'r1' }),
        ext('E2', {
          onResult: (c) => {
            e2Received = c.result
            return 'r2'
          },
        }),
      ],
      okBody,
    )
    expect(calls).toEqual(['E1.result', 'E2.result'])
    expect(e2Received).toBe('r1') // each hook receives the previous hook's returned result
    expect(res.telefunctionReturn).toBe('r2')
    expect(res.telefunctionHasErrored).toBe(false)
  })

  it('a result-hook throw → outcome errors, result discarded, later hooks still run on the last-good value', async () => {
    const boom = new Error('result-fail')
    let e3Received: unknown
    const res = await run(
      [
        ext('E1', { onResult: () => 'r1' }),
        ext('E2', {
          onResult: () => {
            throw boom
          },
        }),
        ext('E3', {
          onResult: (c) => {
            e3Received = c.result
            return 'r3'
          },
        }),
      ],
      okBody,
    )
    expect(e3Received).toBe('r1') // the last SUCCESSFUL value, not r0 or E2's output
    expect(res.telefunctionHasErrored).toBe(true)
    expect(res.telefunctionTopLevelError).toBe(boom)
    expect(calls).toEqual(['E1.result', 'E2.result', 'E3.result']) // all attempted
  })

  it('result-hook `data` is present only when the request carried it; the hook still runs otherwise', async () => {
    const seen: Record<string, unknown[]> = { E1: [], F: [] }
    await run(
      [
        ext('E1', {
          onResult: (c) => {
            seen.E1!.push(c.data)
            return c.result
          },
        }),
        ext('F', {
          onResult: (c) => {
            seen.F!.push(c.data)
            return c.result
          },
        }),
      ],
      okBody,
      { E1: { q: 1 } },
    )
    expect(seen.E1).toEqual([{ q: 1 }])
    expect(seen.F).toEqual([undefined]) // no request data for F, but its hook still ran
  })

  it('the result hook observes the request context', async () => {
    const seen: boolean[] = []
    await run(
      [
        ext('E1', {
          onResult: (c) => {
            seen.push(getRawContext() !== null)
            return c.result
          },
        }),
      ],
      okBody,
    )
    expect(seen).toEqual([true])
  })
})

describe('lifecycle — body outcome + core settle on every path (§3.A)', () => {
  it('body error: RESULT skipped, outcome errors, original error propagates', async () => {
    const boom = new Error('boom')
    const res = await run([ext('E1'), ext('E2')], async () => {
      throw boom
    })
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0) // result skipped on error
    expect(res.telefunctionHasErrored).toBe(true)
    expect(res.telefunctionTopLevelError).toBe(boom)
  })

  it('body abort: outcome aborts, abortValue preserved, RESULT skipped', async () => {
    const res = await run([ext('E1')], async () => {
      throw Abort('v')
    })
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0)
    expect(res.telefunctionAborted).toBe(true)
    expect(res.telefunctionReturn).toBe('v')
  })

  it('a non-async return is a usage error, RECORDED (not thrown) so settle still runs', async () => {
    const res = await run([ext('E1')], (() => 'sync') as never)
    expect(res.telefunctionHasErrored).toBe(true)
  })

  it('core settleLiveState publishes a body-queued tag on SUCCESS', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await run([ext('E1')], async () => {
      invalidateTag('z')
      return 'r'
    })
    const batches = tagBatchesFrom(publishSpy)
    expect(batches).toHaveLength(1)
    expect(batches[0]!.tags).toContain('z')
  })

  it('core settleLiveState runs on EVERY path — a body-queued tag publishes even when the body ERRORS', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await run([ext('E1')], async () => {
      invalidateTag('z')
      throw new Error('boom')
    })
    const batches = tagBatchesFrom(publishSpy)
    expect(batches).toHaveLength(1) // settle is not skipped by the body error
    expect(batches[0]!.tags).toContain('z')
  })
})

describe('forged / legacy request-extension data is inert (§3.B)', () => {
  it('request data for an unregistered extension invokes nothing', async () => {
    const res = await run([ext('E1')], okBody, { 'not-registered': { anything: true } })
    expect(calls).toEqual(['E1.result'])
    expect(res.telefunctionHasErrored).toBe(false)
  })

  it('forged live data triggers no tag publish', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await run([ext('E1')], okBody, { '@telefunc/live': { queryKey: ['x'], invalidates: [['y']] } })
    expect(tagBatchesFrom(publishSpy)).toHaveLength(0) // barrier frames are not tag batches
  })
})
