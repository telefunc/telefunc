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

type Hooks = {
  onStart?: (ctx: { data?: Record<string, unknown> }) => void
  onResult?: (ctx: { result: unknown; data?: Record<string, unknown> }) => unknown
  onSettled?: (ctx: { outcome: 'success' | 'error' | 'abort'; error?: unknown; data?: Record<string, unknown> }) => void
}

function ext(name: string, h: Hooks = {}): TelefuncServerExtension {
  return {
    name,
    hooks: {
      onTelefunctionStart: (ctx) => {
        calls.push(`${name}.start`)
        h.onStart?.(ctx)
      },
      onTelefunctionResult: (ctx) => {
        calls.push(`${name}.result`)
        return h.onResult ? h.onResult(ctx) : ctx.result
      },
      onTelefunctionSettled: (ctx) => {
        calls.push(`${name}.settled`)
        h.onSettled?.(ctx)
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

describe('lifecycle state machine (§3.A)', () => {
  it('T1.A1 success path — cross-extension phase order', async () => {
    const settledOutcomes: string[] = []
    const capture = (ctx: { outcome: string }) => settledOutcomes.push(ctx.outcome)
    await run([ext('E1', { onSettled: capture }), ext('E2', { onSettled: capture })], okBody)
    expect(calls).toEqual(['E1.start', 'E2.start', 'E1.result', 'E2.result', 'E1.settled', 'E2.settled'])
    expect(settledOutcomes).toEqual(['success', 'success'])
  })

  it('T1.A2 result hook is success-only (body error and abort)', async () => {
    await run([ext('E1'), ext('E2')], async () => {
      throw new Error('boom')
    })
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0)
    calls = []
    await run([ext('E1'), ext('E2')], async () => {
      throw Abort('v')
    })
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0)
  })

  it('T1.A3 settled runs on body error for all extensions; original error propagates', async () => {
    const boom = new Error('boom')
    const errors: unknown[] = []
    const res = await run(
      [ext('E1', { onSettled: (c) => errors.push(c.error) }), ext('E2', { onSettled: (c) => errors.push(c.error) })],
      async () => {
        throw boom
      },
    )
    expect(calls.filter((c) => c.endsWith('.settled'))).toEqual(['E1.settled', 'E2.settled'])
    expect(errors).toEqual([boom, boom])
    expect(res.telefunctionHasErrored).toBe(true)
    expect(res.telefunctionTopLevelError).toBe(boom)
  })

  it('T1.A4 settled runs on abort; abortValue preserved', async () => {
    const outcomes: string[] = []
    const res = await run([ext('E1', { onSettled: (c) => outcomes.push(c.outcome) })], async () => {
      throw Abort('v')
    })
    expect(outcomes).toEqual(['abort'])
    expect(res.telefunctionAborted).toBe(true)
    expect(res.telefunctionReturn).toBe('v')
  })

  it('T1.A5 no short-circuit across extensions (result transform)', async () => {
    await run([ext('E1', { onResult: () => 'wrapped' }), ext('E2')], okBody)
    expect(calls).toEqual(['E1.start', 'E2.start', 'E1.result', 'E2.result', 'E1.settled', 'E2.settled'])
  })

  it('T1.A6 data present only when carried', async () => {
    const seen: Record<string, unknown[]> = { E1: [], F: [] }
    await run(
      [
        ext('E1', {
          onStart: (c) => seen.E1!.push(c.data),
          onResult: (c) => {
            seen.E1!.push(c.data)
            return c.result
          },
          onSettled: (c) => seen.E1!.push(c.data),
        }),
        ext('F', {
          onStart: (c) => seen.F!.push(c.data),
          onResult: (c) => {
            seen.F!.push(c.data)
            return c.result
          },
          onSettled: (c) => seen.F!.push(c.data),
        }),
      ],
      okBody,
      { E1: { q: 1 } },
    )
    expect(seen.E1).toEqual([{ q: 1 }, { q: 1 }, { q: 1 }])
    expect(seen.F).toEqual([undefined, undefined, undefined]) // no request data, but hooks still ran
  })

  it('T1.A7 result transform applies without request data', async () => {
    const res = await run([ext('E1', { onResult: () => 'transformed' })], okBody, {})
    expect(res.telefunctionReturn).toBe('transformed')
  })

  it('T1.A8 start-hook throw — remaining starts attempted, body+result skipped, settled runs', async () => {
    const boom = new Error('start-fail')
    const res = await run(
      [
        ext('E1', {
          onStart: () => {
            throw boom
          },
        }),
        ext('E2'),
      ],
      okBody,
    )
    expect(calls).toContain('E2.start') // remaining start attempted
    expect(calls).not.toContain('body')
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0)
    expect(calls.filter((c) => c.endsWith('.settled'))).toEqual(['E1.settled', 'E2.settled'])
    expect(res.telefunctionTopLevelError).toBe(boom)
  })

  it('T1.A9 settled failure on success → first throw is primary, rest still run', async () => {
    const ea = new Error('Ea')
    const eb = new Error('Eb')
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const res = await run(
      [
        ext('E1', {
          onSettled: () => {
            throw ea
          },
        }),
        ext('E2', {
          onSettled: () => {
            throw eb
          },
        }),
      ],
      okBody,
    )
    expect(res.telefunctionTopLevelError).toBe(ea)
    expect(calls).toContain('E2.settled') // still ran
    expect(errorSpy).toHaveBeenCalled() // second throw logged
    errorSpy.mockRestore()
  })

  it('T1.A10 settled failure behind body failure is logged, original preserved', async () => {
    const bodyErr = new Error('body')
    const settledErr = new Error('settled')
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const res = await run(
      [
        ext('E1'),
        ext('E2', {
          onSettled: () => {
            throw settledErr
          },
        }),
      ],
      async () => {
        throw bodyErr
      },
    )
    expect(res.telefunctionTopLevelError).toBe(bodyErr) // not settledErr
    expect(errorSpy).toHaveBeenCalled()
    errorSpy.mockRestore()
  })

  it('T1.A13 result-hook throw — later results still run on last-good value; result discarded', async () => {
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
    expect(e3Received).toBe('r1') // last successful value, not r0 or E2's output
    expect(res.telefunctionHasErrored).toBe(true)
    expect(res.telefunctionTopLevelError).toBe(boom)
    expect(calls.filter((c) => c.endsWith('.settled'))).toEqual(['E1.settled', 'E2.settled', 'E3.settled'])
  })

  it('T1.A14 success transform chain threads the prior result', async () => {
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
    expect(e2Received).toBe('r1')
    expect(res.telefunctionReturn).toBe('r2')
  })

  it('T1.A15 settled failure on abort preserves Abort + logs', async () => {
    const settledErr = new Error('settled')
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const res = await run(
      [
        ext('E1', {
          onSettled: () => {
            throw settledErr
          },
        }),
        ext('E2'),
      ],
      async () => {
        throw Abort('v')
      },
    )
    expect(res.telefunctionAborted).toBe(true)
    expect(res.telefunctionReturn).toBe('v') // abort not converted to error
    expect(calls).toContain('E2.settled')
    expect(errorSpy).toHaveBeenCalled()
    errorSpy.mockRestore()
  })

  it('T1.A17 ctx.error is exact (undefined on success/abort, primary on error)', async () => {
    const captured: Array<unknown> = []
    const cap = (c: { error?: unknown }) => captured.push(c.error)
    await run([ext('E1', { onSettled: cap }), ext('E2', { onSettled: cap })], okBody)
    expect(captured).toEqual([undefined, undefined]) // success
    captured.length = 0
    await run([ext('E1', { onSettled: cap })], async () => {
      throw Abort('v')
    })
    expect(captured).toEqual([undefined]) // abort
    captured.length = 0
    const bodyErr = new Error('body')
    await run([ext('E1', { onSettled: cap })], async () => {
      throw bodyErr
    })
    expect(captured).toEqual([bodyErr]) // error
  })

  it('T1.A11 settleLiveState runs after settled hooks and includes a late-queued tag', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await run([ext('E1', { onSettled: () => invalidateTag('z') })], okBody)
    const tagBatches = tagBatchesFrom(publishSpy)
    expect(tagBatches).toHaveLength(1)
    expect(tagBatches[0]!.tags).toContain('z') // the settled-hook tag reached the settle publish
  })

  it('T1.A12 every hook observes the request context', async () => {
    const seen: boolean[] = []
    const check = () => seen.push(getRawContext() !== null)
    await run(
      [
        ext('E1', {
          onStart: check,
          onResult: (c) => {
            check()
            return c.result
          },
          onSettled: check,
        }),
      ],
      okBody,
    )
    expect(seen).toEqual([true, true, true])
  })

  it('T1.A16 core settle publishes a later settled hook’s tag even after an earlier settled hook throws', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await run(
      [
        ext('E1', {
          onSettled: () => {
            throw new Error('settled-fail')
          },
        }),
        ext('E2', { onSettled: () => invalidateTag('late') }),
      ],
      okBody,
    )
    const tagBatches = tagBatchesFrom(publishSpy)
    expect(tagBatches).toHaveLength(1)
    expect(tagBatches[0]!.tags).toContain('late')
    errorSpy.mockRestore()
  })

  it('T1.A18 table-driven trace over the 7 scenarios — attempt-all, single primary error', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const startErr = new Error('start')
    const bodyErr = new Error('body')
    const resultErr = new Error('result')
    const firstSettled = new Error('first')
    const secondSettled = new Error('second')

    // 1. start-throw → body+result skipped, all settled run, primary = start error
    calls = []
    let r = await run(
      [
        ext('A', {
          onStart: () => {
            throw startErr
          },
        }),
        ext('B'),
      ],
      okBody,
    )
    expect(calls).toEqual(['A.start', 'B.start', 'A.settled', 'B.settled'])
    expect(r.telefunctionTopLevelError).toBe(startErr)

    // 2. body-ok → full sequence
    calls = []
    r = await run([ext('A'), ext('B')], okBody)
    expect(calls).toEqual(['A.start', 'B.start', 'A.result', 'B.result', 'A.settled', 'B.settled'])
    expect(r.telefunctionHasErrored).toBe(false)

    // 3. body-error → result skipped, settled run
    calls = []
    r = await run([ext('A'), ext('B')], async () => {
      throw bodyErr
    })
    expect(calls).toEqual(['A.start', 'B.start', 'A.settled', 'B.settled'])
    expect(r.telefunctionTopLevelError).toBe(bodyErr)

    // 4. body-abort → result skipped, settled run, aborted
    calls = []
    r = await run([ext('A'), ext('B')], async () => {
      throw Abort('v')
    })
    expect(calls).toEqual(['A.start', 'B.start', 'A.settled', 'B.settled'])
    expect(r.telefunctionAborted).toBe(true)

    // 5. result-throw → later result hook STILL runs, primary = result error
    calls = []
    r = await run(
      [
        ext('A', {
          onResult: () => {
            throw resultErr
          },
        }),
        ext('B'),
      ],
      okBody,
    )
    expect(calls).toEqual(['A.start', 'B.start', 'A.result', 'B.result', 'A.settled', 'B.settled'])
    expect(r.telefunctionTopLevelError).toBe(resultErr)

    // 6. settled-throw (one) → surfaced, all settled attempted
    calls = []
    r = await run(
      [
        ext('A', {
          onSettled: () => {
            throw firstSettled
          },
        }),
        ext('B'),
      ],
      okBody,
    )
    expect(calls).toContain('A.settled')
    expect(calls).toContain('B.settled')
    expect(r.telefunctionTopLevelError).toBe(firstSettled)

    // 7. settled-throw (multi) → FIRST wins, both attempted
    calls = []
    r = await run(
      [
        ext('A', {
          onSettled: () => {
            throw firstSettled
          },
        }),
        ext('B', {
          onSettled: () => {
            throw secondSettled
          },
        }),
      ],
      okBody,
    )
    expect(r.telefunctionTopLevelError).toBe(firstSettled)
    expect(calls).toContain('B.settled')

    errorSpy.mockRestore()
  })
})

describe('forged / legacy request-extension data is inert (§3.B)', () => {
  it('T1.B1 request data for an unregistered extension invokes nothing', async () => {
    const res = await run([ext('E1')], okBody, { 'not-registered': { anything: true } })
    expect(calls).toEqual(['E1.start', 'E1.result', 'E1.settled'])
    expect(res.telefunctionHasErrored).toBe(false)
  })

  it('T1.B2 doctored data does not steer the lifecycle', async () => {
    const registered = () => [ext('E1')]
    await run(registered(), okBody, {})
    const plain = [...calls]
    calls = []
    await run(registered(), okBody, { '@telefunc/live': { queryKey: ['x'], invalidates: [['y']] } })
    expect(calls).toEqual(plain) // identical order + no extra behavior
  })

  it('T1.B3 forged data triggers no tag publish', async () => {
    const publishSpy = vi.spyOn(getBroadcastAdapter(), 'publish')
    await run([ext('E1')], okBody, { '@telefunc/live': { queryKey: ['x'], invalidates: [['y']] } })
    expect(tagBatchesFrom(publishSpy)).toHaveLength(0) // barrier frames are not tag batches
  })
})
