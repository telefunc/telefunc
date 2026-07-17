import { beforeEach, describe, expect, it, vi } from 'vitest'
import { executeTelefunction } from './executeTelefunction.js'
import { Abort } from '../Abort.js'
import { createRequestContext } from '../context/requestContext.js'
import { REQUEST_CONTEXT } from '../context/requestContext.js'
import { getRawContext } from '../context/context.js'
import { getServerConfig } from '../serverConfig.js'
import type { TelefuncServerExtension } from '../extensions.js'
import { getBroadcastAdapter } from '../../../wire-protocol/server/broadcast.js'

let calls: string[]

beforeEach(() => {
  calls = []
})

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

describe('lifecycle — result transform chain', () => {
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
      { E1: {}, E2: {} },
    )
    expect(calls).toEqual(['E1.result', 'E2.result'])
    expect(e2Received).toBe('r1') // each hook receives the previous hook's returned result
    expect(res.telefunctionReturn).toBe('r2')
    expect(res.telefunctionHasErrored).toBe(false)
  })

  it('a result-hook throw stops the chain and propagates', async () => {
    const boom = new Error('result-fail')
    await expect(
      run(
        [
          ext('E1', { onResult: () => 'r1' }),
          ext('E2', {
            onResult: () => {
              throw boom
            },
          }),
          ext('E3', { onResult: () => 'r3' }),
        ],
        okBody,
        { E1: {}, E2: {}, E3: {} },
      ),
    ).rejects.toBe(boom)
    // The hooks form a transform chain, not a cleanup chain: once one link throws, the value handed to
    // the next is not a result anyone produced, so the chain stops rather than transforming a stale one.
    expect(calls).toEqual(['E1.result', 'E2.result'])
  })

  it('result-hook runs ONLY for an extension the request activated (pre-PR gate); `data` is that payload', async () => {
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
      { E1: { q: 1 } }, // the request carried data for E1 only
    )
    expect(seen.E1).toEqual([{ q: 1 }]) // E1 was activated → ran with its request payload
    expect(seen.F).toEqual([]) // F carried no request data → its hook did NOT run
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
      { E1: {} },
    )
    expect(seen).toEqual([true])
  })
})

describe('lifecycle — body outcome', () => {
  it('body error: RESULT skipped, outcome errors, original error propagates', async () => {
    const boom = new Error('boom')
    const res = await run(
      [ext('E1'), ext('E2')],
      async () => {
        throw boom
      },
      { E1: {}, E2: {} },
    )
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0) // result skipped on error
    expect(res.telefunctionHasErrored).toBe(true)
    expect(res.telefunctionTopLevelError).toBe(boom)
  })

  it('body abort: outcome aborts, abortValue preserved, RESULT skipped', async () => {
    const res = await run(
      [ext('E1')],
      async () => {
        throw Abort('v')
      },
      { E1: {} },
    )
    expect(calls.filter((c) => c.endsWith('.result'))).toHaveLength(0)
    expect(res.telefunctionAborted).toBe(true)
    expect(res.telefunctionReturn).toBe('v')
  })

  it('a non-async telefunction is a usage error', async () => {
    await expect(run([ext('E1')], (() => 'sync') as never)).rejects.toThrow(
      /did not return a promise or async generator/,
    )
  })
})

describe('forged / legacy request-extension data is inert', () => {
  it('request data for an unregistered extension invokes nothing (only registered, request-activated extensions run)', async () => {
    const res = await run([ext('E1')], okBody, { E1: {}, 'not-registered': { anything: true } })
    expect(calls).toEqual(['E1.result']) // E1 ran via its own data; the unregistered payload invoked nothing
    expect(res.telefunctionHasErrored).toBe(false)
  })
})

describe('regression — a plain telefunction pays no live-query readiness cost', () => {
  it('executing a non-Live telefunction touches the broadcast transport zero times', async () => {
    // The every-telefunction fence that used to run at entry awaited the tag hub's readiness barrier,
    // which subscribed to the tag key and published barrier frames on the broadcast transport — a cost
    // paid by EVERY telefunction, Live or not. With the tag stack cut, a plain telefunction must do
    // none of that. Spying the transport is a can-fail proof: re-introduce the fence and these fire.
    const adapter = getBroadcastAdapter()
    const subscribeSpy = vi.spyOn(adapter, 'subscribe')
    const publishSpy = vi.spyOn(adapter, 'publish')
    const res = await run([], async () => 'plain-result')
    expect(res.telefunctionReturn).toBe('plain-result')
    expect(subscribeSpy).not.toHaveBeenCalled()
    expect(publishSpy).not.toHaveBeenCalled()
    subscribeSpy.mockRestore()
    publishSpy.mockRestore()
  })
})
