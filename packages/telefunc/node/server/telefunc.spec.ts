import { afterEach, describe, expect, it, vi } from 'vitest'
import { serve } from './telefunc.js'

const setFlag = (value: boolean | undefined) => {
  const Cloudflare = value === undefined ? undefined : { compatibilityFlags: { enable_request_signal: value } }
  vi.stubGlobal('Cloudflare', Cloudflare)
}
const flagWarnings = async () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  await serve({ request: new Request('http://localhost/_telefunc', { method: 'POST', body: '{}' }) })
  return warn.mock.calls.filter(([message]) => String(message).includes('enable_request_signal')).length
}

describe('serve(): enable_request_signal', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('warns once on a Worker with the flag off, and nowhere else', async () => {
    setFlag(undefined)
    expect(await flagWarnings()).toBe(0)
    setFlag(true)
    expect(await flagWarnings()).toBe(0)
    setFlag(false)
    expect(await flagWarnings()).toBe(1)
    expect(await flagWarnings()).toBe(0)
  })
})
