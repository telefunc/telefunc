import { describe, it, expect } from 'vitest'
import type { ResolvedConfig } from 'vite'
import { isViteServerSide, isViteClientSide, type ViteEnv } from './isViteServerSide.js'

function globalConfig(build?: { ssr?: boolean }): ResolvedConfig {
  return { build } as unknown as ResolvedConfig
}

function env(name: string | undefined, config: Record<string, unknown>): ViteEnv {
  return { name, config } as unknown as ViteEnv
}

describe('isViteServerSide()', () => {
  it('uses config.consumer for arbitrarily named environments', () => {
    // https://github.com/telefunc/telefunc/issues/458
    const viteEnv = env('vercel_client', { consumer: 'client', build: { ssr: false } })
    expect(isViteServerSide(globalConfig({ ssr: false }), viteEnv)).toBe(false)
    expect(isViteClientSide(globalConfig({ ssr: false }), viteEnv)).toBe(true)
  })

  it('detects server-side environments with custom names', () => {
    const viteEnv = env('vercel_node', { consumer: 'server', build: { ssr: true } })
    expect(isViteServerSide(globalConfig({ ssr: true }), viteEnv)).toBe(true)
  })

  it("handles Vite's default environments", () => {
    expect(isViteServerSide(globalConfig({ ssr: false }), env('client', { consumer: 'client' }))).toBe(false)
    expect(isViteServerSide(globalConfig({ ssr: true }), env('ssr', { consumer: 'server' }))).toBe(true)
  })

  it('falls back to the environment name when config.consumer is missing', () => {
    expect(isViteServerSide(globalConfig({ ssr: false }), env('client', {}))).toBe(false)
    expect(isViteServerSide(globalConfig({ ssr: false }), env('ssr', {}))).toBe(true)
  })

  it('falls back to build.ssr when neither config.consumer nor a known name is available', () => {
    expect(isViteServerSide(globalConfig({ ssr: true }), env('vercel_node', { build: { ssr: true } }))).toBe(true)
    expect(isViteServerSide(globalConfig({ ssr: true }), env('vercel_client', { build: { ssr: false } }))).toBe(false)
  })
})
