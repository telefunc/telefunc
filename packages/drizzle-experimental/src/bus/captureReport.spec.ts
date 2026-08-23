import { readFileSync, readdirSync } from 'node:fs'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { report } from './captureReport.js'

const srcDir = resolve(dirname(fileURLToPath(import.meta.url)), '..')

/** Everything that SHIPS — the same convention as importGraph.spec.ts. Specs and test kits are excluded, so
 *  this file's own mentions of the forbidden call do not make it an offender. */
function productionFiles(): string[] {
  return readdirSync(srcDir, { recursive: true, encoding: 'utf8' })
    .filter((entry) => entry.endsWith('.ts') && !entry.endsWith('.spec.ts') && !entry.endsWith('.testKit.ts'))
    .map((entry) => relative(srcDir, resolve(srcDir, entry)).replace(/\\/g, '/'))
}

describe('the diagnostics seam is the only one', () => {
  it('nothing that ships calls console.error except captureReport.ts', () => {
    // The seam's guarantee — a diagnostic never fails the caller's write — is only worth the words if every
    // diagnostic goes through it. Four modules had grown their own direct calls, each one an unguarded
    // console away from turning a recovered write into a failed one.
    const files = productionFiles()
    expect(files).toContain('bus/captureReport.ts') // FAIL CLOSED: a moved directory must not read as clean

    const callers = files.filter((file) => /\bconsole\.error\s*\(/.test(readFileSync(resolve(srcDir, file), 'utf8')))
    expect(callers).toEqual(['bus/captureReport.ts'])
  })

  it('scopes its claim to console.error — console.warn is a different, advisory seam', () => {
    // Stated so this gate is not mistaken for coverage it does not have. `console.warn` reports a
    // CONFIGURATION problem (a relation declared two ways, a shape that could not be extracted) with no
    // recovery riding on it, so it is deliberately outside the never-throw guarantee.
    const warners = productionFiles().filter((file) =>
      /\bconsole\.warn\s*\(/.test(readFileSync(resolve(srcDir, file), 'utf8')),
    )
    expect(warners.length).toBeGreaterThan(0)
  })
})

describe('report — telling the operator is best-effort, the recovery is not', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('logs a fault WITHOUT a cause as one argument — no trailing undefined', () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    report('tap-unplaceable', { relation: 'users' })
    expect(logged.mock.calls[0]!.length).toBe(1)
    expect(String(logged.mock.calls[0]![0])).toContain('could not observe the statement')
  })

  it('logs a fault WITH a cause as the message beside the error', () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    const cause = new Error('boom')
    report('publish-failed', { cause })
    expect(logged.mock.calls[0]).toEqual([expect.stringContaining('publishing a change batch'), cause])
  })

  it('renders a relation identity the way an operator writes it', () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    report('capture-failed', { relation: '1:a5:users', cause: new Error('x') })
    expect(String(logged.mock.calls[0]![0])).toContain('"a.users"')
  })

  it('a console that THROWS does not propagate to the caller', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {
      throw new Error('console exploded')
    })
    expect(() => report('capture-failed', { relation: 'users', cause: new Error('x') })).not.toThrow()
  })

  it('a relation identity that cannot be RENDERED does not propagate either', () => {
    // Rendering parses the identity, and parsing can fail — `capture-failed` is reported with every table a
    // failed batch touched, joined, which is not a single parseable identity. Building the message is inside
    // the guarantee for exactly this reason: the operator loses the line, never the write.
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    expect(() => report('capture-failed', { relation: '1:a5:users, 1:b5:posts', cause: new Error('x') })).not.toThrow()
    expect(logged).not.toHaveBeenCalled()
  })
})
