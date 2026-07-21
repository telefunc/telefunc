import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, describe, expect, it } from 'vitest'
import { LiveCell } from './primitive/live.js'
// SPEC-ONLY deep imports of the installed telefunc's BUILT modules. Not on telefunc's public entry, and
// nothing at runtime reaches this way — the package talks to core exclusively through the public extension
// seam. But the dependencies being pinned here ARE core's internals — the extension scanner and the
// serializer's request-start snapshot semantics — so the spec has to look at the things it depends on.
//
// Deliberately `dist/`, not `../../telefunc/node/…` source: importing the source drags telefunc's .ts into
// THIS package's tsc program under THIS package's compilerOptions, which fails on core files needing a
// newer lib (`Uint8Array.toBase64`). The built .d.ts avoids recompiling core — and it is also the more
// faithful target, since `dist/` is what a consumer installs. Requires telefunc to be built; it is a
// workspace dependency, so the repo's build already does that.
import { getExtensionImports } from '../../telefunc/dist/node/shared/discoverExtensions.js'
import { serializeTelefunctionResult } from '../../telefunc/dist/node/server/runTelefunc/serializeTelefunctionResult.js'
import { getServerConfig } from '../../telefunc/dist/node/server/serverConfig.js'

// THE VERSION BOUNDARY, as executable checks rather than a number in package.json.
//
// This package registers its wire replacer through telefunc's auto-load seam: core scans the project
// root's package.json for `@telefunc/*` dependencies whose manifest declares `"telefunc": { "server": … }`
// and injects that import AHEAD OF every transformed `.telefunc.*` module — so registration precedes
// serialization for any SSR/preloaded request. It is NOT process-start eager: a DIRECT first request with
// no prior server import snapshots its config before the module evaluates, and that one request misses
// the replacer — the standing envelope of every `telefunc.server` extension on main (@telefunc/rxjs
// included), ledgered as a core follow-up. Both halves of the seam (`config.extensions` + the scanner)
// shipped in telefunc 0.2.21 (commit 56573c1f, first released in v0.2.21) — the floor this file enforces.
//
// FOUR COMPLEMENTARY CHECKS, none substituting for another and none claiming more than it composes:
//
//  - the MANIFEST test proves the declared floor cannot drift below the release that carries the seam;
//  - the DISCOVERY test proves core's REAL scanner finds this package's REAL manifest bytes;
//  - the REGISTRATION-MECHANISM test proves importing the entry registers, and that a snapshot taken
//    after the import serializes a Live (it imports the entry BY HAND — it does not compose the real
//    scanner→transform→module-eval→request schedule, so it proves the mechanism, not end-to-end order);
//  - the PINNED-RESIDUAL test asserts the CURRENT TRUTH that a snapshot from before any server import
//    misses the replacer. If registration ever becomes genuinely process-start eager (a core fix, an
//    eager entry), this case goes RED — deliberately, so the claims above get rewritten rather than
//    silently overclaiming.

/** The release that first carries telefunc's extension seam AND the auto-load scanner (both in 56573c1f). */
const REQUIRED_TELEFUNC_MINIMUM = [0, 2, 21] as const

/** Exactly the range syntaxes this gate claims to understand, ANCHORED to the whole string: `>=x.y.z`,
 *  `^x.y.z`, and a bare exact `x.y.z`. For each, the lowest admitted version is the triple itself. */
const SUPPORTED_RANGE = /^(?:>=|\^)?(\d+)\.(\d+)\.(\d+)$/

/** The lowest version a range admits, as a numeric triple — FAILING CLOSED on anything this gate cannot
 *  reason about.
 *
 *  It used to scan for the first `x.y.z` anywhere in the string and ignore everything around it, which made
 *  the gate green-light precisely the ranges it exists to reject: `<=0.2.21` and `<0.2.21` (which admit
 *  older releases, and in the second case ONLY older ones) both "passed", as did a union whose second arm
 *  re-admits everything the first excludes. An operator-blind read of a range is not a floor.
 *
 *  Unknown syntax now THROWS rather than guessing. A gate that cannot parse its input must not return an
 *  answer: widening the grammar has to be a deliberate edit here, with a control added below for whatever
 *  new form is admitted. */
function minimumOf(range: string): [number, number, number] {
  const matched = range.trim().match(SUPPORTED_RANGE)
  if (!matched) {
    throw new Error(
      `Cannot determine a version floor from peer range ${JSON.stringify(range)}. This gate understands ` +
        `">=x.y.z", "^x.y.z" and exact "x.y.z" only. Use one of those, or extend SUPPORTED_RANGE ` +
        `deliberately and add a control for the new form — do not loosen it to make a range parse.`,
    )
  }
  return [Number(matched[1]), Number(matched[2]), Number(matched[3])]
}

const atLeast = (actual: readonly number[], required: readonly number[]) =>
  actual[0]! !== required[0]!
    ? actual[0]! > required[0]!
    : actual[1]! !== required[1]!
      ? actual[1]! > required[1]!
      : actual[2]! >= required[2]!
describe('telefunc compatibility — the declared peer floor', () => {
  it('peerDependencies.telefunc admits nothing below the release that carries the extension seam', () => {
    // Reads THIS package's own manifest, so loosening the range is what fails — the check the behaviour
    // tests structurally cannot make, since the workspace core carries the seam regardless of what is
    // declared.
    const manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as {
      peerDependencies: Record<string, string>
    }
    const range = manifest.peerDependencies.telefunc
    expect(range, 'telefunc must be a declared peer dependency').toBeTypeOf('string')
    expect(
      atLeast(minimumOf(range!), REQUIRED_TELEFUNC_MINIMUM),
      `peerDependencies.telefunc is "${range}", which admits a telefunc older than ` +
        `${REQUIRED_TELEFUNC_MINIMUM.join('.')} — those releases have no config.extensions seam at all`,
    ).toBe(true)
  })

  it('CONTROL: the floor comparison rejects a lower range and accepts a higher one', () => {
    // Without this, the assertion above could pass because `atLeast` is simply permissive.
    expect(atLeast(minimumOf('>=0.2.0'), REQUIRED_TELEFUNC_MINIMUM)).toBe(false)
    expect(atLeast(minimumOf('>=0.2.20'), REQUIRED_TELEFUNC_MINIMUM)).toBe(false)
    expect(atLeast(minimumOf('>=0.2.21'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true)
    expect(atLeast(minimumOf('>=0.3.0'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true) // a legitimate future raise
    expect(atLeast(minimumOf('^1.0.0'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true)
    expect(atLeast(minimumOf('0.2.21'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true) // bare exact pin
  })

  it('CONTROL: an UNSAFE OPERATOR is rejected, not silently read as a floor', () => {
    // The false green this gate shipped with: reading the first `x.y.z` anywhere ignored the operator, so
    // `<=0.2.21` (admits every older release) and `<0.2.21` (admits ONLY older ones) both passed — the gate
    // green-lighting exactly what it exists to reject. They must now fail loudly instead.
    expect(() => minimumOf('<=0.2.21')).toThrow(/Cannot determine a version floor/)
    expect(() => minimumOf('<0.2.21')).toThrow(/Cannot determine a version floor/)
    expect(() => minimumOf('<0.3.0')).toThrow(/Cannot determine a version floor/)
  })

  it('CONTROL: a UNION is rejected — a safe first arm does not make the range safe', () => {
    // `>=0.2.21 || >=0.2.0` reads as satisfied by the first arm while the second re-admits everything the
    // first excludes. Parsing only the leading triple could never see that.
    expect(() => minimumOf('>=0.2.21 || >=0.2.0')).toThrow(/Cannot determine a version floor/)
    expect(() => minimumOf('>=0.2.21 || <0.2.21')).toThrow(/Cannot determine a version floor/)
  })

  it('CONTROL: other unparsable forms fail closed rather than guessing', () => {
    for (const unsupported of ['*', 'x', '~0.2.21', '0.2.x', '>=0.2.21 <0.3.0', '0.2.0 - 0.2.30', 'latest', '']) {
      expect(() => minimumOf(unsupported), `expected ${JSON.stringify(unsupported)} to be rejected`).toThrow()
    }
  })
})
describe('telefunc compatibility — auto-load discovery, registration, and residual', () => {
  // A project root whose package.json depends on this package, with this package's REAL manifest bytes
  // resolvable from it — what core's scanner reads in a user's app. The fixture fakes the LAYOUT only;
  // the manifest content is byte-identical to ours, so a drifted `telefunc` field or specifier fails here.
  const root = mkdtempSync(join(tmpdir(), 'tf-autoload-'))
  afterAll(() => rmSync(root, { recursive: true, force: true }))

  // Captured BEFORE anything in this file imports the entry — the module-eval schedule a DIRECT first
  // request sees (its per-request `getServerConfig()` runs before loadTelefuncFiles evaluates modules).
  const snapshotBeforeAnyServerImport = getServerConfig().extensionResponseTypes

  it('core’s REAL scanner discovers this package’s REAL manifest and emits the server import', () => {
    writeFileSync(
      join(root, 'package.json'),
      JSON.stringify({ name: 'fixture-app', dependencies: { '@telefunc/drizzle-experimental': '*' } }),
    )
    const pkgDir = join(root, 'node_modules', '@telefunc', 'drizzle-experimental')
    mkdirSync(pkgDir, { recursive: true })
    writeFileSync(join(pkgDir, 'package.json'), readFileSync(new URL('../package.json', import.meta.url)))
    expect(getExtensionImports(root, 'server')).toEqual(["import '@telefunc/drizzle-experimental/telefunc-server';"])
    // …and nothing is declared for the client side: the reviver belongs to the tanstack-query adapter,
    // which registers at ITS import. An empty list here is that decision, pinned.
    expect(getExtensionImports(root, 'client')).toEqual([])
  })

  it('REGISTRATION MECHANISM: importing the entry registers, and a later snapshot serializes the Live', async () => {
    // The entry is imported BY HAND here — this composes import→snapshot→serialize, NOT the real
    // scanner→transform→module-eval→request schedule, so it proves the registration mechanism and the
    // post-registration snapshot semantics, nothing more. What a request scheduled BEFORE the import
    // sees is the pinned-residual case below.
    await import('./telefunc-server.js') // the real entry, side effect only
    const after = getServerConfig().extensionResponseTypes
    expect(after.some((replacer) => replacer.prefix === '!TelefuncLive:')).toBe(true)

    const live = new LiveCell([{ id: 1, text: 'a' }])
    const result = serializeTelefunctionResult({
      telefunctionReturn: live,
      telefunctionName: 'onGetTodos',
      telefuncFilePath: '/app/Todos.telefunc.ts',
      telefunctionAborted: false,
      context: {} as never,
      requestContext: {
        responseAbort: { abort: () => {}, onAbort: () => {} },
        trackPending: () => () => {},
        markComplete: () => {},
        abortSignal: new AbortController().signal,
      } as never,
      abortSignal: new AbortController().signal,
      streamTransport: 'INLINE' as never,
      useNodeStream: false,
      serverConfig: { extensionResponseTypes: after, log: { shieldErrors: {} as never } },
    })
    expect(result.type).toBe('text')
    const body = (result as { body: string }).body
    expect(body).toContain('!TelefuncLive:') // replaced through the snapshot a post-registration request carries
    expect(body).toContain('channelId')
  })

  it('PINNED RESIDUAL (current truth): a snapshot from before any server import misses the replacer', () => {
    // The direct-first-request schedule, measured: `getServerConfig()` ran at module scope above, before
    // this file imported the entry — exactly a request whose config resolves before loadTelefuncFiles
    // evaluates the injected import. Core's serializer reads that snapshot, so the Live serializes as a
    // plain object, silently. The standing envelope of every telefunc.server extension on main; the core
    // snapshot-ordering fix is the ledgered follow-up. IF THIS CASE GOES RED, registration has become
    // genuinely process-start eager — rewrite the package's ordering claims (package.json,
    // telefunc-server.ts, wireServer.ts) alongside flipping this to a positive assertion.
    expect(snapshotBeforeAnyServerImport.some((replacer) => replacer.prefix === '!TelefuncLive:')).toBe(false)
    const live = new LiveCell([{ id: 1, text: 'a' }])
    const result = serializeTelefunctionResult({
      telefunctionReturn: live,
      telefunctionName: 'onGetTodos',
      telefuncFilePath: '/app/Todos.telefunc.ts',
      telefunctionAborted: false,
      context: {} as never,
      requestContext: {
        responseAbort: { abort: () => {}, onAbort: () => {} },
        trackPending: () => () => {},
        markComplete: () => {},
        abortSignal: new AbortController().signal,
      } as never,
      abortSignal: new AbortController().signal,
      streamTransport: 'INLINE' as never,
      useNodeStream: false,
      serverConfig: { extensionResponseTypes: snapshotBeforeAnyServerImport, log: { shieldErrors: {} as never } },
    })
    const body = (result as { body: string }).body
    expect(body).not.toContain('!TelefuncLive:') // unreplaced — the residual, pinned rather than papered over
  })

  it('CONTROL: a value this package does not brand is left alone — the check above can fail', () => {
    // Without this, "the body contains the Live prefix" could pass for a reason unrelated to the replacer.
    const result = serializeTelefunctionResult({
      telefunctionReturn: { data: [{ id: 1 }], notALive: true },
      telefunctionName: 'onGetPlain',
      telefuncFilePath: '/app/Todos.telefunc.ts',
      telefunctionAborted: false,
      context: {} as never,
      requestContext: {
        responseAbort: { abort: () => {}, onAbort: () => {} },
        trackPending: () => () => {},
        markComplete: () => {},
        abortSignal: new AbortController().signal,
      } as never,
      abortSignal: new AbortController().signal,
      streamTransport: 'INLINE' as never,
      useNodeStream: false,
      serverConfig: { extensionResponseTypes: [], log: { shieldErrors: {} as never } },
    })
    const body = (result as { body: string }).body
    expect(body).not.toContain('!TelefuncLive:')
    expect(body).toContain('notALive') // it IS in the payload, just unreplaced
  })
})
