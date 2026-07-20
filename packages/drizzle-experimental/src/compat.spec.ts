import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { LiveCell } from './primitive/live.js'
import { installLiveReplacer } from './primitive/wireServer.js'
// SPEC-ONLY deep import of the installed telefunc's BUILT serializer. Not on telefunc's public entry, and
// nothing at runtime reaches this way — the package talks to core exclusively through the public extension
// seam. But the dependency being pinned here IS core's internal serialize behaviour, so the spec has to
// look at the thing it depends on.
//
// Deliberately `dist/`, not `../../telefunc/node/…` source: importing the source drags telefunc's .ts into
// THIS package's tsc program under THIS package's compilerOptions, which fails on core files needing a
// newer lib (`Uint8Array.toBase64`). Reading the built .d.ts avoids recompiling core — and it is also the
// more faithful target, since `dist/` is what a consumer installs. Requires telefunc to be built; it is a
// workspace dependency, so the repo's build already does that.
import { serializeTelefunctionResult } from '../../telefunc/dist/node/server/runTelefunc/serializeTelefunctionResult.js'

// THE VERSION BOUNDARY, as an executable check rather than a number in package.json.
//
// This package registers its wire replacer from `reactiveDrizzle()`, and the documented shape calls that at
// MODULE LEVEL in a `.telefunc.ts` file — so registration happens while telefunc loads the telefunc files,
// which is AFTER a request has already resolved its config. A telefunc whose serializer reads that
// request-start snapshot never sees the replacer: the Live serializes as an ordinary object, no error is
// raised anywhere, and the client silently receives something that was never replaced. Every released
// telefunc through 0.2.22 behaves that way.
//
// TWO COMPLEMENTARY CHECKS, and neither substitutes for the other:
//
//  - the BEHAVIOUR test proves the telefunc actually installed HAS the seam this package needs. It says
//    nothing about what the manifest promises — it passed identically while `peerDependencies.telefunc`
//    still said `>=0.2.0`, because the workspace core is fixed either way.
//  - the MANIFEST test proves the declared floor cannot drift back below the release that carries the fix.
//    It says nothing about whether the installed core works — only about what consumers are promised.
//
// The first without the second is what let a stale `>=0.2.0` sit unnoticed: a user installing against a
// released 0.2.x would have resolved a core that silently serializes the Live as a plain object, and no
// test in this repo would have objected.
//
// Red-provable in both directions: revert the union read in telefunc's serializeTelefunctionResult (rebuild
// dist — that is the artifact this spec loads) and the behaviour test goes red with the silent plain-object
// miss; loosen the manifest floor and the manifest test goes red.

/** The release that first carries telefunc's live-config union read in `serializeTelefunctionResult`. */
const REQUIRED_TELEFUNC_MINIMUM = [0, 2, 23] as const

/** The lowest version a range admits, as a numeric triple. Deliberately tolerant of the range SYNTAX
 *  (`>=x.y.z`, `^x.y.z`, `x.y.z`) and strict about the floor it implies — the point is to permit a
 *  legitimate future raise while refusing a drop, which an exact-string match could not do. */
function minimumOf(range: string): [number, number, number] {
  const matched = range.match(/(\d+)\.(\d+)\.(\d+)/)
  if (!matched) throw new Error(`Cannot read a version floor from peer range ${JSON.stringify(range)}`)
  return [Number(matched[1]), Number(matched[2]), Number(matched[3])]
}

const atLeast = (actual: readonly number[], required: readonly number[]) =>
  actual[0]! !== required[0]!
    ? actual[0]! > required[0]!
    : actual[1]! !== required[1]!
      ? actual[1]! > required[1]!
      : actual[2]! >= required[2]!

describe('telefunc compatibility — the declared peer floor', () => {
  it('peerDependencies.telefunc admits nothing below the release that carries the fix', () => {
    // Reads THIS package's own manifest, so loosening the range is what fails — the check the behaviour
    // test structurally cannot make, since the workspace core is fixed regardless of what is declared.
    const manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as {
      peerDependencies: Record<string, string>
    }
    const range = manifest.peerDependencies.telefunc
    expect(range, 'telefunc must be a declared peer dependency').toBeTypeOf('string')
    expect(
      atLeast(minimumOf(range!), REQUIRED_TELEFUNC_MINIMUM),
      `peerDependencies.telefunc is "${range}", which admits a telefunc older than ` +
        `${REQUIRED_TELEFUNC_MINIMUM.join('.')} — those releases silently serialize a Live as a plain object`,
    ).toBe(true)
  })

  it('CONTROL: the floor comparison rejects a lower range and accepts a higher one', () => {
    // Without this, the assertion above could pass because `atLeast` is simply permissive.
    expect(atLeast(minimumOf('>=0.2.0'), REQUIRED_TELEFUNC_MINIMUM)).toBe(false)
    expect(atLeast(minimumOf('>=0.2.22'), REQUIRED_TELEFUNC_MINIMUM)).toBe(false)
    expect(atLeast(minimumOf('>=0.2.23'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true)
    expect(atLeast(minimumOf('>=0.3.0'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true) // a legitimate future raise
    expect(atLeast(minimumOf('^1.0.0'), REQUIRED_TELEFUNC_MINIMUM)).toBe(true)
  })
})

describe('telefunc compatibility — the installed core has the seam', () => {
  it('the installed telefunc consults a replacer registered AFTER the request resolved its config', () => {
    // Reproduce runTelefunc's ordering: the request resolves its config, THEN the telefunc files evaluate
    // and this package registers. `installLiveReplacer()` is the real registration path — the same call
    // `reactiveDrizzle()` makes — so this pins the shipping seam, not a stand-in for it.
    const snapshotTakenBeforeRegistration: never[] = [] // what a request that resolved config first carries
    installLiveReplacer()

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
      serverConfig: { extensionResponseTypes: snapshotTakenBeforeRegistration, log: { shieldErrors: {} as never } },
    })

    expect(result.type).toBe('text')
    const body = (result as { body: string }).body
    // The Live must have been REPLACED. Against a snapshot-only telefunc this fails with the body carrying
    // the cell's own shape instead — silently, which is what makes the version floor worth enforcing.
    expect(body).toContain('!TelefuncLive:')
    expect(body).toContain('channelId')
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
