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
// `peerDependencies.telefunc: >=0.2.23` states that requirement; this spec ENFORCES it. A version range is
// a claim about the world and drifts (a republish, a wrong floor, a consumer on an older core resolved by a
// loose range). This fails against any telefunc that cannot serve the package's own registration ordering,
// whatever its version says.
//
// Red-provable: revert the union read in telefunc's serializeTelefunctionResult and this goes red with the
// silent plain-object miss — the real defect, not a crash.

describe('telefunc compatibility — the peer floor, enforced', () => {
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
