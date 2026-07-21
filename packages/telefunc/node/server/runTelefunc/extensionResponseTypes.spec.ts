import { afterEach, describe, expect, it } from 'vitest'
import { config } from '../serverConfig.js'
import { getServerConfig } from '../serverConfig.js'
import { serializeTelefunctionResult } from './serializeTelefunctionResult.js'
import type { ReplacerType, ServerReplacerContext, TypeContract } from '../../../wire-protocol/types.js'

// AN EXTENSION REGISTERED WHILE THE TELEFUNC FILES LOAD MUST STILL BE CONSULTED WHEN THE RESPONSE IS
// SERIALIZED.
//
// The ordering this pins is `runTelefunc`'s own, and it is the whole bug:
//
//   runTelefunc.ts:289   const serverConfig = getServerConfig()      ← config resolved (SNAPSHOT)
//   runTelefunc.ts:311   await parseHttpRequest(runContext)
//   runTelefunc.ts:330   await loadTelefuncFiles(runContext)         ← user files evaluate; extensions register HERE
//   runTelefunc.ts:395   serializeTelefunctionResult(runContext)     ← must see what :330 registered
//
// `config.extensions.push(…)` at module level in a `.telefunc.js` file is a supported way to install an
// extension, and it necessarily runs at :330 — after the snapshot. While the serializer read that snapshot,
// such an extension's `responseTypes` were invisible to the very request that loaded it, and the value it
// exists to encode was serialized as an ordinary object with NO error raised. Silent, and invisible to
// built-in types, which are a static list.
//
// Deliberately NOT a test about any particular extension: the fixture below is a plain branded object with
// no meaning to telefunc. The bug is in the seam, not in any consumer of it. (Discovered by
// @telefunc/drizzle-experimental, the first external wire extension; its browser e2e is the real-path
// witness, and this is the mechanism pinned at the unit that owns it.)

const PREFIX = '!SpecDummyType:'

/** A response type with no relationship to anything telefunc ships — the point is that it is EXTERNAL. */
const dummyReplacer: ReplacerType<TypeContract<{ mark: string }, unknown, { mark: string }>, ServerReplacerContext> = {
  prefix: PREFIX,
  detect(value): value is { mark: string } {
    return typeof value === 'object' && value !== null && (value as { __specDummy?: true }).__specDummy === true
  },
  replace(value) {
    return { metadata: { mark: value.mark }, close() {}, abort() {} }
  },
}

/** The runContext the serializer needs, minus anything this test does not exercise.
 *
 *  `serverConfig` deliberately still carries the request-start snapshot's `extensionResponseTypes`, exactly
 *  as `runTelefunc` assembles it. The serializer no longer reads that field — but passing it reproduces
 *  what production actually handed in, so reverting the fix reproduces the REAL failure (a stale, usually
 *  EMPTY list ⇒ the value serializes unreplaced and silently) rather than a crash on a missing field. A
 *  mutant that fails by throwing would prove nothing about the defect this pins. */
function runContextFor(
  telefunctionReturn: unknown,
  snapshot: ReplacerType<TypeContract, ServerReplacerContext>[] = [],
) {
  return {
    telefunctionReturn,
    telefunctionName: 'onSomething',
    telefuncFilePath: '/spec/Some.telefunc.ts',
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
    serverConfig: { extensionResponseTypes: snapshot, log: { shieldErrors: {} as never } },
  }
}

describe('extension responseTypes registered during telefunc-file evaluation', () => {
  afterEach(() => {
    const registered = config.extensions as unknown as unknown[]
    const index = registered.findIndex((ext) => (ext as { name?: string }).name === 'spec-dummy-extension')
    if (index >= 0) registered.splice(index, 1)
  })

  it('is consulted by the serializer even though the request resolved its config BEFORE it registered', () => {
    // 1. The request resolves its config — as runTelefunc does at :289, before any telefunc file is loaded.
    const snapshotTakenBeforeRegistration = getServerConfig()
    expect(snapshotTakenBeforeRegistration.extensionResponseTypes).not.toContain(dummyReplacer)

    // 2. A telefunc file evaluates and registers an extension — runTelefunc :330.
    config.extensions.push({
      name: 'spec-dummy-extension',
      responseTypes: [dummyReplacer as unknown as ReplacerType<TypeContract, ServerReplacerContext>],
    })

    // 3. The response is serialized — runTelefunc :395. The extension must be consulted.
    const result = serializeTelefunctionResult(
      runContextFor({ __specDummy: true, mark: 'm1' }, snapshotTakenBeforeRegistration.extensionResponseTypes),
    )

    expect(result.type).toBe('text')
    // Reading the request-start snapshot made this fail: the value serialized as a plain object
    // (`{"__specDummy":true,"mark":"m1"}`) with no prefix and no error.
    expect((result as { body: string }).body).toContain(PREFIX)
  })

  it('CONTROL: an unregistered type is NOT replaced — the assertion above can fail', () => {
    // Without this, "the body contains the prefix" could pass for a reason unrelated to registration.
    const result = serializeTelefunctionResult(runContextFor({ __specDummy: true, mark: 'm2' }))
    expect((result as { body: string }).body).not.toContain(PREFIX)
    expect((result as { body: string }).body).toContain('m2') // it IS in the payload, just unreplaced
  })
})
