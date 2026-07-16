import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { runTelefunc } from '../runTelefunc.js'
import { registerTelefunction } from './loadTelefuncFilesUsingRegistration.js'
import { getRawContext } from '../context/context.js'
import { onPostSerialize } from '../context/postSerialize.js'

// Force the in-memory registration path (below) instead of the Vite/production-entry loader, which the
// vitest env otherwise resolves first.
vi.mock('./loadTelefuncFilesUsingVite.js', () => ({ loadTelefuncFilesUsingVite: async () => null }))

// Finding 2 (Ticket 6 R1 re-cert): the post-serialize disposer-drain must run on EVERY outcome of the
// execute→serialize pipeline. A body throw jumps to runTelefunc's catch, skipping serialize entirely, and
// a serialize failure throws before the response is built — the OLD drain (inside serializeTelefunctionResult
// only) missed both, leaking the db.live read token minted during the body. These drive the REAL runTelefunc
// so the drain's placement (runTelefunc's outer finally) — not just the drain mechanism — is under test.

const FILE = '/postSerializeDrain.telefunc.ts'

function register(name: string, fn: () => unknown): void {
  registerTelefunction(fn as never, name, FILE)
}

function telefuncRequest(name: string): Request {
  // TEXT-kind telefunc request: POST the { file, name, args } envelope to the default telefuncUrl.
  return new Request('http://localhost/_telefunc?_telefunc=txt', {
    method: 'POST',
    body: stringify({ file: FILE, name, args: [] }),
  })
}

let errorSpy: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  // Silence the expected server-error logging from the body-throw / serialize-failure paths.
  errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
})
afterEach(() => errorSpy.mockRestore())

describe('R1 post-serialize drain runs on every runTelefunc outcome (Ticket 6, Finding 2)', () => {
  it('body-throw BEFORE serialize → the minted disposer still drains (net-zero)', async () => {
    let drained = 0
    register('bodyThrows', async () => {
      onPostSerialize(getRawContext()!, () => {
        drained++
      })
      throw new Error('body boom')
    })
    const res = await runTelefunc({ request: telefuncRequest('bodyThrows') })
    expect(res.statusCode).toBe(500) // telefunction bug → server-error response
    expect(drained).toBe(1) // the OUTER finally drained even though serialize never ran
  })

  it('serialize FAILURE after the body → the minted disposer still drains', async () => {
    let drained = 0
    register('serializeFails', async () => {
      onPostSerialize(getRawContext()!, () => {
        drained++
      })
      // A property whose getter throws when the serializer reads it → stringify throws → assertUsage.
      return {
        get unserializable(): never {
          throw new Error('cannot serialize')
        },
      }
    })
    const res = await runTelefunc({ request: telefuncRequest('serializeFails') })
    expect(res.statusCode).toBe(500)
    expect(drained).toBe(1) // the serialize throw propagated to the SAME outer finally
  })

  it('success → the disposer drains exactly once', async () => {
    let drained = 0
    register('ok', async () => {
      onPostSerialize(getRawContext()!, () => {
        drained++
      })
      return 'ok'
    })
    const res = await runTelefunc({ request: telefuncRequest('ok') })
    expect(res.statusCode).toBe(200)
    expect(res.body).toContain('ok')
    expect(drained).toBe(1)
  })
})
