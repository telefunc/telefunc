import { describe, expect, it, beforeEach } from 'vitest'
import { defineTelefunctions } from './defineTelefunctions.js'
import { serve } from './telefunc.js'
import { shield } from './shield.js'
import { clearRegistration } from './runTelefunc/loadTelefuncFilesUsingRegistration.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { createClient } from '../../client/createClient.js'
import { withContext } from '../../client/withContext.js'
import { abort } from '../../client/abort.js'

describe('defineTelefunctions', () => {
  beforeEach(() => {
    clearRegistration()
  })

  it('registers functions and returns api object unchanged', async () => {
    const fn1 = async () => 'hello'
    const fn2 = async (x: number) => x * 2

    const api = defineTelefunctions({
      sayHello: fn1,
      double: fn2,
    })

    expect(api.sayHello).toBe(fn1)
    expect(api.double).toBe(fn2)
    expect((fn1 as any)._key).toBe(':sayHello')
    expect((fn1 as any)._isStandalone).toBe(true)
  })

  it('throws usage error for invalid names or duplicate registration', () => {
    expect(() => {
      defineTelefunctions({
        'invalid:name': async () => {},
      })
    }).toThrow('must not contain')

    expect(() => {
      defineTelefunctions({
        then: async () => {},
      })
    }).toThrow('"then" is a reserved name')

    defineTelefunctions({
      first: async () => {},
    })

    expect(() => {
      defineTelefunctions({
        first: async () => {},
      })
    }).toThrow('is already registered')

    expect(() => {
      defineTelefunctions({
        toString: async () => 'ok',
      })
    }).not.toThrow()
  })

  it('throws usage error for non-function values', () => {
    expect(() => {
      defineTelefunctions({
        notAFunc: 'string' as any,
      })
    }).toThrow('expected `api.notAFunc` to be a function')
  })

  describe('execution via serve()', () => {
    it('executes standalone telefunction with zero arguments and no shield', async () => {
      defineTelefunctions({
        getTime: async () => 12345,
      })

      const res = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'getTime',
          args: [],
        }),
      })

      expect(res.statusCode).toBe(200)
      expect(await res.getBody()).toContain('12345')
    })

    it('rejects standalone telefunction without shield when arguments are passed (422)', async () => {
      defineTelefunctions({
        noArgsFn: async () => 'ok',
      })

      const res = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'noArgsFn',
          args: ['unexpected argument'],
        }),
      })

      expect(res.statusCode).toBe(422)
    })

    it('validates arguments when shield is used', async () => {
      defineTelefunctions({
        sum: shield([shield.type.number, shield.type.number], async (a: number, b: number) => a + b),
      })

      // Valid args
      const resValid = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'sum',
          args: [2, 3],
        }),
      })
      expect(resValid.statusCode).toBe(200)
      expect(await resValid.getBody()).toContain('5')

      // Invalid args -> 422
      const resInvalid = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'sum',
          args: ['2', '3'],
        }),
      })
      expect(resInvalid.statusCode).toBe(422)
    })

    it('handles shield.type.any for accepting arbitrary arguments', async () => {
      defineTelefunctions({
        echo: shield([shield.type.any], async (x: any) => x),
      })

      const res = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'echo',
          args: [{ foo: 'bar' }],
        }),
      })
      expect(res.statusCode).toBe(200)
      expect(await res.getBody()).toContain('bar')
    })

    it('returns 400 for malformed request with colon in name (not 500)', async () => {
      defineTelefunctions({
        testFn: async () => 'ok',
      })

      const res = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'malformed:name',
          args: [],
        }),
      })
      expect(res.statusCode).toBe(400)
    })

    it('returns 400 when file is not found (not 500)', async () => {
      defineTelefunctions({
        testFn: async () => 'ok',
      })

      const res = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain' },
        body: JSON.stringify({
          file: 'non-telefunc-file',
          name: 'testFn',
          args: [],
        }),
      })
      expect(res.statusCode).toBe(400)
    })

    it('handles non-JSON types (like BigInt) in args without crashing on shield error logging', async () => {
      defineTelefunctions({
        noShieldFn: async () => 'ok',
      })

      const res = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: stringify({
          file: '',
          name: 'noShieldFn',
          args: [BigInt('12345678901234567890')],
        }),
      })
      expect(res.statusCode).toBe(422)
    })

    it('allows standalone telefunctions and file-based telefunctions to coexist', async () => {
      defineTelefunctions({
        standalonePing: async () => 'pong',
      })

      const resPing = await serve({
        url: '/_telefunc',
        method: 'POST',
        headers: { 'content-type': 'text/plain', 'x-telefunc-request': 'txt' },
        body: JSON.stringify({
          file: '',
          name: 'standalonePing',
          args: [],
        }),
      })
      expect(resPing.statusCode).toBe(200)
      expect(await resPing.getBody()).toContain('pong')
    })
  })
})

describe('createClient', () => {
  it('returns undefined for .then so awaiting the client does not send a call', async () => {
    type Api = {
      test: () => Promise<string>
    }
    const client = createClient<Api>({ url: 'http://localhost:3000/_telefunc' })
    expect((client as any).then).toBeUndefined()
    const resolved = await client
    expect(resolved).toBe(client)
  })

  it('exposes callable members that wrap remoteTelefunctionCall with merged context', async () => {
    // Simulate browser environment
    const prevWindow = (globalThis as any).window
    ;(globalThis as any).window = { scrollY: 0 }

    try {
      type Api = {
        greet: (name: string) => Promise<string>
      }

      const client = createClient<Api>({
        url: 'http://localhost:3000/_telefunc',
        headers: { 'X-Custom-Client': 'value' },
      })

      expect(typeof client.greet).toBe('function')

      // withContext on member
      const withCustomHeader = withContext(client.greet, {
        headers: { 'X-Per-Call': 'call-value' },
      })
      expect(typeof withCustomHeader).toBe('function')
    } finally {
      if (prevWindow === undefined) {
        delete (globalThis as any).window
      } else {
        ;(globalThis as any).window = prevWindow
      }
    }
  })
})
