import { describe, it, expect } from 'vitest'
import { makeAbortError } from './errors.js'
import { Abort, abortValueMessage } from '../../shared/Abort.js'

const ctx = { telefunctionName: 'onDoThing', telefuncFilePath: '/pages/x.telefunc.ts' }
const generic = 'Aborted telefunction call onDoThing() (/pages/x.telefunc.ts).'

describe('abortValueMessage', () => {
  it('derives a message from a string / Error / { message }, else undefined', () => {
    expect(abortValueMessage('banned')).toBe('banned')
    expect(abortValueMessage(new Error('boom'))).toBe('boom')
    expect(abortValueMessage({ message: 'nope', code: 403 })).toBe('nope')
    expect(abortValueMessage({ code: 403 })).toBeUndefined() // structured payload, no message
    expect(abortValueMessage(undefined)).toBeUndefined()
    expect(abortValueMessage(null)).toBeUndefined()
    expect(abortValueMessage(42)).toBeUndefined()
  })
})

describe('makeAbortError — the value message wins, context is the fallback', () => {
  it("surfaces a server `throw Abort('reason')` on err.message (was masked by the generic text)", () => {
    const err = makeAbortError('You are banned', ctx)
    expect(err.message).toBe('You are banned')
    expect(err.abortValue).toBe('You are banned')
    expect(err instanceof Abort).toBe(true)
  })

  it('surfaces the .message of a structured abort value when it carries one', () => {
    const err = makeAbortError({ message: 'Rate limited', retryIn: 30 }, ctx)
    expect(err.message).toBe('Rate limited')
    expect(err.abortValue).toEqual({ message: 'Rate limited', retryIn: 30 })
  })

  it('keeps the generic call description for a message-less structured abort (the documented pattern)', () => {
    const err = makeAbortError({ notLoggedIn: true }, ctx)
    expect(err.message).toBe(generic) // diagnostics preserved; the app reads err.abortValue.notLoggedIn
    expect(err.abortValue).toEqual({ notLoggedIn: true })
  })

  it('keeps the generic description for a value-less (client-initiated) abort', () => {
    const err = makeAbortError(undefined, ctx)
    expect(err.message).toBe(generic)
  })
})
