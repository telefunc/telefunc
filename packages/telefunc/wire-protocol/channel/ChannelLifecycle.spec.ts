import { describe, expect, test } from 'vitest'

import { ChannelLifecycle, type ChannelLifecycleOwner } from './ChannelLifecycle.js'
import { createAbortError, type AbortError } from '../../shared/Abort.js'

function createLifecycle() {
  const aborts: AbortError[] = []
  const owner: ChannelLifecycleOwner = {
    _end() {},
    _abortFromCallback: (err) => aborts.push(err),
    _reportCallbackError() {},
  }
  return { aborts, lifecycle: new ChannelLifecycle({ owner }) }
}

describe('ChannelLifecycle', () => {
  test('onOpen registered after the channel opened runs at once', () => {
    const { lifecycle } = createLifecycle()
    const opened: string[] = []
    lifecycle.onOpen(() => opened.push('early'))
    lifecycle.fireOpen()
    lifecycle.onOpen(() => opened.push('late'))
    expect(opened).toEqual(['early', 'late'])
  })

  test('an Abort an onOpen throws goes to the owner and skips the callbacks after it', () => {
    const { aborts, lifecycle } = createLifecycle()
    const opened: string[] = []
    lifecycle.onOpen(() => {
      throw createAbortError('a')
    })
    lifecycle.onOpen(() => opened.push('second'))
    lifecycle.fireOpen()
    expect(aborts.map((err) => err.abortValue)).toEqual(['a'])
    expect(opened).toEqual([])
  })

  test('onClose registered after the channel ended runs at once, with the error it ended with', () => {
    const { lifecycle } = createLifecycle()
    const err = new Error('gone')
    lifecycle.end(err)
    lifecycle.fireClose(err)
    let closeErr: Error | undefined
    lifecycle.onClose((e) => {
      closeErr = e
    })
    expect(closeErr).toBe(err)
  })
})
