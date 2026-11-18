import { afterEach, describe, expect, it, vi } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { reportBroadcastDeliveryLost } from './reportBroadcastDeliveryLost.js'

afterEach(() => {
  config.broadcast = {}
})

// A monitoring client the app reports lost messages to may fail on its own.
const FAILURES: Record<string, () => unknown> = {
  'throws an Error': () => {
    throw new Error('monitoring client failed')
  },
  'returns a rejected promise': () => Promise.reject(new Error('monitoring client failed')),
  'throws a value without a prototype': () => {
    throw { __proto__: null, code: 'MONITORING_DOWN' }
  },
}

describe('config.broadcast.onDeliveryLost', () => {
  for (const [fails, onDeliveryLost] of Object.entries(FAILURES)) {
    it(`a callback that ${fails} is logged as the app's bug, and the report goes on`, async () => {
      const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => unhandled.push(reason)
      process.on('unhandledRejection', onUnhandled)
      try {
        config.broadcast = { onDeliveryLost }
        let threw: unknown = null
        try {
          reportBroadcastDeliveryLost('room:lost', 'missed 2 messages')
        } catch (err) {
          threw = err
        }
        await new Promise((resolve) => setTimeout(resolve, 10))
        expect({
          threw,
          unhandled,
          logged: logged.mock.calls.some(([error]) => /monitoring client failed|MONITORING_DOWN/.test(String(error))),
        }).toEqual({ threw: null, unhandled: [], logged: true })
      } finally {
        process.off('unhandledRejection', onUnhandled)
        logged.mockRestore()
      }
    })
  }
})
