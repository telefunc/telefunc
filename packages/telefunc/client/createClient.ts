export { createClient }
export type { Client, ClientOptions }

import { remoteTelefunctionCall } from './remoteTelefunctionCall.js'
import { withContext, getPendingContext, type ClientCallContext } from './withContext.js'
import { randomUuid } from '../utils/randomUuid.js'

type Promisify<T> = T extends Promise<any> ? T : Promise<T>

type Client<Api> = {
  [K in keyof Api as K extends 'then' ? never : K]: Api[K] extends (...args: infer Args) => infer Return
    ? (...args: Args) => Promisify<Return>
    : never
}

type ClientOptions = {
  url?: string
  headers?: Record<string, string>
}

function createClient<Api extends Record<string, any>>(options: ClientOptions = {}): Client<Api> {
  const instanceConnectionKey = randomUuid()
  const callers = new Map<string, Function>()

  return new Proxy({} as Client<Api>, {
    get(_target, prop) {
      if (prop === 'then') {
        return undefined
      }
      if (typeof prop !== 'string') {
        return undefined
      }

      let caller = callers.get(prop)
      if (!caller) {
        caller = (...args: unknown[]) => {
          const pending = getPendingContext()
          const mergedContext: ClientCallContext = {
            ...pending,
            telefuncUrl: pending?.telefuncUrl ?? options.url,
            headers: {
              ...options.headers,
              ...pending?.headers,
            },
            channel: {
              ...pending?.channel,
              connectionKey: pending?.channel?.connectionKey ?? instanceConnectionKey,
            },
          }
          return withContext(remoteTelefunctionCall, mergedContext)('', prop, args)
        }
        callers.set(prop, caller)
      }

      return caller
    },
  })
}
