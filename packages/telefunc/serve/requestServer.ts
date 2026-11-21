export { serverOfRequest, storeServerOfRequests }

import { AsyncLocalStorage } from 'node:async_hooks'
import type { HttpServer } from './nodeUpgrades.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'

const { requestServer, serversWithRequestStore } = getGlobalObject('serve/requestServer.ts', {
  /** The server whose `request` event is running, for `serve({ request })`, which gets a `Request` without it. */
  requestServer: new AsyncLocalStorage<HttpServer>(),
  serversWithRequestStore: new WeakSet<HttpServer>(),
})

/** Runs the server's `request` event in `requestServer`'s store, once per server: every other event, and the event's
 *  arguments and return value, pass through as they are. */
function storeServerOfRequests(server: HttpServer): void {
  if (serversWithRequestStore.has(server)) return
  serversWithRequestStore.add(server)
  const emit = server.emit
  Reflect.set(server, 'emit', function (this: unknown, event: unknown, ...args: unknown[]): unknown {
    const pass = () => Reflect.apply(emit, this, [event, ...args])
    return event === 'request' ? requestServer.run(server, pass) : pass()
  })
}

/** The server whose `request` event is running, where that server has the store. */
function serverOfRequest(): HttpServer | undefined {
  return requestServer.getStore()
}
