export { inRequestScope, setRequestScope }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'

/** Runs `fn` as code of the call `request` made runs. */
type RequestScope = <T>(request: Request, fn: () => T) => T

const globalObject = getGlobalObject<{ scope: RequestScope | null }>('requestScope.ts', { scope: null })

/** Runs `fn` as code of the call `request` made runs, where the host lends that code what AsyncLocalStorage doesn't
 *  carry on it: a Cloudflare session Durable Object on a Worker without AsyncLocalStorage. Elsewhere, and without
 *  `request`, it runs `fn`. */
function inRequestScope<T>(request: Request | undefined, fn: () => T): T {
  const { scope } = globalObject
  return scope !== null && request !== undefined ? scope(request, fn) : fn()
}

/** The host's scope of a call's code: Cloudflare's adapter installs it. */
function setRequestScope(scope: RequestScope): void {
  globalObject.scope = scope
}
