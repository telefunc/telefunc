export { telefuncMiddleware }
export type { TelefuncMiddlewareOptions }

import { enhance, type Get, type UniversalMiddleware } from '@universal-middleware/core'
import { serve } from '../server/index.js'

type TelefuncMiddlewareOptions = {
  /**
   * The Telefunc HTTP endpoint URL.
   *
   * Make sure it matches your `config.telefuncUrl` setting.
   *
   * @default '/_telefunc'
   *
   * https://telefunc.com/telefuncUrl
   */
  telefuncUrl?: string
}

/**
 * Create Telefunc's Universal Middleware.
 *
 * Use this instead of the default export when you need to customize the middleware, e.g. a custom `telefuncUrl`.
 *
 * @example
 * ```ts
 * // +middleware.ts (Vike)
 * import { telefuncMiddleware } from 'telefunc/universal-middleware'
 * export default telefuncMiddleware({ telefuncUrl: '/api/_telefunc' })
 * ```
 */
const telefuncMiddleware = ((options?: TelefuncMiddlewareOptions) => {
  const telefuncUniversalMiddleware: UniversalMiddleware = async (request, context, runtime) => {
    const httpResponse = await serve({
      request,
      context: {
        ...context,
        ...runtime,
      },
    })
    const { statusCode, headers } = httpResponse
    return new Response(httpResponse.getReadableWebStream(), {
      status: statusCode,
      headers,
    })
  }
  return enhance(telefuncUniversalMiddleware, {
    name: 'telefunc',
    method: 'POST',
    path: options?.telefuncUrl ?? '/_telefunc',
  })
}) satisfies Get<[options?: TelefuncMiddlewareOptions], UniversalMiddleware>

/**
 * Telefunc's Universal Middleware.
 *
 * It works with any server (Hono, Express, Cloudflare, Vercel, ...) thanks to the universal-middleware standard.
 *
 * https://universal-middleware.dev
 *
 * @example
 * ```ts
 * // +middleware.ts (Vike)
 * export { default } from 'telefunc/universal-middleware'
 * ```
 */
export default telefuncMiddleware()
