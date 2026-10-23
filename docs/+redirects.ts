export { redirects }

import type { Config } from 'vike/types'
import type { HeadingsURL } from './headings'
import { checkType } from './utils/checkType'

// Use TypeScript to check whether redirect targets point to an existing page
type RemoveHash<T extends string> = T extends `${infer Path}#${string}` ? Path : T
type RedirectsURL = RemoveHash<(typeof redirects)[keyof typeof redirects]>
checkType<HeadingsURL>(0 as unknown as RedirectsURL)

const redirects = {
  '/remix': '/react-router',
  '/httpHeaders': '/headers',
  '/telefunc': '/serve',
  '/server': '/Telefunc',
  '/initial-page-data': '/initial-data',
  '/install': '/bundler',
  '/transformer': '/how-it-works',
  '/RPC': '/schemaless#rpc',
  '/RPC-vs-GraphQL-REST': '/schemaless#rpc-vs-graphql-rest',
  '/event-based': '/best-practices#event-based-architecture',
  '/abort-vs-error': '/error-handling#error-flows',
  '/form-validation': '/validation',
  '/multiple-clients': '/best-practices#multiple-clients',
} as const satisfies Config['redirects']
