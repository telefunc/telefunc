export { redirects }

import type { Config } from 'vike/types'
import type { HeadingsURL } from './headings'
import { checkType } from './utils/checkType'

// Use TypeScript to check whether redirect targets point to an existing page
type RemoveHash<T extends string> = T extends `${infer Path}#${string}` ? Path : T
type RedirectsURL = RemoveHash<(typeof redirects)[keyof typeof redirects]>
checkType<HeadingsURL>(0 as any as RedirectsURL)

const redirects = {
  '/remix': '/react-router',
} as const satisfies Config['redirects']
