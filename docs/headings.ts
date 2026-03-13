export { categories }
export { headings }
export { headingsDetached }
export type { HeadingsURL }

import type {
  Config,
  HeadingDefinition,
  HeadingDetachedDefinition as HeadingDetachedDefinition_,
} from '@brillout/docpress'
import { iconScroll, iconEyes, iconGear, iconSeedling } from '@brillout/docpress' with { type: 'vike:pointer' }
type HeadingDetachedDefinition = Omit<HeadingDetachedDefinition_, 'category'> & {
  category: CategoryNames | 'Miscellaneous'
}

type ExtractHeadingUrl<C> = C extends { url: infer N extends string } ? N : C extends string ? C : never
type HeadingsURL = ExtractHeadingUrl<(typeof headings)[number]> | ExtractHeadingUrl<(typeof headingsDetached)[number]>
type ExtractCategoryName<C> = C extends { name: infer N extends string } ? N : C extends string ? C : never
type CategoryNames = ExtractCategoryName<(typeof categories)[number]>

const categories = ['Guides', 'API', 'Get Started', 'Overview', 'Miscellaneous'] as const satisfies Config['categories']

const headingsDetached = [
  {
    title: 'Cloudflare Workers',
    url: '/telefuncWebSocketCloudflare',
    category: 'API',
  },
  ...misc(),
] satisfies HeadingDetachedDefinition[]

const headings = [
  {
    level: 1,
    title: 'Learn',
    titleIcon: iconEyes,
    color: '#bd55dd',
    titleIconStyle: {
      width: 30,
      height: 30,
      position: 'relative',
      top: -2,
    },
  },
  {
    level: 2,
    title: 'Introduction',
    titleDocument: 'Telefunc',
    url: '/',
  },
  {
    level: 2,
    title: 'RPC',
    url: '/RPC',
  },
  {
    level: 2,
    title: 'RPC vs GraphQL/REST',
    url: '/RPC-vs-GraphQL-REST',
  },
  {
    level: 1,
    title: 'Get Started',
    titleIcon: iconSeedling,
    color: '#74d717',
  },
  {
    level: 4,
    title: 'Framework integration',
  },
  {
    level: 2,
    title: 'Next.js',
    url: '/next',
  },
  {
    level: 2,
    title: 'SvelteKit',
    url: '/svelte-kit',
  },
  {
    level: 2,
    title: 'Vike',
    url: '/vike',
  },
  {
    level: 2,
    title: 'Nuxt',
    url: '/nuxt',
  },
  {
    level: 2,
    title: 'React Native',
    url: '/react-native',
  },
  {
    level: 2,
    title: 'React Router',
    url: '/react-router',
  },
  {
    level: 4,
    title: 'Server integration',
  },
  {
    level: 2,
    title: 'Server (Hono, Express, ...)',
    url: '/server',
  },
  {
    level: 4,
    title: 'Bundler integration',
  },
  {
    level: 2,
    title: 'Custom bundler',
    titleInNav: 'Custom bundler',
    url: '/install',
  },
  {
    level: 1,
    title: 'Guides',
    titleIcon: iconScroll,
    color: '#ffd511',
  },
  {
    level: 2,
    title: 'Permissions',
    url: '/permissions',
    sectionTitles: ['`getContext()` wrapping'],
  },
  {
    level: 2,
    title: 'Error handling',
    url: '/error-handling',
  },
  {
    level: 2,
    title: 'Form validation',
    url: '/form-validation',
    sectionTitles: ['`throw Abort(someValue)`'],
  },
  {
    level: 2,
    title: 'Event-based telefunctions',
    url: '/event-based',
  },
  {
    level: 2,
    title: 'File upload',
    url: '/file-upload',
  },
  {
    level: 2,
    title: 'Stream',
    url: '/stream',
  },
  {
    level: 2,
    title: 'Real-Time',
    url: '/real-time',
  },
  {
    level: 1,
    title: 'API',
    titleIcon: iconGear,
    color: '#80c1db',
    menuModalFullWidth: true,
  },
  {
    level: 4,
    title: 'Context',
  },
  {
    level: 2,
    title: '`getContext()`',
    url: '/getContext',
  },
  {
    level: 4,
    title: 'Protection',
  },
  {
    level: 2,
    title: '`throw Abort()`',
    url: '/Abort',
  },
  {
    level: 2,
    title: '`shield()`',
    url: '/shield',
    sectionTitles: ['TypeScript - Automatic', 'TypeScript - Manual'],
  },
  {
    level: 4,
    title: 'Server Middleware',
  },
  {
    level: 2,
    title: '`telefunc()',
    url: '/telefunc',
  },
  {
    level: 2,
    title: '`telefuncWebSocket()`',
    url: '/telefuncWebSocket',
  },
  {
    level: 4,
    title: 'Channels',
  },
  {
    level: 2,
    title: '`createChannel()`',
    url: '/createChannel',
  },
  {
    level: 4,
    title: 'Error Handling',
  },
  {
    level: 2,
    title: '`onBug()`',
    url: '/onBug',
  },
  {
    level: 2,
    title: '`onAbort()`',
    url: '/onAbort',
  },
  {
    level: 4,
    title: 'Config',
  },
  {
    level: 2,
    title: '`telefuncUrl`',
    url: '/telefuncUrl',
  },
  {
    level: 2,
    title: '`disableNamingConvention`',
    url: '/disableNamingConvention',
  },
  {
    level: 2,
    title: '`httpHeaders`',
    url: '/httpHeaders',
  },
  {
    level: 2,
    title: '`transport`',
    url: '/transport',
  },
  {
    level: 2,
    title: '`fetch`',
    url: '/fetch',
  },
  {
    level: 2,
    title: '`telefuncFiles',
    url: '/telefuncFiles',
  },
  {
    level: 2,
    title: '`root`',
    url: '/root',
  },
  {
    level: 2,
    title: '`shield`',
    url: '/shield-config',
  },
  {
    level: 2,
    title: '`log`',
    url: '/log',
  },
  {
    level: 4,
    title: 'Plugins',
  },
  {
    level: 2,
    title: 'Vite Plugin',
    url: '/vite-plugin',
  },
  {
    level: 2,
    title: 'Webpack Plugin',
    url: '/webpack-plugin',
  },
  {
    level: 2,
    title: 'Babel Plugin',
    url: '/babel-plugin',
  },
] as const satisfies HeadingDefinition[]

function misc() {
  return (
    [
      {
        title: '`throw Abort()` vs `throw new Error()`',
        url: '/abort-vs-error',
      },
      {
        title: 'Telefunc Transformer',
        url: '/transformer',
      },
      {
        title: 'Initial Page Data',
        url: '/initial-page-data',
      },
      {
        title: 'Initial Data',
        url: '/initial-data',
      },
      {
        title: 'Multiple Clients',
        url: '/multiple-clients',
      },
      {
        title: '❌ Non-function exports',
        url: '/warning/non-function-export',
      },
    ] as const
  ).map((h) => ({ ...h, category: 'Miscellaneous' as const })) satisfies HeadingDetachedDefinition[]
}
