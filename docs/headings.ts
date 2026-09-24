export { categories }
export { headings }
export { headingsDetached }
export type { HeadingsURL }

import type {
  Config,
  HeadingDefinition,
  HeadingDetachedDefinition as HeadingDetachedDefinition_,
} from '@brillout/docpress'
import { iconGear, iconPlug, iconSeedling } from '@brillout/docpress' with { type: 'vike:pointer' }
type HeadingDetachedDefinition = Omit<HeadingDetachedDefinition_, 'category'> & {
  category: CategoryNames | 'Miscellaneous'
}

type ExtractHeadingUrl<C> = C extends { url: infer N extends string } ? N : C extends string ? C : never
type HeadingsURL = ExtractHeadingUrl<(typeof headings)[number]> | ExtractHeadingUrl<(typeof headingsDetached)[number]>
type ExtractCategoryName<C> = C extends { name: infer N extends string } ? N : C extends string ? C : never
type CategoryNames = ExtractCategoryName<(typeof categories)[number]>

const categories = [
  'Guides',
  'Guides (more)',
  'Integrations',
  'API',
  'Miscellaneous',
] as const satisfies Config['categories']

const headingsDetached = [...misc(), ...guidesMore()] satisfies HeadingDetachedDefinition[]

const headings = [
  // #region Onboarding
  {
    level: 1,
    title: 'Guides',
    titleIcon: iconSeedling,
    color: '#74d717',
  },
  {
    level: 2,
    title: 'Introduction',
    titleDocument: 'Telefunc',
    url: '/',
  },
  {
    level: 2,
    title: 'Get Started',
    url: '/start',
    sectionTitles: ['My first telefunction'],
  },
  {
    level: 2,
    title: 'Server Integration',
    url: '/server-integration',
  },
  {
    level: 2,
    title: 'Initial Data',
    url: '/initial-data',
  },
  {
    level: 2,
    title: 'Why Telefunc?',
    url: '/why-telefunc',
  },
  {
    level: 2,
    title: 'Best Practices',
    url: '/best-practices',
  },

  // #region Guides
  {
    level: 4,
    title: 'Basics',
  },
  {
    level: 2,
    title: 'Permissions',
    url: '/permissions',
    sectionTitles: ['DRY Permissions'],
  },
  {
    level: 2,
    title: 'Validation',
    url: '/validation',
  },
  {
    level: 2,
    title: 'Error handling',
    url: '/error-handling',
  },
  {
    level: 2,
    title: 'Testing',
    url: '/testing',
  },
  {
    level: 4,
    title: 'Streaming & real-time',
  },
  {
    level: 2,
    title: 'Stream',
    url: '/stream',
  },
  {
    level: 2,
    title: '`@telefunc/tanstack-query`',
    url: '/tanstack-query',
  },
  {
    level: 2,
    title: '`@telefunc/rxjs`',
    url: '/rxjs',
  },
  {
    level: 4,
    title: 'Files',
  },
  {
    level: 2,
    title: 'File upload',
    url: '/file-upload',
  },
  {
    level: 2,
    title: 'File download',
    url: '/file-download',
  },
  // #endregion

  {
    level: 4,
    title: 'Learn More',
  },
  {
    level: 2,
    title: 'Why Schemaless?',
    url: '/schemaless',
    sectionTitles: ['Schemaless vs schema-full', 'RPC vs GraphQL/REST'],
  },
  {
    level: 2,
    title: 'How it works',
    url: '/how-it-works',
    sectionTitles: ['Telefunction lifecycle'],
  },
  // #endregion

  // #region Integrations
  {
    level: 1,
    title: 'Integrations',
    titleIcon: iconPlug,
    color: '#ffd511',
  },
  {
    level: 4,
    title: 'Metaframeworks',
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
    title: 'React Router',
    url: '/react-router',
  },
  {
    level: 4,
    title: 'Native',
  },
  {
    level: 2,
    title: 'React Native',
    url: '/react-native',
  },
  {
    level: 4,
    title: 'Bundlers',
  },
  {
    level: 2,
    title: 'Custom bundler',
    url: '/bundler',
  },
  {
    level: 2,
    title: 'Vite',
    url: '/vite-plugin',
  },
  {
    level: 2,
    title: 'Webpack',
    url: '/webpack-plugin',
  },
  {
    level: 2,
    title: 'Babel',
    url: '/babel-plugin',
  },
  // #endregion

  // #region API
  {
    level: 1,
    title: 'API',
    titleIcon: iconGear,
    color: '#80c1db',
  },
  {
    level: 4,
    title: 'Server Middleware',
  },
  {
    level: 2,
    title: '`new Telefunc()`',
    url: '/Telefunc',
  },
  {
    level: 2,
    title: '`serve()`',
    url: '/serve',
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
    level: 2,
    title: '`provideTelefuncContext()`',
    url: '/provideTelefuncContext',
  },
  {
    level: 2,
    title: '`withContext()`',
    url: '/withContext',
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
    sectionTitles: ['Automatic (from TypeScript)', 'Manual'],
  },
  {
    level: 4,
    title: 'Hooks',
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
    level: 2,
    title: '`onClose()`',
    url: '/onClose',
    sectionTitles: ['`context.onClose()`', '`channel.onClose()`', '`context.signal`'],
  },
  {
    level: 4,
    title: 'Stream',
  },
  {
    level: 2,
    title: '`Channel`',
    url: '/channel',
    sectionTitles: ['`new Channel()`', '`Broadcast`', '`new BroadcastChannel()`'],
  },
  {
    level: 2,
    title: '`close()`',
    url: '/close',
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
    title: '`headers`',
    url: '/headers',
  },
  {
    level: 2,
    title: '`transport`',
    url: '/transport',
  },
  {
    level: 2,
    titleInNav: '`channel`',
    title: '`channel` (config)',
    url: '/channel-config',
  },
  {
    level: 2,
    title: '`fetch`',
    url: '/fetch',
  },
  {
    level: 2,
    title: '`telefuncFiles`',
    url: '/telefuncFiles',
  },
  {
    level: 2,
    title: '`root`',
    url: '/root',
  },
  {
    level: 2,
    titleInNav: '`shield`',
    title: '`shield` (config)',
    url: '/shield-config',
  },
  {
    level: 2,
    title: '`log`',
    url: '/log',
  },
  // #endregion
] as const satisfies HeadingDefinition[]

function misc() {
  return (
    [
      {
        title: '❌ Non-function exports',
        url: '/warning/non-function-export',
      },
    ] as const
  ).map((h) => ({ ...h, category: 'Miscellaneous' as const })) satisfies HeadingDetachedDefinition[]
}

function guidesMore() {
  return (
    [
      {
        title: 'Stream at Scale',
        url: '/stream/scale',
      },
      {
        title: 'Stream on Cloudflare',
        url: '/stream/cloudflare',
      },
      {
        title: '`@telefunc/redis`',
        url: '/redis',
      },
    ] as const
  ).map((h) => ({ ...h, category: 'Guides (more)' as const })) satisfies HeadingDetachedDefinition[]
}
