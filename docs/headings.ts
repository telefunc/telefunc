export { categories }
export { headings }
export type { HeadingsURL }

import type { Config, HeadingDefinition } from '@brillout/docpress'
import { iconGear, iconPlug, iconSeedling } from '@brillout/docpress' with { type: 'vike:pointer' }

type ExtractHeadingUrl<C> = C extends { url: infer N extends string } ? N : C extends string ? C : never
type HeadingsURL = ExtractHeadingUrl<(typeof headings)[number]>

const categories = ['Guides', 'Integrations', 'API'] as const satisfies Config['categories']

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
    title: 'File uploads',
    url: '/file-upload',
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
    title: 'Server frameworks',
  },
  {
    level: 2,
    title: 'Hono, Express, etc.',
    url: '/server',
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
    title: 'Server',
  },
  {
    level: 2,
    title: '`telefunc()',
    url: '/telefunc',
  },
  {
    level: 2,
    title: '`throw Abort()`',
    url: '/Abort',
  },
  {
    level: 2,
    title: '`getContext()`',
    url: '/getContext',
  },
  {
    level: 2,
    title: '`shield()`',
    url: '/shield',
    sectionTitles: ['Automatic (from TypeScript)', 'Manual'],
  },
  {
    level: 2,
    title: '`onBug()`',
    url: '/onBug',
  },
  {
    level: 4,
    title: 'Client',
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
  // #endregion
] satisfies HeadingDefinition[]
