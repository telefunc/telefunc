export { config as default }

import type { Config } from '@brillout/docpress'
import logo from './icons/telefunc.svg'
import iconNode from './icons/runtime/node.svg'
import iconExpress from './icons/runtime/express.svg'
import iconBun from './icons/runtime/bun.svg'
import iconDeno from './icons/runtime/deno.svg'
import iconCloudflare from './icons/runtime/cloudflare.svg'
import { categories, headings, headingsDetached } from './headings'
import { PROJECT_VERSION } from './utils/PROJECT_VERSION.js'

const config: Config = {
  name: 'Telefunc',
  darkMode: true,
  tocProgress: true,
  categoryTabs: true,
  version: PROJECT_VERSION,
  url: 'https://telefunc.com',
  tagline: 'Remote Functions.',
  logo,

  github: 'https://github.com/telefunc/telefunc',
  discord: 'https://discord.gg/VJKjMNMguV',
  twitter: '@brillout',

  headings,
  headingsDetached,
  categories,

  algolia: {
    appId: 'NONXS2JSTL',
    apiKey: '9bf6a6f9bc168ca425e8e19a62cd8ba1',
    indexName: 'telefunc',
  },

  umamiId: 'd03d0873-19dc-42c4-a250-cf8500171a9e',

  // Globally-synced runtime toggle (used by /Telefunc).
  choices: {
    runtime: {
      choices: [
        { name: 'Node', icon: iconNode },
        { name: 'Express', icon: iconExpress, iconMono: true },
        { name: 'Bun', icon: iconBun },
        { name: 'Deno', icon: iconDeno, iconMono: true },
        { name: 'Cloudflare', icon: iconCloudflare },
      ],
      default: 'Node',
    },
  },

  navMaxWidth: 950 + 20 * 2,
  navLogoSize: 36,
  navLogoStyle: {
    position: 'relative',
    top: -1,
  },
} satisfies Config
