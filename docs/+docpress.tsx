export { config as default }

import type { Config } from '@brillout/docpress'
import logo from './icons/telefunc.svg'
import { categories, headings } from './headings'
import { PROJECT_VERSION } from './utils/PROJECT_VERSION.js'

const config: Config = {
  name: 'Telefunc',
  version: PROJECT_VERSION,
  url: 'https://telefunc.com',
  tagline: 'Remote Functions.',
  logo,

  github: 'https://github.com/telefunc/telefunc',
  discord: 'https://discord.gg/VJKjMNMguV',
  twitter: '@brillout',

  headings,
  headingsDetached: [],
  categories,

  algolia: {
    appId: 'NONXS2JSTL',
    apiKey: '9bf6a6f9bc168ca425e8e19a62cd8ba1',
    indexName: 'telefunc',
  },

  umamiId: 'd03d0873-19dc-42c4-a250-cf8500171a9e',

  navMaxWidth: 950 + 20 * 2,
  navLogoSize: 36,
  navLogoStyle: {
    position: 'relative',
    top: -1,
  },
} satisfies Config
