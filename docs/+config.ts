export { config }

import docpress from '@brillout/docpress/config'
import type { Config } from 'vike/types'

const config = {
  extends: docpress,
  choices: {
    server: {
      choices: ['Hono', 'Express', 'Fastify'],
      default: 'Hono',
    },
    uiFrameworks: {
      choices: ['react', 'vue', 'solid', 'svelte'],
      default: 'solid',
    },
  },
} satisfies Config
