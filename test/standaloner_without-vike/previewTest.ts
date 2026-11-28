export { runPreview }

import { run } from '@brillout/test-e2e'

function runPreview(command: 'pnpm run preview' | 'pnpm run preview:manual') {
  run(command, {
    tolerateError: ({ logText }) => logText.includes('[build:warning]') && logText.includes('skipped: missing name.'),
  })
}
