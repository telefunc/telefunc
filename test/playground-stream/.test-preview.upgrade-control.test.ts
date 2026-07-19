// Control for `.test-preview.upgrade.test.ts` — see `.test-dev.upgrade-control.test.ts`.
import { testRunUpgrade } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
testRunUpgrade('pnpm preview')
