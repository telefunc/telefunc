// Control for `.test-dev.upgrade.test.ts`: identical traffic and identical oracle, with the one
// line that enables the upgrade removed. The commit counter must stay at zero here — without it,
// a counter stuck high would satisfy the positive test.
import { testRunUpgrade } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
testRunUpgrade('pnpm dev')
