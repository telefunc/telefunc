import { runPlayground } from './.testRun'
import { testSseRecoveryRefused } from './pages/channel/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ reconnectTimeout: 2_000 })

runPlayground('pnpm preview')
testSseRecoveryRefused()
