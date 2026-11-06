import { runPlayground } from './.testRun'
import { testSmallPending } from './pages/channel/small-pending-e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ maxPendingBytes: 2 * 1024 * 1024 })

runPlayground('pnpm preview')
testSmallPending()
