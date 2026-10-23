import { runPlayground } from './.testRun'
import { testWireSlowPing } from './pages/channel/wire-e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ pingInterval: 7_000 })

runPlayground('pnpm preview')
testWireSlowPing()
