import { runPlayground } from './.testRun'
import { testWireShortReconnect } from './pages/channel/wire-e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ reconnectTimeout: 2_000 })

runPlayground('pnpm preview')
testWireShortReconnect()
