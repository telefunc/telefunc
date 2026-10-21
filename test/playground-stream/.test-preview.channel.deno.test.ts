import { runPlayground } from './.testRun'
import { testChannel } from './pages/channel/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'

runPlayground('pnpm preview:deno')
testChannel(false)
