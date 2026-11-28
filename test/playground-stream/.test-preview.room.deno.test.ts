import { runPlayground } from './.testRun'
import { testRoom } from './pages/room/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'

runPlayground('pnpm preview:deno')
testRoom()
