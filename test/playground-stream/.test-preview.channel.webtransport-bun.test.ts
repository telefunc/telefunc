import { runPlayground } from './.testRun'
import { testWebTransport } from './pages/webtransport/e2e-test'
import { testRoom } from './pages/room/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws', 'webtransport'])
process.env.NO_HTTPS = 'true'

runPlayground('pnpm preview:bun')
testWebTransport()
testRoom()
