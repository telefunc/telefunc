import { runPlayground } from './.testRun'
import { testWebTransport } from './pages/webtransport/e2e-test'
import { checkWire } from './laneWire'
import { testRoom } from './pages/room/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws', 'webtransport'])
process.env.NO_HTTPS = 'true'
// Each test checks, from its pages, that their channels ran on WebTransport and each Room track on a stream of its own.
checkWire({ wire: 'webtransport', tracksOnOwnStreams: true })

runPlayground('pnpm preview:bun')
testWebTransport()
testRoom()
