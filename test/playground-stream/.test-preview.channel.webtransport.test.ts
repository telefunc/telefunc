import { runPlayground } from './.testRun'
import { testWebTransport } from './pages/webtransport/e2e-test'
import { testRoom } from './pages/room/e2e-test'
import { testRoomLostFrames } from './pages/room/lost-e2e-test'
import { testVideoChat } from './pages/video-chat/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws', 'webtransport'])
process.env.NO_HTTPS = 'true'

runPlayground('pnpm preview')
testWebTransport()
// Room's tracks, each on a domain of its own, so each on a WebTransport stream of its own.
testRoom()
testRoomLostFrames()
testVideoChat()
