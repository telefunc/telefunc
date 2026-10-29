import { testRun } from './.testRun'
import { testRoom, testRoomAckTimeout, testRoomFailingBackend } from './pages/room/e2e-test'
import { testRoomLostFrames } from './pages/room/lost-e2e-test'
import { testVideoChat } from './pages/video-chat/e2e-test'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
testRun('pnpm preview')
testRoom()
testRoomFailingBackend()
testRoomAckTimeout()
testRoomLostFrames()
testVideoChat()
