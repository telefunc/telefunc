import { runPlayground } from './.testRun'
import { testRoomPublishHold } from './pages/room/e2e-test'
import { testPublishSmallBuffer } from './pages/publish/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ bufferLimit: 4096 })

// A publish waits while the server's own new subscriptions on its key establish (Redis's SUBSCRIBE round trip),
// within bufferLimit.
runPlayground('pnpm preview:redis')
testPublishSmallBuffer()
testRoomPublishHold()
