import { runPlayground } from './.testRun'
import { testRoomSmallBuffer } from './pages/room/e2e-test'
import { testPublishSmallBuffer } from './pages/publish/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ bufferLimit: 4096 })

runPlayground('pnpm preview')
testPublishSmallBuffer()
testRoomSmallBuffer()
