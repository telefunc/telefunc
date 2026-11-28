import { runPlayground } from './.testRun'
import { testPublishStartup } from './pages/publish/e2e-test'
import { testRoomListenerContext } from './pages/room/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.TEST_ASYNC_CONTEXT = '1'
process.env.CHANNEL_CONFIG = JSON.stringify({ maxMemory: 2 * 1024 * 1024 })

runPlayground('pnpm preview')
testRoomListenerContext(true)
testPublishStartup()
