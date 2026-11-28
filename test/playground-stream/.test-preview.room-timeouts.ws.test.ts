import { runPlayground } from './.testRun'
import { testRoomLongWaits } from './pages/room/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'

// Room's tail lease and heartbeat are fixed: these tests wait them out.
runPlayground('pnpm preview', 60_000)
testRoomLongWaits()
