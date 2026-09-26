import { testRun } from './.testRun'
import { testRoom } from './pages/room/e2e-test'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
testRun('pnpm preview')
testRoom()
