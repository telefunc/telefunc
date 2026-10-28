import { runPlayground } from './.testRun'
import { testRoom } from './pages/room/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'

// Room on the compose stack's Redis. testRoomFailingBackend() stays with the in-memory lanes: a server with Redis
// installs no failing backend to inject its store faults.
runPlayground('pnpm preview:redis')
testRoom()
