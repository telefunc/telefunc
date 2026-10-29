import { runPlayground } from './.testRun'
import { testWireSmallReplay } from './pages/channel/wire-e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ serverReplayBuffer: 65_536, clientReplayBuffer: 65_536 })

runPlayground('pnpm preview')
testWireSmallReplay()
