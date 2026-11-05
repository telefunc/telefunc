import { runPlayground } from './.testRun'
import { testZeroSettings } from './pages/channel/zero-settings-e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({
  bufferLimit: 0,
  bufferLimitBinary: 0,
  serverReplayBuffer: 0,
  clientReplayBuffer: 0,
  serverReplayBufferBinary: 0,
  clientReplayBufferBinary: 0,
})

runPlayground('pnpm preview')
testZeroSettings()
