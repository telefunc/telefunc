import { runPlayground } from './.testRun'
import { testSlowPost } from './pages/channel/slow-post-e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
process.env.CHANNEL_CONFIG = JSON.stringify({ sseFlushThrottle: 1000 })

runPlayground('pnpm preview')
testSlowPost()
