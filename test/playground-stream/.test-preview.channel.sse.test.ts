import { testRun } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
testRun('pnpm preview')
