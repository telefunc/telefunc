import { testRun } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'sse-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
testRun('pnpm preview')
