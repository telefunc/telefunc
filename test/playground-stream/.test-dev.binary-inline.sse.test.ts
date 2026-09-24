import { testRun } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
process.env.NO_HTTPS = 'true'
testRun('pnpm dev')
