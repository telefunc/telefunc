import { testRunUpgrade } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws'])
process.env.NO_HTTPS = 'true'
testRunUpgrade('pnpm preview')
