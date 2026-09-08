import { testRun } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws'])
process.env.NO_HTTPS = 'true'
// Exercise the native Node response path; the other inline jobs use Web streams.
process.env.TELEFUNC_NATIVE = '1'
testRun('pnpm preview')
