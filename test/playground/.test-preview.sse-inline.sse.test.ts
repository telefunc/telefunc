import { testRun } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'sse-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORT = 'sse'
testRun('npm run preview')
