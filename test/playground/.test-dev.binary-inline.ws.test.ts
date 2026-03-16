import { testRun } from './.testRun'
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORT = 'ws'
testRun('npm run dev')
