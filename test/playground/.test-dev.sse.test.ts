import { testRun } from './.testRun'
process.env.PUBLIC_ENV__TRANSPORT = 'sse'
testRun('npm run dev')
