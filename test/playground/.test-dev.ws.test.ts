import { testRun } from './.testRun'
process.env.PUBLIC_ENV__TRANSPORT = 'ws'
testRun('npm run dev')
