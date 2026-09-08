import { runPlayground } from './.testRun'
import { testClose } from './pages/close/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws'])
process.env.NO_HTTPS = 'true'
process.env.TELEFUNC_NATIVE = '1'

runPlayground('pnpm preview')
testClose()
