import { runPlayground } from './.testRun'
import { testClose } from './pages/close/e2e-test'
import { testAbort } from './pages/abort/e2e-test'
import { testFunction } from './pages/function/e2e-test'

process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse', 'ws'])
process.env.NO_HTTPS = 'true'
process.env.TELEFUNC_NATIVE = 'readable'

runPlayground('pnpm preview')
testClose()
testAbort()
testFunction(false)
