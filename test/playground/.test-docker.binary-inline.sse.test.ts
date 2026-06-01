import { test, skip } from '@brillout/test-e2e'
import { testRunDocker } from './.testRun-docker'

if (process.platform === 'win32') {
  test('docker compose cluster (Linux only)', () => skip('Docker compose stack requires Linux containers'))
} else {
  process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'binary-inline'
  process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['sse'])
  testRunDocker()
}
