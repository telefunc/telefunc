// T5 — cross-instance live-query invalidation (@telefunc/drizzle-experimental). Its own runner entry rather than a case
// in `.testRun.ts`: the reactive path is independent of the stream transport, so folding it into the shared
// suite would run it once per transport permutation for no added coverage.

import { run } from '@brillout/test-e2e'
import { testReactive } from './pages/reactive/e2e-test'

// The live handle rides telefunc's stream transport, so one has to be selected explicitly: unset,
// `pages/+client.ts` defaults the channel to SSE, which fails ALPN negotiation against the plain-HTTP dev
// server here. Matches `.test-dev.channel.ws.test.ts`.
process.env.PUBLIC_ENV__STREAM_TRANSPORT = 'channel'
process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS = JSON.stringify(['ws'])
process.env.NO_HTTPS = 'true'

run('pnpm dev', {
  // `vike dev` prints `Local: http://localhost:3000`, which test-e2e's default matcher doesn't recognise.
  serverIsReadyMessage: (log: string) =>
    log.includes('Listening on') ||
    log.includes('Server running at') ||
    (log.includes('Local:') && log.includes('http://localhost:3000')),
})

testReactive()
