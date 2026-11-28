export { openChannel }

import { ConnectionError } from 'telefunc/client'

const OPEN_CHANNEL_ATTEMPTS = 4
const OPEN_CHANNEL_RETRY_DELAY_MS = 500

/**
 * Establishes a channel, retrying if the handshake's HTTP round-trip is dropped.
 *
 * Opening a channel is a one-shot telefunc call. In the Docker e2e environment, Chromium's
 * NetworkChangeNotifier aborts in-flight requests with a transient `ConnectionError` when the Docker
 * bridge interface shifts — most notably right after a reconnect test restarts the proxy container.
 * If that abort lands on a channel handshake the channel never comes back, so retry until it does.
 *
 * This is the channel-handshake half of the suite's Docker-resilience story; the navigation half
 * lives in `navigate()` (../../e2e-utils.ts). Steady-state message delivery needs no such retry —
 * telefunc's own reconnect/replay covers it. Retrying the handshake is safe: each attempt opens an
 * independent channel, so a superseded half-open one merely gets reaped server-side after its grace
 * period.
 */
async function openChannel<T>(open: () => Promise<T>): Promise<T> {
  let lastErr: unknown
  for (let attempt = 1; attempt <= OPEN_CHANNEL_ATTEMPTS; attempt++) {
    try {
      return await open()
    } catch (err) {
      if (!(err instanceof ConnectionError)) throw err
      lastErr = err
      await new Promise((resolve) => setTimeout(resolve, OPEN_CHANNEL_RETRY_DELAY_MS))
    }
  }
  throw lastErr
}
