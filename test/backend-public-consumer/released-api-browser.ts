// Browser-condition half of the released client API snapshot.
import { ChannelClosedError, ChannelOverflowError, ConnectionError, NetworkError, withContext } from 'telefunc/client'

declare const signal: AbortSignal
const _call = withContext(async () => 1, {
  signal,
  headers: { Priority: 'u=0' },
  telefuncUrl: '/_telefunc',
  stream: { transport: 'binary-inline' },
  channel: { transports: ['sse', 'ws'], connectionKey: 'released', idleTimeout: 0 },
  extensions: { released: { enabled: true } },
})
void [_call, ChannelClosedError, ChannelOverflowError, ConnectionError, NetworkError]
