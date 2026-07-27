// Browser-condition half of the released client API snapshot.
import { ChannelClosedError, ChannelOverflowError, ConnectionError, NetworkError, withContext } from 'telefunc/client'

const _call = withContext(async () => 1, { channel: { idleTimeout: 0 } })
void [_call, ChannelClosedError, ChannelOverflowError, ConnectionError, NetworkError]
