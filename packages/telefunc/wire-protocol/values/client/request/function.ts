export { functionType }

import type { FunctionContract } from '../../types.js'
import type { ChannelCarriedType } from './registry.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../../constants.js'
import { randomUuid } from '../../../../utils/randomUuid.js'

const functionType: ChannelCarriedType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect: (value): value is FunctionContract['value'] => typeof value === 'function',
  claim(fn) {
    const channelId = randomUuid()
    return {
      metadata: { channelId },
      open({ ClientChannel }, connection) {
        const channel = new ClientChannel<unknown, readonly unknown[]>({ channelId, ack: true, connection })
        channel.listen((args) => fn(...args))
        return {
          async close() {
            await channel.close()
          },
          abort() {
            channel.abort()
          },
        }
      },
      release() {},
    }
  },
}
