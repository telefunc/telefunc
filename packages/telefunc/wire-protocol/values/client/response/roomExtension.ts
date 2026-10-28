import { config } from '../../../../client/clientConfig.js'
import { SERIALIZER_PREFIX_ROOM_STEM } from '../../../constants.js'

// Keeps Room's client code out of the entry bundle: only a response whose text holds a Room prefix loads it.
config.extensions.push({
  name: 'room',
  lazyResponseTypes: { stem: SERIALIZER_PREFIX_ROOM_STEM, load: async () => (await import('./room.js')).roomTypes },
})
