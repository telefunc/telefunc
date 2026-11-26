// The browser half of telefunc/__internal/broadcasts: an extension whose values create broadcasts imports it, so that
// the page loads Telefunc's broadcast code, and the channel code a broadcast rides, with the extension's own.
import * as channels from '../../wire-protocol/values/client/channels.js'
import * as broadcasts from '../../wire-protocol/values/client/broadcasts.js'
import { loadedWithExtensions } from '../../wire-protocol/values/client/loadFeatures.js'

loadedWithExtensions({ channels, broadcasts })
