// The browser half of telefunc/__internal: what extension code shared with the browser imports. An extension whose
// values create channels or broadcasts imports it, so that the page loads Telefunc's code for them with the extension's own.
export { isPeerGone } from '../node/server/runTelefunc/isPeerGone.js'

import * as channels from '../wire-protocol/values/client/channels.js'
import * as broadcasts from '../wire-protocol/values/client/broadcasts.js'
import { loadedWithExtensions } from '../wire-protocol/values/client/loadFeatures.js'

loadedWithExtensions({ channels, broadcasts })
