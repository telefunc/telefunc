// Internal API surface — minimal set consumed by substrate-implementor packages
// (`@telefunc/redis`, `@telefunc/cloudflare`, …). Not for end users.

export {
  decodeProxyEnvelope,
  dispatchEnvelope,
  encodeProxyEnvelope,
  ENVELOPE_KIND,
  PROXY_DIRECTION,
} from '../../wire-protocol/server/substrate.js'

export type {
  ChannelSubstrate,
  ChannelSubstrateHandlers,
  ProxyEnvelope,
} from '../../wire-protocol/server/substrate.js'
