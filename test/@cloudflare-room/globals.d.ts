import type { TelefuncDurableObjectNamespace } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/namespace.js'
import type { RoomSessionNamespace } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/fanout.js'

declare global {
  var __TELEFUNC__IS_NON_RUNNABLE_DEV: undefined | true
  namespace Cloudflare {
    interface Env {
      ROOM: DurableObjectNamespace
      TelefuncDurableObject: DurableObjectNamespace & RoomSessionNamespace
      PUBLIC: TelefuncDurableObjectNamespace
    }
  }
}
