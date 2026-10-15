import type { TelefuncDurableObjectNamespace } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/namespace.js'
import type { RoomSessionNamespace } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/fanout.js'

declare global {
  var __TELEFUNC__IS_NON_RUNNABLE_DEV: undefined | true
  var __TELEFUNC__DYNAMIC_IMPORT: (module: `virtual:${string}`) => Promise<Record<string, unknown>>
  namespace Cloudflare {
    interface Env {
      // The adapter reads a binding by the name its options give.
      [binding: string]: unknown
      ROOM: DurableObjectNamespace
      TelefuncDurableObject: DurableObjectNamespace & RoomSessionNamespace
      TELEFUNC: TelefuncDurableObjectNamespace
    }
  }
}
