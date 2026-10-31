/// <reference types="@cloudflare/workers-types" />
export type { TelefuncDurableObjectNamespace }

import type { TelefuncBroadcastStub } from './broadcast.js'
import type { TelefuncExtensionStub } from './extensions.js'

/** The Telefunc Durable Object binding, typed by the RPC methods its roles call on one another. */
type TelefuncDurableObjectNamespace = {
  idFromName(name: string): DurableObjectId
  idFromString(id: string): DurableObjectId
  get(
    id: DurableObjectId,
    options?: DurableObjectNamespaceGetDurableObjectOptions,
  ): DurableObjectStub & TelefuncBroadcastStub & TelefuncExtensionStub
  jurisdiction(jurisdiction: DurableObjectJurisdiction): TelefuncDurableObjectNamespace
}
