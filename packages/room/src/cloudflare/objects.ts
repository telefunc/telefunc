export { openRoomAuthority, openRegionDirectory }

// Room's Durable Objects share the Telefunc namespace with sessions and Broadcast: a room's authority, and each
// region's directory, which lists the rooms whose authority it was first written from.

import type { TelefuncExtensionNamespace, TelefuncExtensionStub } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_EXTENSION, ROOM_CLOUDFLARE_PREFIX } from '../server/constants.js'
import type { CloudflareRoomAuthorityStub } from './RoomAuthority.js'

/** `roomId`'s authority, placed in `bucket` if this `get()` creates it: Cloudflare places a Durable Object on its first
 *  `get()` and never moves it. */
function openRoomAuthority(
  namespace: TelefuncExtensionNamespace,
  roomId: string,
  bucket: string | null,
): CloudflareRoomAuthorityStub {
  const id = namespace.idFromName(ROOM_CLOUDFLARE_PREFIX + roomId)
  return roomObjectStub(namespace.get(id, bucket === null ? undefined : { locationHint: bucket }))
}

/** The directory of the rooms listed in `bucket`, placed there. */
function openRegionDirectory(namespace: TelefuncExtensionNamespace, bucket: string): CloudflareRoomAuthorityStub {
  // Not under ROOM_CLOUDFLARE_PREFIX, which a room id may follow with anything.
  const id = namespace.idFromName(`__telefunc_room_directory__:${bucket}`)
  return roomObjectStub(namespace.get(id, { locationHint: bucket }))
}

/** Room's methods of a Durable Object, called through its `telefuncDurableObjectCall()`. */
function roomObjectStub(stub: TelefuncExtensionStub): CloudflareRoomAuthorityStub {
  return new Proxy({} as CloudflareRoomAuthorityStub, {
    get:
      (_, method: string) =>
      (...args: unknown[]) =>
        stub.telefuncDurableObjectCall(ROOM_CLOUDFLARE_EXTENSION, method, args),
  })
}
