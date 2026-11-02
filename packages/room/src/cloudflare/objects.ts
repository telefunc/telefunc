export { ROOM_AUTHORITY_METHODS, openRoomAuthority, openRegionDirectory }
export type { CloudflareRoomAuthorityStub, RegionDirectoryStub }

// Room's Durable Objects share the Telefunc namespace with sessions and Broadcast: a room's authority, and each
// region's directory, which lists the rooms whose authority it was first written from.

import type { TelefuncExtensionNamespace, TelefuncExtensionStub } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_EXTENSION, ROOM_CLOUDFLARE_PREFIX } from '../server/constants.js'
import type { RoomAuthority } from './RoomAuthority.js'

/** The methods a room authority's Durable Object serves, all a call reaches of it. */
const ROOM_AUTHORITY_METHODS = [
  'readHead',
  'compareExchangeHead',
  'readCells',
  'readCellPage',
  'compareExchangeCells',
  'commitLane',
  'runOperation',
  'readLaneOrder',
  'readRetained',
  'listRetained',
  'registerRoute',
  'renewRoute',
  'unsubscribeRoute',
  'dropIncarnation',
  'directoryPut',
  'directoryDelete',
  'directoryGet',
  'directoryList',
  'listInRegion',
  'unlistInRegion',
] as const satisfies readonly (keyof RoomAuthority)[]

type CloudflareRoomAuthorityStub = Pick<RoomAuthority, (typeof ROOM_AUTHORITY_METHODS)[number]>

/** `roomId`'s authority, placed in `bucket` if this `get()` creates it: Cloudflare places a Durable Object on its first
 *  `get()` and never moves it. */
function openRoomAuthority(
  namespace: TelefuncExtensionNamespace,
  roomId: string,
  bucket: string,
): CloudflareRoomAuthorityStub {
  const id = namespace.idFromName(ROOM_CLOUDFLARE_PREFIX + roomId)
  return roomObjectStub(namespace.get(id, { locationHint: bucket }))
}

/** The methods a region's directory serves. */
type RegionDirectoryStub = Pick<CloudflareRoomAuthorityStub, 'directoryList' | 'listInRegion' | 'unlistInRegion'>

/** The directory of the rooms listed in `bucket`, placed there. */
function openRegionDirectory(namespace: TelefuncExtensionNamespace, bucket: string): RegionDirectoryStub {
  // Not under ROOM_CLOUDFLARE_PREFIX, which a room id may follow with anything.
  const id = namespace.idFromName(`__telefunc_room_directory__:${bucket}`)
  return roomObjectStub(namespace.get(id, { locationHint: bucket }))
}

/** Room's methods of a Durable Object, called through its `telefuncDurableObjectCall()`. */
function roomObjectStub(stub: TelefuncExtensionStub): CloudflareRoomAuthorityStub {
  const call =
    (method: string) =>
    (...args: unknown[]) =>
      stub.telefuncDurableObjectCall(ROOM_CLOUDFLARE_EXTENSION, method, args)
  return Object.fromEntries(
    ROOM_AUTHORITY_METHODS.map((method) => [method, call(method)]),
  ) as CloudflareRoomAuthorityStub
}
