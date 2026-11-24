export { ROOM_AUTHORITY_METHODS, REGION_DIRECTORY_METHODS, openRoomAuthority, openRegionDirectory }
export type { CloudflareRoomAuthorityStub, RegionDirectoryStub }

// Room's Durable Objects share the Telefunc namespace with sessions and Broadcast: a room's authority, and each
// region's directory, which lists the rooms whose authority it was first written from.

import { extensionStub, type TelefuncExtensionNamespace } from 'telefunc/__internal'
import {
  ROOM_CLOUDFLARE_EXTENSION,
  ROOM_CLOUDFLARE_PREFIX,
  ROOM_DIRECTORY_CLOUDFLARE_EXTENSION,
} from '../server/constants.js'
import type { RegionDirectory } from './RegionDirectory.js'
import type { RoomAuthority } from './RoomAuthority.js'

/** The methods a room authority's Durable Object serves, all a call reaches of it. */
const ROOM_AUTHORITY_METHODS = [
  'readHead',
  'compareExchangeHead',
  'readCells',
  'readExpiringCells',
  'readCellPage',
  'compareExchangeCells',
  'commitLane',
  'runOperation',
  'runOperationWithInbox',
  'readLaneOrder',
  'readRetained',
  'listRetained',
  'grantHolder',
  'registerRoute',
  'renewRoutes',
  'unsubscribeRoute',
  'dropIncarnation',
  'directoryShow',
  'directoryGet',
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
  return extensionStub<CloudflareRoomAuthorityStub>(
    namespace.get(id, { locationHint: bucket }),
    ROOM_CLOUDFLARE_EXTENSION,
    ROOM_AUTHORITY_METHODS,
  )
}

/** The methods a region's directory serves, all a call reaches of it. */
const REGION_DIRECTORY_METHODS = ['list', 'show'] as const satisfies readonly (keyof RegionDirectory)[]

type RegionDirectoryStub = Pick<RegionDirectory, (typeof REGION_DIRECTORY_METHODS)[number]>

/** The directory of the rooms listed in `bucket`, placed there. */
function openRegionDirectory(namespace: TelefuncExtensionNamespace, bucket: string): RegionDirectoryStub {
  // Not under ROOM_CLOUDFLARE_PREFIX, which a room id may follow with anything.
  const id = namespace.idFromName(`__telefunc_room_directory__:${bucket}`)
  const stub = namespace.get(id, { locationHint: bucket })
  return extensionStub<RegionDirectoryStub>(stub, ROOM_DIRECTORY_CLOUDFLARE_EXTENSION, REGION_DIRECTORY_METHODS)
}
