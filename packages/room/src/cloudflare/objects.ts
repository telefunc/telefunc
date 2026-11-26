export {
  ROOM_AUTHORITY_METHODS,
  REGION_DIRECTORY_METHODS,
  ROOM_JOIN_DEADLINE_MS,
  openRoomAuthority,
  openRegionDirectory,
}
export type { CloudflareRoomAuthorityStub, RegionDirectoryStub }

// Room's Durable Objects share the Telefunc namespace with sessions and Broadcast: a room's authority, and each
// region's directory, which lists the rooms whose authority it was first written from.

import { LEASE_CALL_DEADLINE_MS, extensionStub, type TelefuncExtensionNamespace } from 'telefunc/__internal'
import {
  ROOM_HORIZON_MS,
  ROOM_CLOUDFLARE_EXTENSION,
  ROOM_CLOUDFLARE_PREFIX,
  ROOM_DIRECTORY_CLOUDFLARE_EXTENSION,
} from '../server/constants.js'
import type { RegionDirectory } from './RegionDirectory.js'
import type { RoomAuthority } from './RoomAuthority.js'

/** How long a session waits for a room's authority to answer a member's join, which carries the member's inbox: the
 *  authority's wait for the join's owed hand-on, the horizon, then a call's own. */
const ROOM_JOIN_DEADLINE_MS = ROOM_HORIZON_MS + LEASE_CALL_DEADLINE_MS

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
  'runJoinWithInbox',
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
