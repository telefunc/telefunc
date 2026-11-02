/// <reference types="@cloudflare/workers-types" />

// Room on Cloudflare: a room authority, and the room directory, are Room's part of the Telefunc Durable Object, its
// frames reach session Durable Objects through Broadcast's regions, and Room's storage driver is the Cloudflare backend's
// Room plane.

import { registerCloudflareExtension } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_EXTENSION } from '../server/constants.js'
import { ROOM_PLANE } from '../backend/plane.js'
import { RoomAuthority } from './do.js'
import { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

registerCloudflareExtension({
  name: ROOM_CLOUDFLARE_EXTENSION,
  durableObject: (ctx: DurableObjectState, _env, regions) => new RoomAuthority(ctx, regions),
  planes: { [ROOM_PLANE]: (getNamespace) => new CloudflareRoomDriver({ getNamespace }) },
})
