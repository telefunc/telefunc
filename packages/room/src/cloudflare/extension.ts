/// <reference types="@cloudflare/workers-types" />

// Room on Cloudflare: a room authority, and the room directory, are a role of the Telefunc Durable Object, a session
// Durable Object takes the frames a room authority delivers to it, and Room's storage driver is the Cloudflare
// backend's Room plane.

import { registerCloudflareExtension } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_EXTENSION } from '../server/constants.js'
import { ROOM_PLANE } from '../backend/plane.js'
import { RoomAuthority } from './do.js'
import { CloudflareRoomDriver } from './driver.js'
import { getRoomSessionManager, type RoomSessionDeliveryRequest } from './subscription.js'

registerCloudflareExtension({
  name: ROOM_CLOUDFLARE_EXTENSION,
  role: (ctx: DurableObjectState, _env, namespace) => new RoomAuthority(ctx, namespace()),
  session: { deliver: (request: RoomSessionDeliveryRequest) => getRoomSessionManager().deliver(request) },
  planes: { [ROOM_PLANE]: (namespace) => new CloudflareRoomDriver({ namespace }) },
})
