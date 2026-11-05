/// <reference types="@cloudflare/workers-types" />

// Room on Cloudflare: a room authority, and each region's room directory, are Room's part of the Telefunc Durable
// Object, its frames reach session Durable Objects through Broadcast's regions, and Room's storage driver is
// `CloudflareRoomDriver`.

import { registerCloudflareExtension, serveCloudflarePlane } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_EXTENSION } from '../server/constants.js'
import { ROOM_PLANE } from '../backend/plane.js'
import { RoomAuthority } from './RoomAuthority.js'
import { ROOM_AUTHORITY_METHODS } from './objects.js'
import { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

registerCloudflareExtension({
  name: ROOM_CLOUDFLARE_EXTENSION,
  durableObject: (ctx: DurableObjectState, _env, kit) => {
    const authority = new RoomAuthority(ctx, kit)
    const methods = Object.fromEntries(
      ROOM_AUTHORITY_METHODS.map((method) => [method, authority[method].bind(authority)]),
    )
    return { methods, alarm: () => authority.alarm() }
  },
  planes: [serveCloudflarePlane(ROOM_PLANE, (kit) => new CloudflareRoomDriver(kit))],
})
