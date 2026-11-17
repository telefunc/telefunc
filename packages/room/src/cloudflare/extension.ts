/// <reference types="@cloudflare/workers-types" />

// Room on Cloudflare: a room authority, and each region's room directory, are Room's parts of the Telefunc Durable
// Object, its frames reach session Durable Objects through Broadcast's regions, and Room's storage driver is
// `CloudflareRoomDriver`.

import { registerCloudflareExtension, serveCloudflarePlane } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_EXTENSION, ROOM_DIRECTORY_CLOUDFLARE_EXTENSION } from '../server/constants.js'
import { ROOM_PLANE } from '../backend/plane.js'
import { RegionDirectory } from './RegionDirectory.js'
import { RoomAuthority } from './RoomAuthority.js'
import { REGION_DIRECTORY_METHODS, ROOM_AUTHORITY_METHODS } from './objects.js'
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

registerCloudflareExtension({
  name: ROOM_DIRECTORY_CLOUDFLARE_EXTENSION,
  durableObject: (ctx: DurableObjectState) => {
    const directory = new RegionDirectory(ctx)
    return {
      methods: Object.fromEntries(
        REGION_DIRECTORY_METHODS.map((method) => [method, directory[method].bind(directory)]),
      ),
    }
  },
})
