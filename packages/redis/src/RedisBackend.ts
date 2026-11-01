export { RedisBackend }

import type {
  Backend,
  BroadcastPayload,
  BroadcastRoute,
  OrderingInfo,
  PublishResult,
  Subscriber,
  Unsubscribe,
} from 'telefunc/__internal'
import type { RoomDriver, RoomPlaneKit } from '@telefunc/room/backend'
import { REDIS_COMMANDS } from './commands.js'
import { broadcastChannel } from './keys.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'
import type { RedisPlane } from './RedisSubscriptions.js'

/** The installed backend: Broadcast on `connection`, and Room's plane, built on the same connection. */
class RedisBackend implements Backend, RedisPlane {
  readonly connection: RedisConnection
  readonly planes = {
    [Symbol.for('telefunc.room.plane')]: (kit: RoomPlaneKit): RoomDriver =>
      new RedisRoomDriver({ connection: this.connection, kit }),
  } satisfies Backend['planes']

  constructor({ connection }: { connection: RedisConnection }) {
    this.connection = connection
  }

  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const { hold } = this.connection
    const held = hold.active ? hold.holdBroadcast(route, payload) : null
    if (held !== null) payload = await held
    const { seq, timestamp, receivers } = await this.connection.run(REDIS_COMMANDS.publish, { route, payload })
    return { seq, timestamp, receivers: this.connection.reportsReceivers ? receivers : undefined }
  }

  subscribe(route: BroadcastRoute, subscriber: Subscriber): Unsubscribe {
    const laneChannel = broadcastChannel(this.connection.prefix, route)
    return this.connection.subscriptions.subscribe({
      source: route,
      laneChannel,
      invalidationChannel: null,
      plane: this,
      subscriber,
    })
  }

  /** The route's last publish, read once its SUBSCRIBE is acknowledged. */
  readLaneStarts(route: BroadcastRoute): Promise<Map<string, OrderingInfo>> {
    return this.connection.run(REDIS_COMMANDS.broadcastStart, route)
  }
}
