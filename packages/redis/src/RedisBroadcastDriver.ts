export { RedisBroadcastDriver }

import type {
  BackendDriver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  OrderingInfo,
  PublishResult,
  SubscriptionDriver,
} from 'telefunc/__internal'
import type { RoomDriver, RoomPlaneKit } from '@telefunc/room/backend'
import { REDIS_COMMANDS } from './commands.js'
import { broadcastChannel } from './keys.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

/** The installed backend: Broadcast on `connection`, and Room's plane, built on the same connection. */
class RedisBroadcastDriver implements BroadcastDriver {
  readonly connection: RedisConnection
  readonly subscriptions: SubscriptionDriver<BroadcastRoute> = {
    bind: (route) =>
      this.connection.subscriber.bind(route, {
        laneChannel: broadcastChannel(this.connection.prefix, route),
        invalidationChannel: null,
        readLaneStarts: this.readLaneStarts,
      }),
    partitionHere: () => '',
  }
  readonly planes = {
    [Symbol.for('telefunc.room.plane')]: (kit: RoomPlaneKit): RoomDriver =>
      new RedisRoomDriver({ connection: this.connection, kit }),
  } satisfies BackendDriver['planes']
  /** The route's last publish, read once its SUBSCRIBE is acknowledged. */
  private readonly readLaneStarts = (route: BroadcastRoute): Promise<Map<string, OrderingInfo>> =>
    this.connection.run(REDIS_COMMANDS.broadcastStart, route)

  constructor({ connection }: { connection: RedisConnection }) {
    this.connection = connection
  }

  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const { seq, timestamp, receivers } = await this.connection.run(REDIS_COMMANDS.publish, { route, payload })
    return { seq, timestamp, receivers: this.connection.reportsReceivers ? receivers : undefined }
  }
}
