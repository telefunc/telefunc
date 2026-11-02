export { RedisBackend }

import type {
  BackendReceiver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  DriverAttempt,
  OrderingInfo,
  PublishResult,
} from 'telefunc/__internal'
import type { RoomDriver, RoomPlaneKit } from '@telefunc/room/backend'
import { REDIS_COMMANDS } from './commands.js'
import { broadcastChannel } from './keys.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'
import type { RedisDriver } from './RedisSubscriptions.js'

type RedisBackendOptions = { connection: RedisConnection }

/** The installed backend: Broadcast on `connection`, and Room's driver, built on the same connection. */
class RedisBackend implements BroadcastDriver, RedisDriver {
  readonly connection: RedisConnection
  readonly publishOvertakesSubscribe = true
  readonly planes = {
    [Symbol.for('telefunc.room.plane')]: (kit: RoomPlaneKit): RoomDriver =>
      new RedisRoomDriver({ connection: this.connection, kit }),
  } satisfies BroadcastDriver['planes']

  constructor({ connection }: RedisBackendOptions) {
    this.connection = connection
  }

  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const { seq, timestamp, receivers } = await this.connection.run(REDIS_COMMANDS.publish, { route, payload })
    return { seq, timestamp, receivers: this.connection.reportsReceivers ? receivers : undefined }
  }

  subscribe(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    const channel = broadcastChannel(this.connection.prefix, route)
    return this.connection.subscriptions.subscribe({
      source: route,
      channel,
      invalidationChannel: null,
      driver: this,
      receiver,
    })
  }

  /** The route's last publish, read once its SUBSCRIBE is acknowledged. */
  readStarts(route: BroadcastRoute): Promise<Map<string, OrderingInfo>> {
    return this.connection.run(REDIS_COMMANDS.broadcastStart, route)
  }
}
