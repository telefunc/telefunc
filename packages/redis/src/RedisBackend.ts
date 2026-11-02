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
import { planeKey, servePlane } from 'telefunc/__internal'
import type { RoomPlaneKey } from '@telefunc/room/backend'
import { REDIS_COMMANDS } from './commands.js'
import { broadcastChannel } from './keys.js'
import type { RedisConnection } from './RedisConnection.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'
import type { RedisDriver } from './RedisSubscriptions.js'
import type { WaitingSend, WaitRule } from './NewSubscriptionWait.js'

type RedisBackendOptions = { connection: RedisConnection }

/** The installed backend: Broadcast on `connection`, and Room's driver, built on the same connection. */
class RedisBackend implements BroadcastDriver, RedisDriver {
  readonly connection: RedisConnection
  readonly planes = [
    servePlane(
      planeKey<RoomPlaneKey>('telefunc.room.plane'),
      (kit) => new RedisRoomDriver({ connection: this.connection, kit }),
    ),
  ]

  constructor({ connection }: RedisBackendOptions) {
    this.connection = connection
  }

  /** Publishes `payload` as it is at the call. */
  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const { sends, prefix } = this.connection
    const waiting = sends.sendsMayWait ? sends.waitTurn(broadcastWaitRule(prefix, route), payload) : null
    if (waiting === null) return this.connection.run(REDIS_COMMANDS.publish, { route, payload })
    return this.publishInTurn(route, waiting)
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

  /** Publishes in its turn while one of our subscriptions is new, then lets later publishes past. */
  private async publishInTurn(route: BroadcastRoute, waiting: WaitingSend<BroadcastPayload>): Promise<PublishResult> {
    const payload = await waiting.turn
    try {
      return this.connection.run(REDIS_COMMANDS.publish, { route, payload })
    } finally {
      waiting.sent()
    }
  }

  /** The route's last publish, read once its SUBSCRIBE is acknowledged. */
  readStarts(route: BroadcastRoute): Promise<Map<string, OrderingInfo>> {
    return this.connection.run(REDIS_COMMANDS.broadcastStart, route)
  }
}

/** A key's two kinds share its order, so a publish of either waits behind the key's waiting ones. */
function broadcastWaitRule(prefix: string, { key, kind }: BroadcastRoute): WaitRule {
  const text = broadcastChannel(prefix, { key, kind: 'text' })
  const binary = broadcastChannel(prefix, { key, kind: 'binary' })
  const bufferLimit = kind === 'binary' ? 'bufferLimitBinary' : 'bufferLimit'
  return { queue: text, channels: [text, binary], budget: kind === 'text' ? text : binary, bufferLimit }
}
