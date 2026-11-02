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
import type { WaitRule } from './SendWait.js'

type RedisBackendOptions = { connection: RedisConnection }

/** The installed backend: Broadcast on `connection`, and Room's driver, built on the same connection. */
class RedisBackend implements BroadcastDriver, RedisDriver {
  readonly connection: RedisConnection
  readonly planes = {
    [Symbol.for('telefunc.room.plane')]: (kit: RoomPlaneKit): RoomDriver =>
      new RedisRoomDriver({ connection: this.connection, kit }),
  } satisfies BroadcastDriver['planes']

  constructor({ connection }: RedisBackendOptions) {
    this.connection = connection
  }

  /** Publishes `payload` as it is at the call. */
  publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.connection.sends.sendsMayWait ? this.publishInTurn(route, payload) : this.publishNow(route, payload)
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

  /** While one of our subscriptions is new: publishes in its turn, then lets later publishes past. */
  private async publishInTurn(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const waiting = this.connection.sends.waitTurn(broadcastWaitRule(this.connection.prefix, route), payload)
    if (waiting === null) return this.publishNow(route, payload)
    const owned = await waiting.turn
    try {
      return this.publishNow(route, owned)
    } finally {
      waiting.sent()
    }
  }

  private publishNow(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    return this.connection.run(REDIS_COMMANDS.publish, { route, payload })
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
