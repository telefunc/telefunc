export { RedisConnection }

import { assert } from './assert.js'
import {
  callDefinedCommand,
  createCommandClient,
  createSubscriberSocket,
  isCluster,
  type RedisClient,
} from './ioredis.js'
import { assertKeyPrefix } from './keys.js'
import { REDIS_COMMANDS, type RedisCommand } from './commands.js'
import { RedisSubscriptionDriver } from './subscriber.js'

/** One Redis deployment as Broadcast's and Room's drivers reach it: the backend's command client, the key prefix, and
 *  the one subscriber connection their subscriptions share. */
class RedisConnection {
  readonly prefix: string
  /** A Cluster node's PUBLISH count is node-local, so it cannot prove global absence. */
  readonly reportsReceivers: boolean
  readonly subscriber: RedisSubscriptionDriver
  private commands: RedisClient
  /** The app clients installed with this backend: its command client ends once the last one does. */
  private readonly holders = new Set<RedisClient>()
  /** The last holder ended, so `commands` was disconnected; its socket may still be closing. */
  private commandsEnded = false

  constructor({ redis, prefix }: { redis: RedisClient; prefix: string }) {
    assertKeyPrefix(prefix)
    this.commands = commandClientOf(redis)
    // Pub/Sub channels span every database, so a standalone client's names carry its database.
    this.prefix = isCluster(this.commands) ? prefix : `${prefix}${this.commands.options.db ?? 0}:`
    this.reportsReceivers = !isCluster(this.commands)
    this.subscriber = new RedisSubscriptionDriver({ createSubscriber: () => createSubscriberSocket(this.commands) })
  }

  /** The command client, for the commands that need no script. */
  get client(): RedisClient {
    return this.commands
  }

  /** The backend's command client lives while `redis`, or another app client installed with it, does; a client
   *  installed after the last one ended gets a new one, as the old one's close may still be pending. */
  holdFor(redis: RedisClient): void {
    if (this.holders.has(redis)) return
    this.holders.add(redis)
    if (this.commandsEnded) {
      this.commands = commandClientOf(redis)
      this.commandsEnded = false
    }
    redis.once('end', () => {
      this.holders.delete(redis)
      if (this.holders.size > 0) return
      this.commands.disconnect()
      this.commandsEnded = true
    })
  }

  async run<Input, Output>(command: RedisCommand<Input, Output>, input: Input): Promise<Output> {
    const { keys, argv } = command.invoke(this.prefix, input)
    assert(command.numberOfKeys === null || command.numberOfKeys === keys.length)
    const keysAndArgs = command.numberOfKeys === null ? [String(keys.length), ...keys, ...argv] : [...keys, ...argv]
    const name = command.binaryReply ? `${command.name}Buffer` : command.name
    return command.parse(await callDefinedCommand(this.commands, name, keysAndArgs), input)
  }
}

/** The backend's own client for its commands, with its scripts defined. */
function commandClientOf(redis: RedisClient): RedisClient {
  const commands = createCommandClient(redis)
  for (const { name, lua, numberOfKeys } of Object.values(REDIS_COMMANDS)) {
    commands.defineCommand(name, numberOfKeys === null ? { lua } : { numberOfKeys, lua })
  }
  return commands
}
