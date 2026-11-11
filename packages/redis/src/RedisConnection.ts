export { RedisConnection }

import { assert } from './assert.js'
import {
  addressOf,
  callDefinedCommand,
  createCommandClient,
  isUnreachable,
  createSubscriberSocket,
  isCluster,
  type RedisClient,
  type SubscriberSocket,
} from './ioredis.js'
import { assertKeyPrefix } from './keys.js'
import { REDIS_COMMANDS, type RedisCommand } from './commands.js'
import { NewSubscriptionWait } from './NewSubscriptionWait.js'
import { RedisSubscriptions } from './RedisSubscriptions.js'

type RedisConnectionOptions = { redis: RedisClient; prefix: string }

/** One Redis deployment as the Broadcast and Room drivers reach it: the backend's command client, the key prefix, and
 *  the one subscriber connection their subscriptions share. */
class RedisConnection {
  readonly prefix: string
  readonly subscriptions: RedisSubscriptions
  /** Broadcast's and Room's: their queues and budgets are channel names, which never collide. */
  readonly sends: NewSubscriptionWait
  private commands: RedisClient
  /** The app clients installed with this backend: its command client ends once the last one does. */
  private readonly holders = new Set<RedisClient>()
  /** The last holder ended, so `commands` was disconnected; its socket may still be closing. */
  private commandsEnded = false

  constructor({ redis, prefix }: RedisConnectionOptions) {
    assertKeyPrefix(prefix)
    this.commands = commandClientOf(redis)
    // Pub/Sub channels span every database, so a standalone client's names carry its database.
    this.prefix = isCluster(this.commands) ? prefix : `${prefix}${this.commands.options.db ?? 0}:`
    this.subscriptions = new RedisSubscriptions({ connection: this })
    this.sends = new NewSubscriptionWait({ subscriptions: this.subscriptions })
  }

  /** A fresh, unconnected subscriber socket on the current command client, which never retries on its own. */
  openSubscriberSocket(): Promise<SubscriberSocket> {
    return createSubscriberSocket(this.commands)
  }

  /** The command client, for the commands that need no script: sent through `send()`. */
  get client(): RedisClient {
    return this.commands
  }

  /** Sends `command` on the command client. A Redis it can't reach rejects with where it looked, ioredis' error as the
   *  cause: that error names a retry limit the backend set, not the app. */
  async send<Output>(command: (client: RedisClient) => Promise<Output>): Promise<Output> {
    try {
      return await command(this.commands)
    } catch (error) {
      if (!isUnreachable(error)) throw error
      throw new Error(`Telefunc's Redis backend can't reach Redis at ${addressOf(this.commands)}`, { cause: error })
    }
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
    return command.parse(await this.send((client) => callDefinedCommand(client, name, keysAndArgs)), input)
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
