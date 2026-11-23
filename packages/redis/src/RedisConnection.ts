export { RedisConnection }

import { assert } from './assert.js'
import {
  callDefinedCommand,
  connectsAlike,
  createCommandClient,
  createSubscriberSocket,
  isCluster,
  redisFailure,
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
  /** The app client `commands` was made from. */
  private source: RedisClient
  /** The app clients installed with this backend, with their `end` listeners: its command client ends once the last one
   *  does. */
  private readonly holders = new Map<RedisClient, () => void>()
  /** The last holder ended, so `commands` was disconnected; its socket may still be closing. */
  private commandsEnded = false

  constructor({ redis, prefix }: RedisConnectionOptions) {
    assertKeyPrefix(prefix)
    this.source = redis
    this.commands = this.makeCommandClient(redis)
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

  /** Sends `command` on the command client; a failure of Redis rejects as the backend reports it (`redisFailure()`). */
  async send<Output>(command: (client: RedisClient) => Promise<Output>): Promise<Output> {
    try {
      return await command(this.commands)
    } catch (error) {
      throw redisFailure(error, this.commands)
    }
  }

  /** The backend's command client lives while `redis`, or another app client installed with it, does; a client
   *  installed after the last one ended gets a new one, as the old one's close may still be pending. A client that
   *  connects otherwise, with other credentials or TLS, replaces the ones installed before it. */
  holdFor(redis: RedisClient): void {
    if (this.holders.has(redis)) return
    if (!connectsAlike(this.source, redis)) this.connectWith(redis)
    else if (this.commandsEnded) {
      this.commands = this.makeCommandClient(redis)
      this.commandsEnded = false
    }
    const ended = () => {
      this.holders.delete(redis)
      if (this.holders.size > 0) return
      this.commands.disconnect()
      this.commandsEnded = true
    }
    this.holders.set(redis, ended)
    redis.once('end', ended)
  }

  /** Moves the backend's connections to ones made from `redis`. */
  private connectWith(redis: RedisClient): void {
    for (const [holder, ended] of this.holders) holder.off('end', ended)
    this.holders.clear()
    const previous = this.commands
    this.source = redis
    this.commands = this.makeCommandClient(redis)
    // QUIT lets the commands already sent get their replies; a client that never connected has none.
    if (previous.status === 'wait') previous.disconnect()
    else if (!this.commandsEnded) previous.quit().catch(() => {})
    this.commandsEnded = false
    this.subscriptions.reconnect(
      new Error("Telefunc's Redis backend moved to a client installed with other connection options"),
    )
  }

  /** The backend's command client made from `redis`. On a Cluster, a failover gives a slot to a replica that may lack
   *  what the old master accepted, and a subscriber connection on another node isn't cut by it: once a refresh of the
   *  client's slot map shows a slot on one of its replicas, the subscriptions resume on a fresh connection, each from
   *  where its key's master stands now. A slot that resharding moves to another master takes its keys with it, and
   *  the bus keeps delivering. */
  private makeCommandClient(redis: RedisClient): RedisClient {
    const commands = commandClientOf(redis)
    if (!isCluster(commands)) return commands
    /** Each slot's nodes at the last refresh that listed it, its master first. */
    const nodesBySlot: string[][] = []
    commands.on('refresh', () => {
      let failedOver = false
      commands.slots.forEach((nodes, slot) => {
        const before = nodesBySlot[slot]
        if (before !== undefined && before[0] !== nodes[0] && before.includes(nodes[0]!)) failedOver = true
        // A copy: a MOVED reply rewrites ioredis's entry in place.
        nodesBySlot[slot] = [...nodes]
      })
      if (failedOver && commands === this.commands) {
        this.subscriptions.reconnect(new Error('A Redis Cluster master failed over to its replica'))
      }
    })
    return commands
  }

  async run<Input, Output>(command: RedisCommand<Input, Output>, input: Input): Promise<Output> {
    const { keys, argv } = command.invoke(this.prefix, input)
    assert(command.numberOfKeys === null || command.numberOfKeys === keys.length)
    const parts = command.numberOfKeys === null ? [[String(keys.length)], keys, argv] : [keys, argv]
    const name = command.binaryReply ? `${command.name}Buffer` : command.name
    return command.parse(await this.send((client) => callDefinedCommand(client, name, ...parts)), input)
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
