export { RedisConnection }

import { assert } from './assert.js'
import {
  connectsAlike,
  createCommandClient,
  createSubscriberSocket,
  isCluster,
  nextReady,
  rawWrites,
  type RawCommand,
  type RedisClient,
  type SubscriberSocket,
} from './ioredis.js'
import { assertKeyPrefix } from './keys.js'
import { redisFailure } from './redisFailure.js'
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

  /** The command client, which the drivers ask whether it is a Cluster and the subscriber names in its failures;
   *  commands go through `send()` and `run()`. */
  get client(): RedisClient {
    return this.commands
  }

  /** Sends the command client's command `name` once the client is ready, as `call` makes it, which reaches no other
   *  command: a read or a write as ioredis's own table of Redis's commands says (`rawWrites()`). */
  send<Name extends RawCommand, Output>(
    name: Name,
    call: (client: Pick<RedisClient, Name>) => Promise<Output>,
  ): Promise<Output> {
    return this.dispatch(rawWrites(name), call)
  }

  /** Sends `command` on the command client once it is ready; a failure of Redis rejects as the backend reports it, with
   *  what this dispatch proves of a command that `writes`, or can't (`redisFailure()`). */
  private async dispatch<Output>(writes: boolean, command: (client: RedisClient) => Promise<Output>): Promise<Output> {
    const client = this.commands
    try {
      if (client.status !== 'ready') await nextReady(client)
    } catch (error) {
      // Never handed over, it wrote nothing.
      throw redisFailure(error, client, false)
    }
    try {
      return await command(client)
    } catch (error) {
      throw redisFailure(error, client, writes)
    }
  }

  /** The backend's command client lives while `redis`, or another app client installed with it, does; a client
   *  installed after the last one ended gets a new one, as the old one's close may still be pending. A client that
   *  connects otherwise, with other credentials or TLS, replaces the ones installed before it. */
  holdFor(redis: RedisClient): void {
    if (this.holders.has(redis)) return
    if (!connectsAlike(this.source, redis)) this.connectWith(redis)
    else if (this.commandsEnded) {
      this.commands = commandClientOf(redis)
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
    this.commands = commandClientOf(redis)
    // QUIT lets the commands already sent get their replies; a client that never connected has none.
    if (previous.status === 'wait') previous.disconnect()
    else if (!this.commandsEnded) previous.quit().catch(() => {})
    this.commandsEnded = false
    this.subscriptions.reconnect()
  }

  async run<Input, Output>(command: RedisCommand<Input, Output>, input: Input): Promise<Output> {
    const { keys, argv } = command.invoke(this.prefix, input)
    assert(command.numberOfKeys === null || command.numberOfKeys === keys.length)
    const keysAndArgs = command.numberOfKeys === null ? [String(keys.length), ...keys, ...argv] : [...keys, ...argv]
    const name = command.binaryReply ? bufferForm(command.name) : command.name
    // One array argument, which ioredis flattens into the command: an incarnation's keys can outnumber a call's
    // arguments. The method is the script's, as definedCommands.ts types it.
    return command.parse(await this.dispatch(command.writes, (client) => client[name](keysAndArgs)), input)
  }
}

/** The method of a defined command whose reply holds bytes. */
const bufferForm = <Name extends string>(name: Name): `${Name}Buffer` => `${name}Buffer`

/** The backend's own client for its commands, with its scripts defined. */
function commandClientOf(redis: RedisClient): RedisClient {
  const commands = createCommandClient(redis)
  for (const { name, lua, numberOfKeys } of Object.values(REDIS_COMMANDS)) {
    commands.defineCommand(name, numberOfKeys === null ? { lua } : { numberOfKeys, lua })
  }
  return commands
}
