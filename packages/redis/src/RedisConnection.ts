export { RedisConnection }

import { assert } from './assert.js'
import {
  callDefinedCommand,
  connectsAlike,
  createCommandClient,
  createSubscriberSocket,
  isCluster,
  redisFailure,
  type Dispatch,
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
  /** The wait for the command client's next `ready`, shared by the commands made meanwhile. */
  private readyWait: { client: RedisClient; promise: Promise<void> } | null = null

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

  /** The command client, for the commands that need no script: sent through `send()`. */
  get client(): RedisClient {
    return this.commands
  }

  /** Sends `command` on the command client once it is ready; a failure of Redis rejects as the backend reports it, with
   *  what this dispatch proves (`redisFailure()`). */
  async send<Output>(dispatch: Dispatch, command: (client: RedisClient) => Promise<Output>): Promise<Output> {
    const client = this.commands
    try {
      if (client.status !== 'ready') await this.ready(client)
    } catch (error) {
      // Never written: as for a read, it ran nowhere.
      throw redisFailure(error, client, 'read')
    }
    try {
      return await command(client)
    } catch (error) {
      throw redisFailure(error, client, dispatch)
    }
  }

  /** The client's next `ready`, one wait for every command made meanwhile; it rejects with the client's last error once
   *  the connection closes first, which is when ioredis would fail a command it held. */
  private ready(client: RedisClient): Promise<void> {
    if (this.readyWait?.client === client) return this.readyWait.promise
    if (client.status === 'end') return Promise.reject(new Error('Connection is closed.'))
    const promise = new Promise<void>((resolve, reject) => {
      let lastError: unknown = new Error('Connection is closed.')
      const onError = (error: unknown) => {
        lastError = error
      }
      const settle = () => {
        client.off('error', onError)
        client.off('ready', onReady)
        client.off('close', onClose)
        client.off('end', onClose)
        if (this.readyWait?.client === client) this.readyWait = null
      }
      const onReady = () => {
        settle()
        resolve()
      }
      const onClose = () => {
        settle()
        reject(lastError)
      }
      client.on('error', onError)
      client.once('ready', onReady)
      client.once('close', onClose)
      client.once('end', onClose)
    })
    this.readyWait = { client, promise }
    // A client made with `lazyConnect` connects at its first command.
    if (client.status === 'wait') void client.connect().catch(() => {})
    return promise
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
    const name = command.binaryReply ? `${command.name}Buffer` : command.name
    const dispatch = command.writes ? 'write' : 'read'
    return command.parse(await this.send(dispatch, (client) => callDefinedCommand(client, name, keysAndArgs)), input)
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
