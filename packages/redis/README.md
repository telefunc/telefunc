# `@telefunc/redis`

Redis-backed broadcast fan-out and Room state for Telefunc: one setup call makes both work across instances.

## Install

```sh
npm install @telefunc/redis ioredis
```

## Setup

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis('redis://localhost:6379')
installRedis(redis)
```

That one `installRedis()` call configures Broadcast and Room from the same client. Telefunc runs its commands on its own connection, duplicated from your client with retries off, so a command whose reply was lost rejects rather than executes twice. Your client keeps its options, and Telefunc's connection closes when your client does. Make the call before the first Broadcast or Room use: an earlier use starts the in-memory backend, and `installRedis()` then throws.

`installRedis()` uses the same optional `prefix` for Broadcast and Room; `{` is reserved in prefixes. Calling it again with the same client and prefix does nothing. ioredis applies `keyPrefix` to commands but not to Pub/Sub channels, so `installRedis()` throws for a client that sets one; use `installRedis(redis, { prefix })` instead.

`Channel` is per-instance — reconnects must land on the instance holding the channel's state. Pair this package with sticky sessions at the load balancer; see [Scaling](https://telefunc.com/stream/scale).

### Redis Cluster

```ts
import { Cluster } from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new Cluster([
  { host: '127.0.0.1', port: 7000 },
  { host: '127.0.0.1', port: 7001 },
  { host: '127.0.0.1', port: 7002 },
])
installRedis(redis)
```

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) instance to share TLS/authentication settings:

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {} })
installRedis(redis)
```

## Delivery and Cluster notes

On a Cluster, a room's keys share one hash slot.

All subscriptions share one subscriber connection. When it drops, they resume on a fresh one; messages published in between are lost. Delivery stays at-most-once while a Cluster reshards: a message that arrives after a newer one is dropped, never replayed, so callbacks never go back in order. A failover whose new master missed the last writes rewinds their sequence numbers, so subscribers still connected drop as many later messages as it lost. When you remove a Cluster node (`redis-cli --cluster del-node`), shut it down too: a removed node left running keeps the subscribers connected to it, and they receive nothing until it stops. Keep master clocks synchronized: expiries use the clock of the master that owns the room's slot.

Room keeps its state (rooms, members, lane order) in keys with no expiry, so Redis must never evict them: use `maxmemory-policy noeviction`, or a `volatile-*` policy, which evicts only keys with an expiry.

On a Cluster, a publish's `receivers` is omitted: a master's `PUBLISH` counts only its own subscribers, so it can't prove that nobody is subscribed.
