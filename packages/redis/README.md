# `@telefunc/redis`

Redis-backed broadcast fan-out and Room state for Telefunc: one setup call makes both work across instances.

## Install

```sh
npm install @telefunc/redis ioredis
```

Requires Redis 5 or later.

## Setup

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis('redis://localhost:6379')
installRedis(redis)
```

That one `installRedis()` call configures Broadcast and Room from the same client. Make the call before the first Broadcast or Room use: an earlier use starts the in-memory backend, and `installRedis()` then throws.

`installRedis()` uses the same optional `prefix` for Broadcast and Room. Calling it again with the same client and prefix does nothing. A client that sets ioredis's `keyPrefix` makes `installRedis()` throw; pass `installRedis(redis, { prefix })` instead.

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

When you remove a Cluster node (`redis-cli --cluster del-node`), shut it down too: a removed node left running keeps the subscribers connected to it, and they receive nothing until it stops. Keep master clocks synchronized: room expiries use the masters' clocks.

Room keeps its state (rooms, members, lane order) in keys that Redis must never evict: use `maxmemory-policy noeviction`. Any other policy can silently close a room.

On Redis Cluster, publish receipts omit `receivers`.
