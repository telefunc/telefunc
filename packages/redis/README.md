# `@telefunc/redis`

Redis-backed `Broadcast` fan-out for Telefunc — publishes on any instance reach subscribers on every other instance.

## Install

```sh
pnpm add @telefunc/redis ioredis
```

## Setup

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis('redis://localhost:6379')
installRedis(redis)
```

That swaps Telefunc's default in-memory broadcast transport for Redis Pub/Sub. All subscribers across the cluster observe the same publish order for a given key.

`Channel` is per-instance — its reconnects need to land on the instance holding the channel's state. Pair this package with sticky sessions at the load balancer; see [Using multiple nodes](https://telefunc.com/multiple-nodes).

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options (TLS, retry strategy, etc):

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {} })
installRedis(redis)
```
