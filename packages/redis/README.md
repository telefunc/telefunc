# `@telefunc/redis`

Redis-backed scaling for Telefunc — pub/sub fan-out across instances, plus cross-instance routing for channels.

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

That's it. Your Telefunc app now scales horizontally:

- Publishes from any instance fan out to every subscriber instance.
- Channels route correctly when their connection lands on a different instance behind your load balancer.

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options (TLS, retry strategy, etc):

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {} })
installRedis(redis)
```
