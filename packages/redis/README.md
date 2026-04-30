# `@telefunc/redis`

Redis-backed scaling for Telefunc — pub/sub fan-out across instances, plus cross-instance routing for channels.

Uses [`ioredis`](https://github.com/redis/ioredis) under the hood.

## Install

```sh
pnpm add @telefunc/redis ioredis
```

## Setup

One line:

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

installRedis(new IORedis(process.env.REDIS_URL))
```

That's it. Your Telefunc app now scales horizontally:

- Publishes from any instance fan out to every subscriber instance.
- Channels route correctly when their connection lands on a different instance behind your load balancer.
