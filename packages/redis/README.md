# `@telefunc/redis`

Share Telefunc broadcasts and rooms across servers with Redis.

## Install

```sh
npm install @telefunc/redis ioredis
```

Set `REDIS_URL` on every server. Point every Telefunc server at the same Redis 5 or later.

Requires Redis 5 or later.

## Setup

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL!)
installRedis(redis)
```

Call `installRedis()` in your server entry before using Broadcast or Room. Pass an existing ioredis client to reuse its TLS and authentication settings. To use a dedicated key and channel prefix, pass `installRedis(redis, { prefix })` (default: `tf:`).

> Use `maxmemory-policy noeviction` for Room state and [sticky sessions](https://telefunc.com/stream/scale) for browser connections.

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

See [telefunc.com/redis](https://telefunc.com/redis) for the full documentation.
