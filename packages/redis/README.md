# `@telefunc/redis`

Redis-backed broadcast fan-out for Telefunc — a `publish()` on any instance reaches subscribers on every other instance.

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

## Room backend

`RedisRoomBackend` accepts either an ioredis `Redis` or `Cluster` client. The Cluster realization keeps
each room's atomic records in one hash slot, follows `MOVED` and `ASK` redirections, and discovers room
generation data across every master. Its `receivers` capability is `node-local`: Redis reports the
subscriber connections attached to the master executing `PUBLISH`, while Cluster Pub/Sub still delivers
the frame to the package's shared subscriber connection on another master.

```ts
import { Cluster } from 'ioredis'
import { RedisRoomBackend } from '@telefunc/redis'

const redis = new Cluster([
  { host: '127.0.0.1', port: 7000 },
  { host: '127.0.0.1', port: 7001 },
  { host: '127.0.0.1', port: 7002 },
])
const backend = new RedisRoomBackend({ redis })
```

That swaps Telefunc's default in-memory broadcast transport for Redis Pub/Sub. All subscribers across the cluster observe the same publish order for a given key.

`Channel` is per-instance — reconnects must land on the instance holding the channel's state. Pair this package with sticky sessions at the load balancer; see [Scaling](https://telefunc.com/stream/scale).

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options (e.g. TLS or a retry strategy):

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {} })
installRedis(redis)
```
