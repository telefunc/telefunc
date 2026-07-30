# `@telefunc/redis`

Redis-backed broadcast fan-out and Room state for Telefunc — one setup call makes both work across instances.

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

That one `installRedis()` call configures both the broadcast transport and the Room backend from the
same client. An explicit `installBackend()` remains available as an advanced override.

## Room backend

`RedisRoomBackend` accepts either an ioredis `Redis` or `Cluster` client. The Cluster realization keeps
each room's atomic records in one hash slot, follows `MOVED` and `ASK` redirections, and discovers room
generation data across every master. Its `receivers` capability is `none`: Cluster-wide Pub/Sub delivery
can reach a subscriber connection on another master, but the executing master's `PUBLISH` count cannot
report that global receiver total.

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

`installRedis()` uses the same optional `prefix` for broadcast and Room keys. Repeating it with the same
client and prefix is idempotent for the Room connection.

`Channel` is per-instance — reconnects must land on the instance holding the channel's state. Pair this package with sticky sessions at the load balancer; see [Scaling](https://telefunc.com/stream/scale).

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) Redis or Cluster instance when you want to share a connection or set custom options (e.g. TLS or a retry strategy):

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {} })
installRedis(redis)
```
