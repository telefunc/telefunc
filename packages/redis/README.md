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
each room's atomic records and generation manifest in one hash slot and follows `MOVED` and `ASK`
redirections. A Cluster client must use ioredis's default `scaleReads: 'master'`; replica or custom read
routing is rejected because Room reads are strongly consistent. Head expiry and cell expiry use keyed Lua
reads, so each value is interpreted with `TIME` from the master that owns its room slot.

Redis Cluster delivery stays explicitly at-most-once during resharding. While a slot changes owner, a
frame already published by the old master can arrive after a newer frame or generation invalidation from
the new master. Telefunc drops that late lower-sequence frame (and ignores frames after invalidation), so
callbacks never move backward, but the in-flight frame is lost and is not replayed. An old frame that
reaches the subscriber before the invalidation signal can still be handed off even if cleanup has already
started. Keep Redis master clocks synchronized: after ownership changes, expiries use the new owner's
clock.

The Cluster `receivers` capability is `none`: cluster-wide Pub/Sub can reach a subscriber connection on
another master, but the executing master's `PUBLISH` count cannot report that global receiver total.

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
