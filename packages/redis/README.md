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

const redis = new IORedis('redis://localhost:6379', { maxRetriesPerRequest: 0 })
installRedis(redis)
```

That one `installRedis()` call configures Broadcast and Room from the same client. Never-resend options make a lost command reply reject rather than execute twice; `installBackend()` remains an advanced override.

## Room storage

The installed backend accepts either an ioredis `Redis` or `Cluster` client. Cluster keeps each room's atomic records and generation manifest in one hash slot and follows `MOVED`/`ASK`; replica or custom routing is rejected because Room reads are strongly consistent. Head and cell expiry use keyed Lua `TIME` from the room-slot master.

Redis Cluster delivery stays at-most-once during resharding. While a slot changes owner, an old-master frame can arrive after a newer frame or generation invalidation; Telefunc drops that late lower sequence (and ignores frames after invalidation).
Callbacks never move backward, but the in-flight frame is lost rather than replayed; a frame arriving before invalidation may still be handed off after cleanup starts. Commands follow pre-execution `MOVED`/`ASK` replies but never resend after connection loss. Keep master clocks synchronized: expiries use the new owner's clock.

The Cluster `receivers` capability is `none`: cluster-wide Pub/Sub can reach another master's subscriber, but the executing master's `PUBLISH` count cannot report that global receiver total.

```ts
import { Cluster } from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new Cluster([
  { host: '127.0.0.1', port: 7000 },
  { host: '127.0.0.1', port: 7001 },
  { host: '127.0.0.1', port: 7002 },
], { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0 } })
installRedis(redis)
```

`installRedis()` uses the same optional `prefix` for Broadcast and Room; `{` is reserved in prefixes and a Broadcast key cannot begin with `}`. Repeating the same client/prefix is idempotent for Room.

`Channel` is per-instance — reconnects must land on the instance holding the channel's state. Pair this package with sticky sessions at the load balancer; see [Scaling](https://telefunc.com/stream/scale).

### Sharing an existing client

Pass an [`ioredis`](https://github.com/redis/ioredis) instance to share TLS/authentication settings. Keep the never-resend settings above; installation rejects retry-capable clients:

```ts
import IORedis from 'ioredis'
import { installRedis } from '@telefunc/redis'

const redis = new IORedis(process.env.REDIS_URL, { tls: {}, maxRetriesPerRequest: 0 })
installRedis(redis)
```
